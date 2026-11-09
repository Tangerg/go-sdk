// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package jsonrpc2

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
)

type capacityTransport struct {
	in     chan Message
	out    chan Message
	closed chan struct{}
	once   sync.Once
}

func newCapacityTransport() *capacityTransport {
	return &capacityTransport{in: make(chan Message), out: make(chan Message), closed: make(chan struct{})}
}

func (t *capacityTransport) Read(ctx context.Context) (Message, error) {
	select {
	case message := <-t.in:
		return message, nil
	case <-t.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (t *capacityTransport) Write(ctx context.Context, message Message) error {
	select {
	case t.out <- message:
		return nil
	case <-t.closed:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (t *capacityTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func assertIncomingIdle(t *testing.T, conn *Connection) {
	t.Helper()
	conn.updateInFlight(func(s *inFlightState) {
		if s.incoming != 0 || len(s.incomingByID) != 0 || len(s.handlerQueue) != 0 || s.handlerRunning {
			t.Errorf("retained work after completion: incoming=%d IDs=%d queued=%d running=%t", s.incoming, len(s.incomingByID), len(s.handlerQueue), s.handlerRunning)
		}
	})
}

func TestIncomingCapacityIncludesQueuedAndUnwrittenResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := newCapacityTransport()
		defer transport.Close()
		conn := NewConnection(context.Background(), ConnectionConfig{
			MaxIncomingRequests: 2, Reader: transport, Writer: transport, Closer: transport,
			Bind: func(*Connection) Handler {
				return HandlerFunc(func(context.Context, *Request) (any, error) { return true, nil })
			},
		})
		transport.in <- &Request{ID: Int64ID(1), Method: "first"}
		transport.in <- &Request{ID: Int64ID(2), Method: "queued"}
		synctest.Wait()
		conn.updateInFlight(func(s *inFlightState) {
			if s.incoming != 2 || len(s.handlerQueue) != 1 || len(s.incomingByID) != 1 {
				t.Errorf("unwritten response lost its admission: incoming=%d queued=%d IDs=%d", s.incoming, len(s.handlerQueue), len(s.incomingByID))
			}
		})
		transport.in <- &Request{ID: Int64ID(3), Method: "excess"}
		if err := conn.Wait(); !errors.Is(err, ErrIncomingCapacity) {
			t.Fatalf("capacity did not close the blocked writer: %v", err)
		}
		assertIncomingIdle(t, conn)
	})
}

func TestIncomingNotificationsReleaseOnlyAfterHandlerExit(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			transport := newCapacityTransport()
			defer transport.Close()
			started := make(chan struct{})
			conn := NewConnection(context.Background(), ConnectionConfig{
				MaxIncomingRequests: 2, Reader: transport, Writer: transport, Closer: transport,
				Bind: func(*Connection) Handler {
					return HandlerFunc(func(ctx context.Context, _ *Request) (any, error) {
						close(started)
						<-ctx.Done()
						return nil, context.Cause(ctx)
					})
				},
			})
			transport.in <- &Request{Method: "running-notification"}
			<-started
			transport.in <- &Request{Method: "queued-notification"}
			synctest.Wait()
			conn.updateInFlight(func(s *inFlightState) {
				if s.incoming != 2 || len(s.handlerQueue) != 1 {
					t.Errorf("notification did not retain admission: incoming=%d queued=%d", s.incoming, len(s.handlerQueue))
				}
			})
			if overflow {
				transport.in <- &Request{Method: "excess-notification"}
			} else {
				transport.Close()
			}
			err := conn.Wait()
			if overflow != errors.Is(err, ErrIncomingCapacity) || !overflow && err != nil {
				t.Errorf("overflow=%t: %v", overflow, err)
			}
			assertIncomingIdle(t, conn)
		})
	}
}

type cancelPreempter struct{ conn *Connection }

func (p *cancelPreempter) Preempt(_ context.Context, request *Request) (any, error) {
	if !request.IsCall() && request.Method == "cancel" {
		p.conn.CancelFromPeer(Int64ID(100), context.Canceled)
		return nil, nil
	}
	return nil, ErrNotHandled
}

func TestIncomingCapacityAllowsResponseAndCancellationProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := newCapacityTransport()
		defer transport.Close()
		replied := make(chan error, 1)
		cancelled := make(chan error, 1)
		preempter := &cancelPreempter{}
		conn := NewConnection(context.Background(), ConnectionConfig{
			MaxIncomingRequests: 1, Reader: transport, Writer: transport, Closer: transport, Preempter: preempter,
			Bind: func(conn *Connection) Handler {
				preempter.conn = conn
				return HandlerFunc(func(ctx context.Context, request *Request) (any, error) {
					if request.Method == "quick" {
						return true, nil
					}
					call := conn.Call(ctx, "callback", nil)
					replied <- call.Await(ctx, nil)
					<-ctx.Done()
					cancelled <- context.Cause(ctx)
					return nil, context.Cause(ctx)
				})
			},
		})
		transport.in <- &Request{ID: Int64ID(100), Method: "wait"}
		callback := (<-transport.out).(*Request)
		response, err := NewResponse(callback.ID, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		transport.in <- response
		if err := <-replied; err != nil {
			t.Fatalf("full incoming budget blocked the callback response: %v", err)
		}
		transport.in <- &Request{Method: "cancel"}
		if err := <-cancelled; !errors.Is(err, context.Canceled) {
			t.Fatalf("full incoming budget blocked cancellation: %v", err)
		}
		<-transport.out
		synctest.Wait()
		assertIncomingIdle(t, conn)
		transport.in <- &Request{ID: Int64ID(101), Method: "quick"}
		if response := (<-transport.out).(*Response); response.ID != Int64ID(101) || response.Error != nil {
			t.Fatalf("completed original call did not restore capacity: %+v", response)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		assertIncomingIdle(t, conn)
	})
}

func TestDuplicateRequestDoesNotReplaceOriginalCapacityOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := newCapacityTransport()
		defer transport.Close()
		started := make(chan struct{})
		conn := NewConnection(context.Background(), ConnectionConfig{
			MaxIncomingRequests: 2, Reader: transport, Writer: transport, Closer: transport,
			Bind: func(*Connection) Handler {
				return HandlerFunc(func(ctx context.Context, _ *Request) (any, error) {
					close(started)
					<-ctx.Done()
					return nil, context.Cause(ctx)
				})
			},
		})
		transport.in <- &Request{ID: Int64ID(1), Method: "original"}
		<-started
		var original *incomingRequest
		conn.updateInFlight(func(s *inFlightState) { original = s.incomingByID[Int64ID(1)] })
		transport.in <- &Request{ID: Int64ID(1), Method: "duplicate"}
		synctest.Wait()
		conn.updateInFlight(func(s *inFlightState) {
			if s.incoming != 1 || s.incomingByID[Int64ID(1)] != original || len(s.handlerQueue) != 0 {
				t.Error("duplicate ID replaced or consumed the original request's admission")
			}
		})
		conn.CancelFromPeer(Int64ID(1), context.Canceled)
		<-transport.out
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		assertIncomingIdle(t, conn)
	})
}

type capacityPreempter func(context.Context, *Request) (any, error)

func (f capacityPreempter) Preempt(ctx context.Context, request *Request) (any, error) {
	return f(ctx, request)
}

func TestPreemptedRepliesRetainBoundedAdmissionWithoutBlockingReads(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			transport := newCapacityTransport()
			defer transport.Close()
			controlled := make(chan struct{}, 1)
			conn := NewConnection(context.Background(), ConnectionConfig{
				MaxIncomingRequests: 1, Reader: transport, Writer: transport, Closer: transport,
				Preempter: capacityPreempter(func(_ context.Context, request *Request) (any, error) {
					if request.IsCall() {
						return true, nil
					}
					controlled <- struct{}{}
					return nil, nil
				}),
				Bind: func(*Connection) Handler {
					return HandlerFunc(func(context.Context, *Request) (any, error) {
						t.Error("preempted request reached ordinary dispatch")
						return nil, ErrNotHandled
					})
				},
			})
			called := make(chan *AsyncCall, 1)
			go func() { called <- conn.Call(context.Background(), "outgoing", nil) }()
			outgoing := (<-transport.out).(*Request)
			call := <-called
			transport.in <- &Request{ID: Int64ID(100), Method: "preempted"}
			synctest.Wait()
			if overflow {
				transport.in <- &Request{ID: Int64ID(101), Method: "excess"}
				if err := conn.Wait(); !errors.Is(err, ErrIncomingCapacity) {
					t.Fatalf("preempted reply blocked overload retirement: %v", err)
				}
				if err := call.Await(context.Background(), nil); !errors.Is(err, ErrIncomingCapacity) {
					t.Fatalf("overflow lost original outgoing call: %v", err)
				}
			} else {
				response, err := NewResponse(outgoing.ID, true, nil)
				if err != nil {
					t.Fatal(err)
				}
				transport.in <- response
				if err := call.Await(context.Background(), nil); err != nil {
					t.Fatalf("preempted reply blocked an incoming response: %v", err)
				}
				transport.in <- &Request{Method: "control"}
				<-controlled
				<-transport.out
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
			}
			assertIncomingIdle(t, conn)
		})
	}
}
