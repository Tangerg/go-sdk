// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/Tangerg/go-sdk/internal/jsonrpc2"
	"github.com/Tangerg/go-sdk/jsonrpc"
)

type versionProjectionTransport struct {
	Transport
	versions chan string
}

func (t versionProjectionTransport) Connect(ctx context.Context) (Connection, error) {
	connection, err := t.Transport.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &versionProjectionConnection{Connection: connection, versions: t.versions}, nil
}

type versionProjectionConnection struct {
	Connection
	versions chan string
}

func (c *versionProjectionConnection) ProtocolVersionNegotiated(version string) {
	if c.versions != nil {
		c.versions <- version
	}
	if receiver, ok := c.Connection.(ProtocolVersionReceiver); ok {
		receiver.ProtocolVersionNegotiated(version)
	}
}

func (c *versionProjectionConnection) DropResponse(id jsonrpc.ID) {
	if dropper, ok := c.Connection.(jsonrpc2.ResponseDropper); ok {
		dropper.DropResponse(id)
	}
}

func TestNegotiatedClientVersionControlsCancellationThroughWrappers(t *testing.T) {
	for _, version := range []string{protocolVersion20251125, protocolVersion20260728} {
		t.Run(version, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := make(chan struct{})
				cancelled := make(chan error, 1)
				var calls atomic.Int32
				session, peer := protocolClient(t, ClientOptions{MaxIncomingRequests: 1}, version, func(client *Client) {
					client.AddReceivingMiddleware(func(next MethodHandler) MethodHandler {
						return func(ctx context.Context, method string, request Request) (Result, error) {
							if method == methodPing && calls.Add(1) == 1 {
								close(started)
								<-ctx.Done()
								cancelled <- context.Cause(ctx)
								return nil, context.Cause(ctx)
							}
							return next(ctx, method, request)
						}
					})
				})
				if session.InitializeResult().ProtocolVersion != version {
					t.Fatalf("negotiated %s, want %s", session.InitializeResult().ProtocolVersion, version)
				}
				original := &jsonrpc.Request{ID: jsonrpc2.Int64ID(1), Method: methodPing}
				if err := writeCapacityMessage(peer, original); err != nil {
					t.Fatal(err)
				}
				<-started
				cancellation, err := jsonrpc2.NewNotification(notificationCancelled, &CancelledParams{RequestID: original.ID.Raw()})
				if err != nil {
					t.Fatal(err)
				}
				if err := writeCapacityMessage(peer, cancellation); err != nil {
					t.Fatal(err)
				}
				if err := <-cancelled; !errors.Is(err, context.Canceled) {
					t.Fatalf("original request was not cancelled: %v", err)
				}
				reader := bufio.NewReader(peer)
				if version < protocolVersion20260728 {
					line, err := reader.ReadBytes('\n')
					if err != nil {
						t.Fatal(err)
					}
					message, err := jsonrpc.DecodeMessage(line)
					if response, ok := message.(*jsonrpc.Response); err != nil || !ok || response.ID != original.ID || response.Error == nil {
						t.Fatalf("legacy cancellation lost its original response: %+v %v", message, err)
					}
				}
				synctest.Wait()
				if err := writeCapacityMessage(peer, &jsonrpc.Request{ID: jsonrpc2.Int64ID(2), Method: methodPing}); err != nil {
					t.Fatal(err)
				}
				line, err := reader.ReadBytes('\n')
				if err != nil {
					t.Fatal(err)
				}
				message, err := jsonrpc.DecodeMessage(line)
				if response, ok := message.(*jsonrpc.Response); err != nil || !ok || response.ID != jsonrpc2.Int64ID(2) || response.Error != nil {
					t.Fatalf("cancelled reply survived or original slot remained held: %+v %v", message, err)
				}
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestNegotiatedClientVersionRejectsUnsupportedBatching(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		session, peer := protocolClient(t, ClientOptions{}, protocolVersion20260728, nil)
		if _, err := io.WriteString(peer, "[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}]\n"); err != nil {
			t.Fatal(err)
		}
		if err := session.Wait(); err == nil || !strings.Contains(err.Error(), "JSON-RPC batching is not supported") {
			t.Fatalf("client transport lost negotiated framing: %v", err)
		}
	})
}

func TestServerProjectsItsNegotiatedVersionOnlyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clientTransport, serverTransport := NewInMemoryTransports()
		versions := make(chan string, 2)
		server := NewServer(testImpl, &ServerOptions{SupportedProtocolVersions: []string{protocolVersion20251125}})
		serverSession, err := server.Connect(t.Context(), versionProjectionTransport{Transport: serverTransport, versions: versions}, nil)
		if err != nil {
			t.Fatal(err)
		}
		clientSession, err := NewClient(testImpl, nil).Connect(t.Context(), clientTransport, &ClientSessionOptions{ProtocolVersion: protocolVersion20241105})
		if err != nil {
			t.Fatal(err)
		}
		if version := <-versions; version != protocolVersion20251125 || version != clientSession.InitializeResult().ProtocolVersion {
			t.Fatalf("projection replaced negotiated version with requested version: %s", version)
		}
		synctest.Wait()
		if len(versions) != 0 {
			t.Fatal("unrelated state update published protocol negotiation again")
		}
		if err := clientSession.Close(); err != nil {
			t.Fatal(err)
		}
		if err := serverSession.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

type panickingVersionProjection struct{ Connection }

func (panickingVersionProjection) ProtocolVersionNegotiated(string) {
	panic("broken transport projection")
}

func TestProtocolVersionProjectionPanicDoesNotRetainSessionLock(t *testing.T) {
	session := &ServerSession{mcpConn: panickingVersionProjection{}}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("fixture did not panic")
			}
		}()
		session.updateState(func(state *ServerSessionState) {
			state.NegotiatedProtocolVersion = protocolVersion20260728
		})
	}()
	if !session.mu.TryLock() {
		t.Fatal("transport projection retained the session state lock after panic")
	}
	defer session.mu.Unlock()
	if session.state.NegotiatedProtocolVersion != protocolVersion20260728 {
		t.Fatal("projection failure replaced the original negotiated fact")
	}
}
