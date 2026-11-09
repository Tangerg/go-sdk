// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by the license
// that can be found in the LICENSE file.

package mcp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"testing/synctest"

	"github.com/Tangerg/go-sdk/internal/jsonrpc2"
	"github.com/Tangerg/go-sdk/jsonrpc"
)

func writeCapacityMessage(peer net.Conn, message jsonrpc.Message) error {
	data, err := jsonrpc.EncodeMessage(message)
	if err != nil {
		return err
	}
	_, err = peer.Write(append(data, '\n'))
	return err
}

func initializeCapacityClient(peer net.Conn, version string) error {
	reader := bufio.NewReader(peer)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	message, err := jsonrpc.DecodeMessage(line)
	if err != nil {
		return err
	}
	request, ok := message.(*jsonrpc.Request)
	if !ok {
		return fmt.Errorf("expected initialization request, got %T", message)
	}
	var result Result
	if version >= protocolVersion20260728 && request.Method == methodDiscover {
		result = &DiscoverResult{SupportedVersions: []string{version}, Capabilities: &ServerCapabilities{}}
	} else if request.Method == methodInitialize {
		result = &InitializeResult{ProtocolVersion: version, ServerInfo: testImpl, Capabilities: &ServerCapabilities{}}
	} else {
		return fmt.Errorf("unexpected initialization method %q", request.Method)
	}
	response, err := jsonrpc2.NewResponse(request.ID, result, nil)
	if err != nil {
		return err
	}
	if err := writeCapacityMessage(peer, response); err != nil {
		return err
	}
	if version >= protocolVersion20260728 {
		return nil
	}
	line, err = reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	message, err = jsonrpc.DecodeMessage(line)
	if err != nil {
		return err
	}
	request, ok = message.(*jsonrpc.Request)
	if !ok || request.Method != notificationInitialized || request.IsCall() {
		return fmt.Errorf("expected initialized notification, got %T", message)
	}
	return nil
}

func capacityClient(t *testing.T, options ClientOptions) (*ClientSession, net.Conn) {
	t.Helper()
	return protocolClient(t, options, protocolVersion20251125, nil)
}

func protocolClient(t *testing.T, options ClientOptions, version string, configure func(*Client)) (*ClientSession, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	t.Cleanup(func() { local.Close(); peer.Close() })
	initialized := make(chan error, 1)
	go func() {
		err := initializeCapacityClient(peer, version)
		if err != nil {
			peer.Close()
		}
		initialized <- err
	}()
	client := NewClient(testImpl, &options)
	if configure != nil {
		configure(client)
	}
	transport := versionProjectionTransport{Transport: &LoggingTransport{
		Transport: &IOTransport{Reader: local, Writer: local, MaxLineLength: 1024}, Writer: io.Discard,
	}}
	session, err := client.Connect(t.Context(), transport, &ClientSessionOptions{ProtocolVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-initialized; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close(); session.Close() })
	return session, peer
}

func TestClientIncomingCapacityClosesAnUnreadReplyFlood(t *testing.T) {
	for _, limit := range []int{0, 3} {
		t.Run(fmt.Sprintf("limit-%d", limit), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				session, peer := capacityClient(t, ClientOptions{MaxIncomingRequests: limit})
				budget := limit
				if budget == 0 {
					budget = DefaultMaxIncomingRequests
				}
				for id := 1; id <= budget+1; id++ {
					request := &jsonrpc.Request{ID: jsonrpc2.Int64ID(int64(id)), Method: methodPing}
					if err := writeCapacityMessage(peer, request); err != nil {
						t.Fatalf("request %d ended before overflow: %v", id, err)
					}
				}
				if err := session.Wait(); !errors.Is(err, ErrIncomingCapacity) {
					t.Fatalf("unread replies did not saturate the original request owner: %v", err)
				}
			})
		})
	}
}

func TestClientIncomingCapacityIncludesRunningCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 1)
		cancelled := make(chan error, 1)
		session, peer := capacityClient(t, ClientOptions{
			MaxIncomingRequests: 1,
			CreateMessageHandler: func(ctx context.Context, _ *CreateMessageRequest) (*CreateMessageResult, error) {
				started <- struct{}{}
				<-ctx.Done()
				cancelled <- context.Cause(ctx)
				return nil, context.Cause(ctx)
			},
		})
		request, err := jsonrpc2.NewCall(jsonrpc2.Int64ID(1), methodCreateMessage, &CreateMessageParams{MaxTokens: 1, Messages: []*SamplingMessage{}})
		if err != nil {
			t.Fatal(err)
		}
		if err := writeCapacityMessage(peer, request); err != nil {
			t.Fatal(err)
		}
		<-started
		if err := writeCapacityMessage(peer, &jsonrpc.Request{ID: jsonrpc2.Int64ID(2), Method: methodPing}); err != nil {
			t.Fatal(err)
		}
		if err := <-cancelled; !errors.Is(err, ErrIncomingCapacity) {
			t.Fatalf("overflow did not cancel the original callback: %v", err)
		}
		if err := session.Wait(); !errors.Is(err, ErrIncomingCapacity) {
			t.Fatal(err)
		}
	})
}

func TestClientCancellationProgressesWithoutConsumingRequestCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		cancelled := make(chan error, 1)
		exit := make(chan struct{})
		session, peer := capacityClient(t, ClientOptions{
			MaxIncomingRequests: 1,
			CreateMessageHandler: func(ctx context.Context, _ *CreateMessageRequest) (*CreateMessageResult, error) {
				close(started)
				<-ctx.Done()
				cancelled <- context.Cause(ctx)
				<-exit
				return nil, context.Cause(ctx)
			},
		})
		request, err := jsonrpc2.NewCall(jsonrpc2.Int64ID(1), methodCreateMessage, &CreateMessageParams{MaxTokens: 1, Messages: []*SamplingMessage{}})
		if err != nil {
			t.Fatal(err)
		}
		if err := writeCapacityMessage(peer, request); err != nil {
			t.Fatal(err)
		}
		<-started
		cancellation, err := jsonrpc2.NewNotification(notificationCancelled, &CancelledParams{RequestID: request.ID.Raw(), Reason: "peer stopped"})
		if err != nil {
			t.Fatal(err)
		}
		for range DefaultMaxIncomingRequests + 1 {
			if err := writeCapacityMessage(peer, cancellation); err != nil {
				t.Fatalf("control notification consumed saturated work capacity: %v", err)
			}
		}
		if err := <-cancelled; !errors.Is(err, context.Canceled) {
			t.Fatalf("full capacity concealed peer cancellation: %v", err)
		}
		close(exit)
		reader := bufio.NewReader(peer)
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		message, err := jsonrpc.DecodeMessage(line)
		if err != nil {
			t.Fatal(err)
		}
		if response, ok := message.(*jsonrpc.Response); !ok || response.ID != request.ID || response.Error == nil {
			t.Fatalf("legacy cancellation did not retain its original response: %+v", message)
		}
		synctest.Wait()
		if err := writeCapacityMessage(peer, &jsonrpc.Request{ID: jsonrpc2.Int64ID(2), Method: methodPing}); err != nil {
			t.Fatal(err)
		}
		line, err = reader.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		message, err = jsonrpc.DecodeMessage(line)
		if err != nil {
			t.Fatal(err)
		}
		if response, ok := message.(*jsonrpc.Response); !ok || response.ID != jsonrpc2.Int64ID(2) || response.Error != nil {
			t.Fatalf("original cancellation completion did not restore capacity: %+v", message)
		}
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestServerIncomingCapacityUsesTheSameRequestOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, peer := net.Pipe()
		defer local.Close()
		defer peer.Close()
		session, err := NewServer(testImpl, &ServerOptions{MaxIncomingRequests: 1}).Connect(t.Context(), &IOTransport{Reader: local, Writer: local}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for id := int64(1); id <= 2; id++ {
			if err := writeCapacityMessage(peer, &jsonrpc.Request{ID: jsonrpc2.Int64ID(id), Method: methodPing}); err != nil {
				t.Fatal(err)
			}
		}
		if err := session.Wait(); !errors.Is(err, ErrIncomingCapacity) {
			t.Fatalf("server options did not reach original admission: %v", err)
		}
	})
}

type unusedCapacityTransport struct{ connected bool }

func (t *unusedCapacityTransport) Connect(context.Context) (Connection, error) {
	t.connected = true
	return nil, errors.New("transport must not connect")
}

func TestNegativeIncomingCapacityRefusesBeforeTransportAcquisition(t *testing.T) {
	clientTransport, serverTransport := &unusedCapacityTransport{}, &unusedCapacityTransport{}
	if _, err := NewClient(testImpl, &ClientOptions{MaxIncomingRequests: -1}).Connect(t.Context(), clientTransport, nil); err == nil || clientTransport.connected {
		t.Fatalf("client acquired transport with invalid admission: connected=%t error=%v", clientTransport.connected, err)
	}
	if _, err := NewServer(testImpl, &ServerOptions{MaxIncomingRequests: -1}).Connect(t.Context(), serverTransport, nil); err == nil || serverTransport.connected {
		t.Fatalf("server acquired transport with invalid admission: connected=%t error=%v", serverTransport.connected, err)
	}
}
