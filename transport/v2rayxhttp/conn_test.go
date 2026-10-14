package v2rayxhttp

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type contextCheckingRoundTripper struct {
	access   sync.Mutex
	requests []*http.Request
}

func (t *contextCheckingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	t.access.Lock()
	t.requests = append(t.requests, request)
	t.access.Unlock()
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func (t *contextCheckingRoundTripper) waitRequests(test *testing.T, count int) []*http.Request {
	test.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		t.access.Lock()
		if len(t.requests) >= count {
			requests := append([]*http.Request(nil), t.requests...)
			t.access.Unlock()
			return requests
		}
		t.access.Unlock()
		if time.Now().After(deadline) {
			test.Fatalf("timed out waiting for %d requests", count)
		}
		time.Sleep(time.Millisecond)
	}
}

func newContextTestClient(ctx context.Context, transport http.RoundTripper) *Client {
	return &Client{
		ctx:          ctx,
		serverAddr:   M.ParseSocksaddr("127.0.0.1:8080"),
		transport:    transport,
		scheme:       "http",
		host:         "example.com",
		path:         "/xhttp",
		paddingMin:   1,
		paddingMax:   1,
		noGRPCHeader: true,
	}
}

func TestPacketUpConnectionOutlivesDialContext(t *testing.T) {
	clientCtx, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	transport := new(contextCheckingRoundTripper)
	client := newContextTestClient(clientCtx, transport)

	dialCtx, cancelDial := context.WithCancel(context.Background())
	conn, err := client.dialPacketUp(dialCtx, "session")
	if err != nil {
		t.Fatal(err)
	}
	cancelDial()

	if _, err = conn.Write([]byte("hello")); err != nil {
		conn.Close()
		t.Fatalf("write after dial context cancellation: %v", err)
	}
	requests := transport.waitRequests(t, 2)
	for index, request := range requests {
		if err = request.Context().Err(); err != nil {
			conn.Close()
			t.Fatalf("request %d inherited dial context lifetime: %v", index, err)
		}
	}

	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	for index, request := range requests {
		select {
		case <-request.Context().Done():
		case <-time.After(time.Second):
			t.Fatalf("request %d context remained active after connection close", index)
		}
	}
}

func TestStreamingConnectionsOutliveDialContext(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		dial         func(*Client, context.Context) (net.Conn, error)
		requestCount int
	}{
		{
			name: "stream-up",
			dial: func(client *Client, ctx context.Context) (net.Conn, error) {
				return client.dialStreamUp(ctx, "session")
			},
			requestCount: 2,
		},
		{
			name: "stream-one",
			dial: func(client *Client, ctx context.Context) (net.Conn, error) {
				return client.dialStreamOne(ctx, "session")
			},
			requestCount: 1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			clientCtx, cancelClient := context.WithCancel(context.Background())
			defer cancelClient()
			transport := new(contextCheckingRoundTripper)
			client := newContextTestClient(clientCtx, transport)
			dialCtx, cancelDial := context.WithCancel(context.Background())

			conn, err := testCase.dial(client, dialCtx)
			if err != nil {
				t.Fatal(err)
			}
			cancelDial()
			requests := transport.waitRequests(t, testCase.requestCount)
			for index, request := range requests {
				if err = request.Context().Err(); err != nil {
					conn.Close()
					t.Fatalf("request %d inherited dial context lifetime: %v", index, err)
				}
			}

			if err = conn.Close(); err != nil {
				t.Fatal(err)
			}
			for index, request := range requests {
				select {
				case <-request.Context().Done():
				case <-time.After(time.Second):
					t.Fatalf("request %d context remained active after connection close", index)
				}
			}
		})
	}
}
