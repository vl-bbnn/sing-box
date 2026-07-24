package v2rayxhttp

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestUploadQueueOrdersPackets(t *testing.T) {
	queue := newUploadQueue(4)
	if err := queue.Push(1, []byte("world")); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(0, []byte("hello ")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 11)
	if _, err := io.ReadFull(queue, buffer); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "hello world" {
		t.Fatalf("unexpected queue output: %q", buffer)
	}
}

func TestUploadQueueIsBounded(t *testing.T) {
	queue := newUploadQueue(1)
	if err := queue.Push(1, []byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(2, []byte("full")); err != errUploadQueueFull {
		t.Fatalf("expected queue full, got %v", err)
	}
}

func TestUploadQueueRejectsConsumedAndDuplicatePackets(t *testing.T) {
	queue := newUploadQueue(4)
	if err := queue.Push(0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 5)
	if _, err := io.ReadFull(queue, buffer); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(0, []byte("consumed")); err != errUploadSequenceConsumed {
		t.Fatalf("expected consumed error, got %v", err)
	}
	if err := queue.Push(2, []byte("once")); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(2, []byte("twice")); err != errUploadSequenceDuplicate {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestUploadQueueCloseUnblocksReader(t *testing.T) {
	queue := newUploadQueue(1)
	result := make(chan error, 1)
	go func() {
		var buffer [1]byte
		_, err := queue.Read(buffer[:])
		result <- err
	}()
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked after close")
	}
	if err := queue.Push(0, []byte("closed")); err != net.ErrClosed {
		t.Fatalf("expected closed error, got %v", err)
	}
}

func TestOrphanSessionExpires(t *testing.T) {
	server, err := NewServer(context.Background(), logger.NOP(), option.V2RayXHTTPOptions{
		Mode: modePacketUp,
	}, nil, &echoTransportHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.sessionTTL = 10 * time.Millisecond
	session := server.loadOrCreateSession("orphan")
	select {
	case <-time.After(time.Second):
		t.Fatal("orphan session was not reaped")
	default:
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, loaded := server.sessions.Load("orphan")
		if !loaded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("orphan session was not removed")
		}
		time.Sleep(time.Millisecond)
	}
	if err := session.queue.Push(0, []byte("closed")); err != net.ErrClosed {
		t.Fatalf("expected expired queue to be closed, got %v", err)
	}
}

func TestServerRejectsInvalidRequestShape(t *testing.T) {
	server, err := NewServer(context.Background(), logger.NOP(), option.V2RayXHTTPOptions{
		Host:          "cover.example.com",
		Path:          "/xhttp-test",
		Mode:          modePacketUp,
		XPaddingBytes: "4-8",
	}, nil, &echoTransportHandler{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	testCases := []struct {
		name       string
		method     string
		target     string
		host       string
		padding    string
		statusCode int
	}{
		{"bad host", http.MethodGet, "/xhttp-test/session", "other.example.com", "0000", http.StatusNotFound},
		{"bad path", http.MethodGet, "/other/session", "cover.example.com", "0000", http.StatusNotFound},
		{"missing padding", http.MethodGet, "/xhttp-test/session", "cover.example.com", "", http.StatusBadRequest},
		{"short padding", http.MethodGet, "/xhttp-test/session", "cover.example.com", "000", http.StatusBadRequest},
		{"download sequence", http.MethodGet, "/xhttp-test/session/0", "cover.example.com", "0000", http.StatusMethodNotAllowed},
		{"upload without sequence", http.MethodPost, "/xhttp-test/session", "cover.example.com", "0000", http.StatusMethodNotAllowed},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(testCase.method, "https://"+testCase.host+testCase.target, nil)
			request.Host = testCase.host
			if testCase.padding != "" {
				request.Header.Set("Referer", "https://"+testCase.host+testCase.target+"?x_padding="+testCase.padding)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != testCase.statusCode {
				t.Fatalf("expected status %d, got %d", testCase.statusCode, response.Code)
			}
		})
	}
}

func TestServerRejectsUnsupportedModeAndNegativePadding(t *testing.T) {
	for _, mode := range []string{modeAuto, modeStreamUp, modeStreamOne} {
		if _, err := NewServer(context.Background(), logger.NOP(), option.V2RayXHTTPOptions{Mode: mode}, nil, &echoTransportHandler{}); err == nil {
			t.Fatalf("expected mode %q to be rejected", mode)
		}
	}
	if _, err := NewServer(context.Background(), logger.NOP(), option.V2RayXHTTPOptions{
		Mode:          modePacketUp,
		XPaddingBytes: "-1",
	}, nil, &echoTransportHandler{}); err == nil {
		t.Fatal("expected negative padding to be rejected")
	}
}

func TestPacketUpClientServerH2C(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handler := &echoTransportHandler{}
	server, err := NewServer(ctx, logger.NOP(), option.V2RayXHTTPOptions{
		Path:          "/xhttp-test",
		Mode:          modePacketUp,
		XPaddingBytes: "100-1000",
	}, nil, handler)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	defer server.Close()
	go func() {
		_ = server.Serve(listener)
	}()

	client, err := NewClient(ctx, N.SystemDialer, M.SocksaddrFromNet(listener.Addr()), option.V2RayXHTTPOptions{
		Path:          "/xhttp-test",
		Mode:          modePacketUp,
		XPaddingBytes: "100-1000",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	conn, err := client.DialContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 5)
	if _, err = io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response, []byte("HELLO")) {
		t.Fatalf("unexpected response: %q", response)
	}
}

type echoTransportHandler struct{}

var _ adapter.V2RayServerTransportHandler = (*echoTransportHandler)(nil)

func (*echoTransportHandler) NewConnectionEx(_ context.Context, conn net.Conn, _ M.Socksaddr, _ M.Socksaddr, onClose N.CloseHandlerFunc) {
	go func() {
		defer func() {
			_ = conn.Close()
			if onClose != nil {
				onClose(nil)
			}
		}()
		request := make([]byte, 5)
		if _, err := io.ReadFull(conn, request); err != nil {
			return
		}
		_, _ = conn.Write(bytes.ToUpper(request))
	}()
}
