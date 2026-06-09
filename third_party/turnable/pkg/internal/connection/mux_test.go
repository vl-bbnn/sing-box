package connection

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestTinyMuxOpenChannelContextOpensFlow(t *testing.T) {
	client, server, cancel := newTestTinyMuxPair(t)
	defer cancel()

	channels := server.AcceptChannels(context.Background())
	done := make(chan error, 1)
	go func() {
		conn, err := client.OpenChannelContext(context.Background(), 0)
		if conn != nil {
			_ = conn.Close()
		}
		done <- err
	}()

	select {
	case channel := <-channels:
		_ = channel.Conn.Close()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server channel")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client open")
	}

	stats := client.Stats()
	if stats.OpenRequests != 1 || stats.OpenReplies != 1 || stats.OpenErrors != 0 || stats.MaxOpenLatencyMillis < 0 {
		t.Fatalf("stats=%+v, want one successful open", stats)
	}
}

func TestTinyMuxOpenChannelContextCancelsPendingOpen(t *testing.T) {
	client, _, cancel := newTestTinyMuxPair(t)
	defer cancel()

	ctx, ctxCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer ctxCancel()

	conn, err := client.OpenChannelContext(ctx, 0)
	if err == nil {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatal("expected canceled open")
	}

	stats := client.Stats()
	if stats.OpenRequests != 1 || stats.OpenCanceled != 1 || stats.OpenErrors != 1 || stats.OpenPending != 0 {
		t.Fatalf("stats=%+v, want one canceled pending open", stats)
	}
}

func newTestTinyMuxPair(t *testing.T) (*TinyMuxClient, *TinyMuxServer, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	left, right := net.Pipe()
	serverCh := make(chan *TinyMuxServer, 1)
	errCh := make(chan error, 1)
	go func() {
		server, err := NewTinyMuxServer(right)
		if err != nil {
			errCh <- err
			return
		}
		serverCh <- server
	}()

	client, err := NewTinyMuxClient(ctx, left)
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	select {
	case server := <-serverCh:
		return client, server, func() {
			_ = client.Close()
			_ = server.Close()
			cancel()
		}
	case err := <-errCh:
		cancel()
		t.Fatal(err)
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out creating tinymux pair")
	}
	panic("unreachable")
}
