//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestCarrierConnectTimeoutMatchesRuntimeDefaults(t *testing.T) {
	for _, value := range []time.Duration{-1, 0, 30 * time.Second} {
		if got, want := CarrierConnectTimeout(value), withCarrierDefaults(CarrierOptions{ConnectTimeout: value}).ConnectTimeout; got != want {
			t.Fatalf("timeout=%v runtime=%v", got, want)
		}
	}
}

func TestCarrierLocalPendingOpenCancellationRetainsAdmissionFailure(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	entered := make(chan struct{})
	cause := errors.New("exact service lease generation retired")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	carrier.dialRoute = func(ctx context.Context, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		return nil, errors.Join(ctx.Err(), context.Cause(ctx))
	}
	done := make(chan error, 1)
	go func() { _, err := carrier.DialStream(ctx, "eu", "example.com:443"); done <- err }()
	<-entered
	cancel(cause)
	if err := <-done; !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cause: %v", err)
	}
	stats := carrier.Stats()
	if stats.FailedStreams != 1 || stats.OpenAttempts != 1 || stats.ActiveStreams != 0 || stats.PendingDials != 0 || len(carrier.openSlots) != 0 || len(carrier.activeSlots) != 0 {
		t.Fatalf("raw failure/admission=%+v", stats)
	}
}

func TestCarrierPublishedStreamSurvivesOpenContextCancellation(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	left, right := net.Pipe()
	defer right.Close()
	carrier.dialRoute = func(context.Context, string) (net.Conn, error) { return left, nil }
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	stream, err := carrier.DialStream(ctx, "eu", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	cancel(nil)
	done := make(chan error, 1)
	go func() { _, err := stream.Write([]byte("published")); done <- err }()
	buffer := make([]byte, len("published"))
	_ = right.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(right, buffer); err != nil {
		t.Fatalf("dial cancel closed real carrier stream: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "published" || carrier.Stats().ActiveStreams != 1 {
		t.Fatal("published stream lost")
	}
}
