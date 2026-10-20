//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestEarlyOpenNoCallbackPreservesOriginalFailure(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	var calls atomic.Int32
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		calls.Add(1)
		return nil, carriercommon.BindPendingOpenPathLoss(ctx, &carriercommon.PeerPathLossError{Peers: 2})
	}
	started := time.Now()
	stream, err := s.DialStream(context.Background(), "eu", "example.com:443")
	if stream != nil || !errors.Is(err, io.EOF) || calls.Load() != 1 || time.Since(started) > 800*time.Millisecond {
		t.Fatalf("error hidden or recovery unbounded: %v calls=%d", err, calls.Load())
	}
	assertNoDialLeases(t, s)
}
func TestEarlyOpenCallerDeadlineNeverRestarts(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	var calls atomic.Int32
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		calls.Add(1)
		return nil, carriercommon.BindPendingOpenPathLoss(ctx, &carriercommon.PeerPathLossError{Peers: 2})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	stream, err := s.DialStream(ctx, "eu", "example.com:443")
	if stream != nil || !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("caller deadline lost: %v calls=%d", err, calls.Load())
	}
	assertNoDialLeases(t, s)
}
func TestEarlyOpenCallerCancellationNeverRestarts(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	failed := make(chan struct{})
	var calls atomic.Int32
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		calls.Add(1)
		close(failed)
		return nil, carriercommon.BindPendingOpenPathLoss(ctx, &carriercommon.PeerPathLossError{Peers: 2})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := dialAsync(s, ctx)
	<-failed
	cancel()
	result := awaitDialResult(t, done)
	if result.stream != nil || !errors.Is(result.err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("caller cancel lost: %v", result.err)
	}
	assertNoDialLeases(t, s)
}
func TestEarlyOpenProviderFailureIsImmediate(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	original := errors.New("provider rejected")
	var calls atomic.Int32
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		calls.Add(1)
		return nil, original
	}
	started := time.Now()
	stream, err := s.DialStream(context.Background(), "eu", "example.com:443")
	if stream != nil || err != original || calls.Load() != 1 || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("provider failure changed: %v", err)
	}
	assertNoDialLeases(t, s)
}
func TestEarlyOpenTerminalServiceCannotRevive(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	serviceCtx, stop := context.WithCancel(s.ctx)
	defer stop()
	s.ctx = serviceCtx
	failed := make(chan struct{})
	var calls atomic.Int32
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		calls.Add(1)
		close(failed)
		return nil, carriercommon.BindPendingOpenPathLoss(ctx, &carriercommon.PeerPathLossError{Peers: 2})
	}
	done := dialAsync(s, context.Background())
	<-failed
	stop()
	result := awaitDialResult(t, done)
	if result.stream != nil || !errors.Is(result.err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("terminal service revived: %v", result.err)
	}
	assertNoDialLeases(t, s)
}
func TestEarlyOpenPublishedStreamDoesNotWaitOrReplay(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	stream := &dialTestStream{}
	var calls atomic.Int32
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		calls.Add(1)
		return stream, nil
	}
	started := time.Now()
	got, err := s.DialStream(context.Background(), "eu", "example.com:443")
	if err != nil || got != stream || calls.Load() != 1 || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("published stream changed: %v", err)
	}
	s.NetworkUnavailable()
	if stream.reads.Load() != 0 || stream.writes.Load() != 0 || stream.aborted.Load() != 0 {
		t.Fatal("published payload replayed")
	}
	assertNoDialLeases(t, s)
}
