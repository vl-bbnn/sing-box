//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
)

func awaitQuiescing(t *testing.T, s *Service) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.access.RLock()
		quiescing := s.quiescing
		s.access.RUnlock()
		if quiescing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("shutdown did not close dial admission")
}

func TestQuiesceFinishesAdmittedOpenAndRejectsNewWireWork(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	entered, release := make(chan dialTestAttempt, 1), make(chan struct{})
	defer close(release)
	var calls, canceled atomic.Int32
	stream := &dialTestStream{}
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		calls.Add(1)
		entered <- dialTestAttempt{ctx, c}
		select {
		case <-release:
			return stream, nil
		case <-ctx.Done():
			canceled.Add(1)
			return nil, ctx.Err()
		}
	}
	first := dialAsync(s, context.Background())
	attempt := awaitDialAttempt(t, entered)
	drain := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { drain <- s.QuiescePendingDials(ctx) }()
	awaitQuiescing(t, s)
	if result := awaitDialResult(t, dialAsync(s, context.Background())); !errors.Is(result.err, context.Canceled) {
		t.Fatalf("new dial was admitted: %+v", result)
	}
	select {
	case err := <-drain:
		t.Fatalf("drain returned before admitted OPEN: %v", err)
	default:
	}
	if attempt.ctx.Err() != nil || calls.Load() != 1 {
		t.Fatal("drain canceled the old request or admitted new wire work")
	}
	// A buffered completion lets deferred close remain the only channel close.
	release <- struct{}{}
	result := awaitDialResult(t, first)
	if result.err != nil || result.stream != stream || canceled.Load() != 0 {
		t.Fatalf("accepted OPEN was lost during graceful stop: %+v", result)
	}
	if err := <-drain; err != nil {
		t.Fatal(err)
	}
	if stream.aborted.Load() != 0 || stream.closed.Load() != 0 {
		t.Fatal("drain touched an already published stream")
	}
	assertNoDialLeases(t, s)
}

func TestQuiesceTimeoutDoesNotExtendOrCancelCallerDeadline(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	entered := make(chan dialTestAttempt, 1)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	caller, stopCaller := context.WithTimeout(context.Background(), time.Second)
	defer stopCaller()
	first := dialAsync(s, caller)
	attempt := awaitDialAttempt(t, entered)
	deadline, _ := attempt.ctx.Deadline()
	drainCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.QuiescePendingDials(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost shutdown timeout: %v", err)
	}
	if time.Since(start) > 300*time.Millisecond || attempt.ctx.Err() != nil {
		t.Fatal("drain did not remain bounded or canceled caller itself")
	}
	if after, _ := attempt.ctx.Deadline(); after != deadline {
		t.Fatal("caller deadline changed")
	}
	stopCaller()
	if result := awaitDialResult(t, first); !errors.Is(result.err, context.Canceled) {
		t.Fatalf("normal cancellation was erased: %v", result.err)
	}
}

func TestQuiesceRetainsActualOpenFailure(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	entered, release := make(chan dialTestAttempt, 1), make(chan struct{})
	defer close(release)
	actual := errors.New("remote OPEN refused")
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		<-release
		return nil, actual
	}
	first := dialAsync(s, context.Background())
	awaitDialAttempt(t, entered)
	done := make(chan error, 1)
	go func() { done <- s.QuiescePendingDials(context.Background()) }()
	awaitQuiescing(t, s)
	release <- struct{}{}
	if result := awaitDialResult(t, first); result.err != actual {
		t.Fatalf("genuine transport error changed: %v", result.err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestQuiesceWakesWaitersWithoutAdmittingAnInitialCarrier(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	s.carrier = nil
	first := dialAsync(s, context.Background())
	if err := s.QuiescePendingDials(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result := awaitDialResult(t, first); !errors.Is(result.err, context.Canceled) {
		t.Fatalf("pre-admission caller stuck during shutdown: %v", result.err)
	}
	if err := s.QuiescePendingDials(context.Background()); err != nil {
		t.Fatal("repeated drain is not idempotent", err)
	}
}
