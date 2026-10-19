//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type holdResumeCall struct {
	ctx   context.Context
	reply chan error
}
type scriptedNetworkRecovery struct {
	deadline time.Time
	calls    chan holdResumeCall
}

func (h *scriptedNetworkRecovery) Deadline() time.Time { return h.deadline }
func (h *scriptedNetworkRecovery) ResumeNetwork(ctx context.Context) error {
	call := holdResumeCall{ctx, make(chan error, 1)}
	select {
	case h.calls <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-call.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func newHoldFixture(t *testing.T, cap time.Duration) (*Service, *wltpkg.Carrier, *scriptedNetworkRecovery) {
	s, a := newPendingDialTestService(t)
	h := &scriptedNetworkRecovery{time.Now().Add(cap), make(chan holdResumeCall, 8)}
	s.suspendNetworkHook = func(*wltpkg.Carrier) wltpkg.NetworkRecovery { return h }
	return s, a, h
}
func awaitResume(t *testing.T, h *scriptedNetworkRecovery) holdResumeCall {
	t.Helper()
	select {
	case c := <-h.calls:
		return c
	case <-time.After(time.Second):
		t.Fatal("resume not invoked")
		return holdResumeCall{}
	}
}
func awaitCoreState(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !predicate() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !predicate() {
		t.Fatal("core state did not settle")
	}
}
func interfaceGateOpen(s *Service) bool {
	s.access.RLock()
	defer s.access.RUnlock()
	select {
	case <-s.interfaceReady:
		return !s.interfaceUnavailable
	default:
		return false
	}
}

func TestRetainedNetworkHoldPublishesExactOriginalOpenAfterProof(t *testing.T) {
	s, a, h := newHoldFixture(t, time.Second)
	entered, release := make(chan dialTestAttempt, 1), make(chan struct{})
	stream := &dialTestStream{}
	var dials, aborts atomic.Int32
	s.abortCarrierHook = func(*wltpkg.Carrier) error { aborts.Add(1); return nil }
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		dials.Add(1)
		entered <- dialTestAttempt{ctx, c}
		<-release
		return stream, nil
	}
	deadline := time.Now().Add(800 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	resultCh := dialAsync(s, ctx)
	attempt := awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	close(release)
	select {
	case <-resultCh:
		t.Fatal("OPEN escaped unavailable admission")
	case <-time.After(20 * time.Millisecond):
	}
	if attempt.ctx.Err() != nil || aborts.Load() != 0 {
		t.Fatal("held original was retired")
	}
	s.interfaceUpdated("wifi", "android", time.Second)
	resume := awaitResume(t, h)
	select {
	case <-resultCh:
		t.Fatal("OPEN published before same-session proof")
	case <-time.After(20 * time.Millisecond):
	}
	resume.reply <- nil
	result := awaitDialResult(t, resultCh)
	if result.err != nil || result.stream != stream || dials.Load() != 1 || aborts.Load() != 0 || s.carrier != a || s.carrierGeneration != 1 {
		t.Fatal("original operation/generation replaced", result)
	}
	if got, _ := attempt.ctx.Deadline(); !got.Equal(deadline) {
		t.Fatal("caller deadline changed")
	}
	if stream.reads.Load() != 0 || stream.writes.Load() != 0 || stream.aborted.Load() != 0 || !interfaceGateOpen(s) {
		t.Fatal("stream replayed/closed or gate remained shut")
	}
	if !s.WLTNetworkResetRetained() {
		t.Fatal("settled duplicate would reset retained nested mux")
	}
	s.interfaceUpdated("wifi", "android", time.Millisecond)
	if !s.WLTNetworkResetRetained() {
		t.Fatal("duplicate notification lost retention")
	}
	assertNoDialLeases(t, s)
}

func TestRetainedNetworkHoldCallerDeadlineAbortsLateOriginalWithoutReplay(t *testing.T) {
	s, _, _ := newHoldFixture(t, time.Second)
	entered, release := make(chan dialTestAttempt, 1), make(chan struct{})
	stream := &dialTestStream{}
	var dials atomic.Int32
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		dials.Add(1)
		entered <- dialTestAttempt{ctx, c}
		<-release
		return stream, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Millisecond)
	defer cancel()
	results := dialAsync(s, ctx)
	awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	close(release)
	result := awaitDialResult(t, results)
	if !errors.Is(result.err, context.DeadlineExceeded) || result.stream != nil || stream.aborted.Load() != 1 || dials.Load() != 1 {
		t.Fatal("deadline replayed or leaked original", result)
	}
	assertNoDialLeases(t, s)
}

func TestRetainedNetworkHoldExpiresIndependentlyWhileUnavailable(t *testing.T) {
	s, a, h := newHoldFixture(t, 45*time.Millisecond)
	aborted := make(chan *wltpkg.Carrier, 2)
	s.abortCarrierHook = func(c *wltpkg.Carrier) error { aborted <- c; return nil }
	s.NetworkUnavailable()
	for i := 0; i < 10; i++ {
		s.NetworkUnavailable()
	}
	select {
	case c := <-aborted:
		if c != a {
			t.Fatal("wrong carrier aborted")
		}
	case <-time.After(time.Second):
		t.Fatal("idle hold did not expire")
	}
	if interfaceGateOpen(s) || s.WLTNetworkRecoveryActive() || !h.Deadline().Before(time.Now()) {
		t.Fatal("expired hold admitted work")
	}
}

func TestRetainedNetworkHoldOldResumeCannotCommitAcrossSecondAbsence(t *testing.T) {
	s, _, h := newHoldFixture(t, time.Second)
	s.NetworkUnavailable()
	s.interfaceUpdated("wifi", "android", time.Second)
	old := awaitResume(t, h)
	s.NetworkUnavailable()
	s.interfaceUpdated("lte", "android", time.Second)
	fresh := awaitResume(t, h)
	old.reply <- nil
	time.Sleep(20 * time.Millisecond)
	if interfaceGateOpen(s) || !s.WLTNetworkRecoveryActive() {
		t.Fatal("old resume opened/failed newer interface epoch")
	}
	fresh.reply <- nil
	awaitCoreState(t, func() bool { return interfaceGateOpen(s) })
}

func TestRetainedNetworkHoldFailureDoesNotReplayPendingOriginal(t *testing.T) {
	s, a, h := newHoldFixture(t, time.Second)
	b := &wltpkg.Carrier{}
	var dials atomic.Int32
	entered := make(chan dialTestAttempt, 1)
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return b, nil }
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		dials.Add(1)
		entered <- dialTestAttempt{ctx, c}
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	results := dialAsync(s, context.Background())
	awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	s.interfaceUpdated("wifi", "android", time.Second)
	awaitResume(t, h).reply <- errors.New("original control path failed")
	result := awaitDialResult(t, results)
	if result.err == nil || result.stream != nil || dials.Load() != 1 {
		t.Fatal("failed retained OPEN replayed", result)
	}
	awaitCoreState(t, func() bool { s.access.RLock(); defer s.access.RUnlock(); return s.carrier == b })
	if s.carrier == a {
		t.Fatal("bounded fallback did not replace failed carrier")
	}
	assertNoDialLeases(t, s)
}

func TestRetainedNetworkHoldStopRejectsLateProof(t *testing.T) {
	s, _, h := newHoldFixture(t, time.Second)
	s.NetworkUnavailable()
	s.interfaceUpdated("wifi", "android", time.Second)
	resume := awaitResume(t, h)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	resume.reply <- nil
	time.Sleep(20 * time.Millisecond)
	s.access.RLock()
	defer s.access.RUnlock()
	if !s.stopped || s.carrier != nil || s.networkHold != nil {
		t.Fatal("late resume resurrected stopped service")
	}
}
