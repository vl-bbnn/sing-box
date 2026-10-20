//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing/common/json/badoption"
)

type dialTestStream struct{ reads, writes, closed, aborted atomic.Int32 }

func (s *dialTestStream) Read([]byte) (int, error)    { s.reads.Add(1); return 0, io.EOF }
func (s *dialTestStream) Write(p []byte) (int, error) { s.writes.Add(1); return len(p), nil }
func (s *dialTestStream) Close() error                { s.closed.Add(1); return nil }
func (s *dialTestStream) Abort() error                { s.aborted.Add(1); return nil }

type dialTestAttempt struct {
	ctx     context.Context
	carrier *wltpkg.Carrier
}
type dialTestResult struct {
	stream io.ReadWriteCloser
	err    error
}

func newPendingDialTestService(t *testing.T) (*Service, *wltpkg.Carrier) {
	t.Helper()
	s := newLifecycleTestService(t)
	a := &wltpkg.Carrier{}
	s.carrier, s.carrierGeneration, s.initialized, s.interfaceKey = a, 1, true, "lte"
	closeSignal(s.carrierReady)
	return s, a
}

func dialAsync(s *Service, ctx context.Context) <-chan dialTestResult {
	done := make(chan dialTestResult, 1)
	go func() { stream, err := s.DialStream(ctx, "eu", "example.com:443"); done <- dialTestResult{stream, err} }()
	return done
}
func awaitDialAttempt(t *testing.T, ch <-chan dialTestAttempt) dialTestAttempt {
	t.Helper()
	select {
	case attempt := <-ch:
		return attempt
	case <-time.After(time.Second):
		t.Fatal("no dial attempt")
		return dialTestAttempt{}
	}
}
func awaitDialResult(t *testing.T, ch <-chan dialTestResult) dialTestResult {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(time.Second):
		t.Fatal("dial did not finish")
		return dialTestResult{}
	}
}
func publishDialReplacement(t *testing.T, s *Service, b *wltpkg.Carrier) {
	t.Helper()
	old, _, generation, ignored := s.beginInterfaceUpdate("wifi", "android")
	if ignored {
		t.Fatal("return notification ignored")
	}
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return b, nil }
	s.restartCarrier(old, "pending dial test", generation)
	s.finishInterfaceRecovery(generation)
}
func assertNoDialLeases(t *testing.T, s *Service) {
	t.Helper()
	s.access.RLock()
	defer s.access.RUnlock()
	if len(s.pendingDialLeases) != 0 {
		t.Fatalf("leaked pending leases: %d", len(s.pendingDialLeases))
	}
}

func TestPendingDialRecoversOnlyUnpublishedOpenOnNewCarrier(t *testing.T) {
	s, a := newPendingDialTestService(t)
	b, stream := &wltpkg.Carrier{}, &dialTestStream{}
	entered := make(chan dialTestAttempt, 2)
	var active, failed atomic.Int32
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		active.Add(1)
		defer active.Add(-1)
		entered <- dialTestAttempt{ctx, c}
		if c == a {
			<-ctx.Done()
			failed.Add(1)
			return nil, context.Cause(ctx)
		}
		if active.Load() != 1 {
			t.Error("replacement admitted before old attempt released")
		}
		return stream, nil
	}
	deadline := time.Now().Add(900 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	resultCh := dialAsync(s, ctx)
	first := awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	publishDialReplacement(t, s, b)
	second := awaitDialAttempt(t, entered)
	result := awaitDialResult(t, resultCh)
	if result.err != nil || result.stream != stream || first.carrier != a || second.carrier != b {
		t.Fatalf("wrong recovery result: %+v", result)
	}
	for _, attempt := range []dialTestAttempt{first, second} {
		if got, ok := attempt.ctx.Deadline(); !ok || !got.Equal(deadline) {
			t.Fatal("absolute caller deadline changed")
		}
	}
	if failed.Load() != 1 || active.Load() != 0 {
		t.Fatal("attempt failure erased or active admission retained")
	}
	assertNoDialLeases(t, s)
	if stream.reads.Load() != 0 || stream.writes.Load() != 0 || stream.aborted.Load() != 0 {
		t.Fatal("published stream was touched")
	}
}

func TestPendingDialUsesSingleConfiguredDeadlineIncludingRecovery(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	s.options.ConnectTimeout = badoption.Duration(80 * time.Millisecond)
	entered := make(chan dialTestAttempt, 1)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	started := time.Now()
	resultCh := dialAsync(s, context.Background())
	first := awaitDialAttempt(t, entered)
	deadline, ok := first.ctx.Deadline()
	if !ok || deadline.Sub(started) > 100*time.Millisecond {
		t.Fatal("missing original configured deadline")
	}
	s.NetworkUnavailable()
	result := awaitDialResult(t, resultCh)
	if result.stream != nil || !errors.Is(result.err, context.DeadlineExceeded) {
		t.Fatalf("expiry=%+v", result)
	}
	if time.Now().Before(deadline) || time.Since(started) > 500*time.Millisecond {
		t.Fatal("deadline reset or returned prematurely")
	}
	var recovery *wltpkg.CarrierRecoveryError
	if !errors.As(result.err, &recovery) {
		t.Fatal("recovery failure lost direct-fallback guard")
	}
	// A late physical return cannot revive the expired logical operation.
	publishDialReplacement(t, s, &wltpkg.Carrier{})
	select {
	case <-entered:
		t.Fatal("OPEN after original deadline")
	default:
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialLateOldSuccessAbortedBeforeReplacement(t *testing.T) {
	s, a := newPendingDialTestService(t)
	oldStream, newStream := &dialTestStream{}, &dialTestStream{}
	entered := make(chan dialTestAttempt, 2)
	release := make(chan struct{})
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		if c == a {
			<-release
			return oldStream, nil
		}
		if oldStream.aborted.Load() != 1 {
			t.Error("replacement before obsolete success abort")
		}
		return newStream, nil
	}
	resultCh := dialAsync(s, context.Background())
	awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	publishDialReplacement(t, s, &wltpkg.Carrier{})
	close(release)
	awaitDialAttempt(t, entered)
	result := awaitDialResult(t, resultCh)
	if result.err != nil || result.stream != newStream {
		t.Fatalf("late success published: %+v", result)
	}
	if oldStream.aborted.Load() != 1 || oldStream.closed.Load() != 0 || oldStream.reads.Load() != 0 || oldStream.writes.Load() != 0 {
		t.Fatal("obsolete stream replayed or gracefully drained")
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialGenuineCurrentErrorsNeverRetry(t *testing.T) {
	for _, realError := range []error{io.EOF, io.ErrUnexpectedEOF, errors.New("tinymux client session closed"), errors.New("provider rejected"), context.Canceled} {
		t.Run(realError.Error(), func(t *testing.T) {
			s, _ := newPendingDialTestService(t)
			calls := 0
			s.dialStreamHook = func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error) {
				calls++
				return nil, realError
			}
			stream, err := s.DialStream(context.Background(), "eu", "example.com:443")
			if stream != nil || err != realError || calls != 1 {
				t.Fatalf("genuine error changed: %v calls=%d", err, calls)
			}
			assertNoDialLeases(t, s)
		})
	}
}

func TestPendingDialDoesNotReclassifyUnrelatedErrorAfterLoss(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	entered := make(chan dialTestAttempt, 1)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		<-ctx.Done()
		return nil, io.EOF
	}
	resultCh := dialAsync(s, context.Background())
	awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	result := awaitDialResult(t, resultCh)
	if result.err != io.EOF {
		t.Fatalf("unrelated EOF reclassified: %v", result.err)
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialCancellationAndTerminalCloseNeverRevive(t *testing.T) {
	for _, action := range []string{"caller_cancel", "service_close", "service_context"} {
		t.Run(action, func(t *testing.T) {
			s, _ := newPendingDialTestService(t)
			serviceCtx, serviceCancel := context.WithCancel(s.ctx)
			defer serviceCancel()
			s.ctx = serviceCtx
			entered := make(chan dialTestAttempt, 1)
			s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
				entered <- dialTestAttempt{ctx, c}
				<-ctx.Done()
				return nil, context.Cause(ctx)
			}
			caller, cancel := context.WithCancel(context.Background())
			defer cancel()
			resultCh := dialAsync(s, caller)
			awaitDialAttempt(t, entered)
			s.NetworkUnavailable()
			switch action {
			case "caller_cancel":
				cancel()
			case "service_close":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			case "service_context":
				serviceCancel()
			}
			result := awaitDialResult(t, resultCh)
			if result.stream != nil || !errors.Is(result.err, context.Canceled) {
				t.Fatalf("terminal=%+v", result)
			}
			select {
			case <-entered:
				t.Fatal("new OPEN after terminal cancellation")
			default:
			}
			if action == "service_close" {
				if err := s.Start(0); err == nil {
					t.Fatal("closed service revived")
				}
			}
			assertNoDialLeases(t, s)
		})
	}
}

func TestPendingDialCloseAbortsLateSuccess(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	entered, release := make(chan struct{}), make(chan struct{})
	late := &dialTestStream{}
	s.dialStreamHook = func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error) {
		close(entered)
		<-release
		return late, nil
	}
	resultCh := dialAsync(s, context.Background())
	awaitLifecycleEvent(t, entered, "OPEN")
	_ = s.Close()
	close(release)
	result := awaitDialResult(t, resultCh)
	if result.stream != nil || !errors.Is(result.err, context.Canceled) || late.aborted.Load() != 1 {
		t.Fatalf("late Close result=%+v aborts=%d", result, late.aborted.Load())
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialRequiresNewPointerAndNewGeneration(t *testing.T) {
	s, a := newPendingDialTestService(t)
	entered := make(chan dialTestAttempt, 2)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		if c == a {
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}
		return &dialTestStream{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resultCh := dialAsync(s, ctx)
	awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	s.access.Lock()
	s.interfaceUnavailable = false
	closeSignal(s.interfaceReady)
	s.notifyDialStateLocked()
	s.access.Unlock()
	select {
	case <-entered:
		t.Fatal("retried retained obsolete pointer")
	case <-time.After(15 * time.Millisecond):
	}
	s.access.Lock()
	s.carrier = &wltpkg.Carrier{}
	s.notifyDialStateLocked()
	s.access.Unlock()
	select {
	case <-entered:
		t.Fatal("retried without newer generation")
	case <-time.After(15 * time.Millisecond):
	}
	s.access.Lock()
	s.carrierGeneration++
	s.notifyDialStateLocked()
	s.access.Unlock()
	awaitDialAttempt(t, entered)
	result := awaitDialResult(t, resultCh)
	if result.err != nil {
		t.Fatal(result.err)
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialLossCancellationPrecedesAbortAndParentCancel(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	s.carrierStartCancel = parentCancel
	entered := make(chan dialTestAttempt, 1)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := dialAsync(s, caller)
	attempt := awaitDialAttempt(t, entered)
	s.abortCarrierHook = func(*wltpkg.Carrier) error {
		if attempt.ctx.Err() == nil {
			t.Error("pending lease not canceled before Abort")
		}
		if parent.Err() != nil {
			t.Error("carrier parent canceled before Abort")
		}
		var cause *localDialGenerationRetired
		if !errors.As(context.Cause(attempt.ctx), &cause) {
			t.Error("missing typed local cause")
		}
		return nil
	}
	s.NetworkUnavailable()
	if parent.Err() == nil {
		t.Fatal("carrier parent never canceled after Abort")
	}
	cancel()
	awaitDialResult(t, resultCh)
	assertNoDialLeases(t, s)
	// Cleanup may invoke the hook again after the ordered edge; do not interpret
	// that duplicate terminal close as a second loss event.
	s.abortCarrierHook = nil
}

func TestPendingDialPublishedStreamNeverReplayedAfterLoss(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	stream := &dialTestStream{}
	calls := 0
	s.dialStreamHook = func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error) {
		calls++
		return stream, nil
	}
	returned, err := s.DialStream(context.Background(), "eu", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = returned.Write([]byte("application payload"))
	s.NetworkUnavailable()
	if calls != 1 || stream.aborted.Load() != 0 || stream.writes.Load() != 1 {
		t.Fatal("published application stream was replayed or retained as pending")
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialStaleLeaseCannotCancelNewGeneration(t *testing.T) {
	s, a := newPendingDialTestService(t)
	old, err := s.acquireDialLease(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.NetworkUnavailable()
	b := &wltpkg.Carrier{}
	publishDialReplacement(t, s, b)
	fresh, err := s.acquireDialLease(context.Background(), old)
	if err != nil {
		t.Fatal(err)
	}
	// Retiring the retained old context again is not a service-wide cancellation.
	old.cancel(old.retired)
	if fresh.ctx.Err() != nil || fresh.carrier == a {
		t.Fatal("old lease canceled the new generation")
	}
	s.access.Lock()
	delete(s.pendingDialLeases, old)
	delete(s.pendingDialLeases, fresh)
	s.access.Unlock()
	fresh.cancel(nil)
	assertNoDialLeases(t, s)
}

func TestPendingDialInitialCarrierWaitConsumesConnectBudget(t *testing.T) {
	s := newLifecycleTestService(t)
	s.options.ConnectTimeout = badoption.Duration(30 * time.Millisecond)
	calls := 0
	s.dialStreamHook = func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error) {
		calls++
		return nil, io.EOF
	}
	_, err := s.DialStream(context.Background(), "eu", "example.com:443")
	if !errors.Is(err, context.DeadlineExceeded) || calls != 0 {
		t.Fatalf("wait budget err=%v calls=%d", err, calls)
	}
}

func TestPendingDialUnexpectedCarrierReplacementIsTerminal(t *testing.T) {
	s, a := newPendingDialTestService(t)
	entered := make(chan dialTestAttempt, 2)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	resultCh := dialAsync(s, context.Background())
	awaitDialAttempt(t, entered)
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return &wltpkg.Carrier{}, nil }
	s.restartCarrierCurrent(a, "unexplained heartbeat replacement")
	result := awaitDialResult(t, resultCh)
	if !errors.Is(result.err, errDialCarrierReplaced) || result.stream != nil {
		t.Fatalf("unexpected restart incorrectly recovered: %+v", result)
	}
	select {
	case <-entered:
		t.Fatal("OPEN retried after untrusted replacement")
	default:
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialTwoLossesKeepOneAbsoluteDeadline(t *testing.T) {
	s, a := newPendingDialTestService(t)
	b, c := &wltpkg.Carrier{}, &wltpkg.Carrier{}
	entered := make(chan dialTestAttempt, 3)
	s.dialStreamHook = func(ctx context.Context, carrier *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, carrier}
		if carrier == c {
			return &dialTestStream{}, nil
		}
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	deadline := time.Now().Add(800 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	resultCh := dialAsync(s, ctx)
	for _, want := range []*wltpkg.Carrier{a, b} {
		attempt := awaitDialAttempt(t, entered)
		got, _ := attempt.ctx.Deadline()
		if attempt.carrier != want || !got.Equal(deadline) {
			t.Fatal("wrong generation or renewed deadline")
		}
		s.NetworkUnavailable()
		replacement := b
		if want == b {
			replacement = c
		}
		publishDialReplacement(t, s, replacement)
	}
	third := awaitDialAttempt(t, entered)
	got, _ := third.ctx.Deadline()
	if third.carrier != c || !got.Equal(deadline) {
		t.Fatal("third attempt changed deadline")
	}
	result := awaitDialResult(t, resultCh)
	if result.err != nil {
		t.Fatal(result.err)
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialInitialWaitWakesOnRealServicePublication(t *testing.T) {
	s := newLifecycleTestService(t)
	carrier := &wltpkg.Carrier{}
	stream := &dialTestStream{}
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return carrier, nil }
	s.dialStreamHook = func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error) { return stream, nil }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resultCh := dialAsync(s, ctx)
	if err := s.Start(0); err != nil {
		t.Fatal(err)
	}
	result := awaitDialResult(t, resultCh)
	if result.err != nil || result.stream != stream {
		t.Fatalf("initial publication=%+v", result)
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialExistingCallerDeadlineLongerThanConfiguredIsPreserved(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	s.options.ConnectTimeout = badoption.Duration(20 * time.Millisecond)
	deadline := time.Now().Add(150 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	entered := make(chan dialTestAttempt, 1)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	resultCh := dialAsync(s, ctx)
	attempt := awaitDialAttempt(t, entered)
	if got, ok := attempt.ctx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatal("explicit original caller deadline was replaced")
	}
	s.NetworkUnavailable()
	result := awaitDialResult(t, resultCh)
	if !errors.Is(result.err, context.DeadlineExceeded) || time.Now().Before(deadline) {
		t.Fatalf("original caller bound changed: %v", result.err)
	}
	assertNoDialLeases(t, s)
}

func TestPendingDialPublishedRealStreamSurvivesLeaseAndCallerCancellation(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	left, right := net.Pipe()
	defer right.Close()
	var attemptCtx context.Context
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		attemptCtx = ctx
		return left, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := s.DialStream(ctx, "eu", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if attemptCtx.Err() != context.Canceled {
		t.Fatal("successful lease context was not released")
	}
	cancel()
	done := make(chan error, 1)
	go func() { _, err := stream.Write([]byte("published")); done <- err }()
	buffer := make([]byte, len("published"))
	_ = right.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(right, buffer); err != nil {
		t.Fatalf("lease cancellation terminated returned stream: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "published" {
		t.Fatal("payload changed")
	}
	assertNoDialLeases(t, s)
}
