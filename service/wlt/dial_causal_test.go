//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
)

func TestDialCausalContextMatchesServiceRecord(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	logger := &dialCorrelationLogger{ContextLogger: s.logger}
	s.logger = logger
	seen := make(chan carriercommon.OpenDiagnostic, 1)
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		seen <- carriercommon.OpenDiagnosticFromContext(ctx)
		return &dialTestStream{}, nil
	}
	result := awaitDialResult(t, dialAsync(s, context.Background()))
	if result.err != nil || result.stream == nil {
		t.Fatalf("dial result=%+v", result)
	}
	defer result.stream.Close()
	diagnostic := <-seen
	if diagnostic.LogicalDialID == 0 || diagnostic.LogicalDialID != diagnostic.DialAttemptID ||
		diagnostic.CarrierGeneration == 0 {
		t.Fatalf("missing diagnostic join: %+v", diagnostic)
	}
	if !logger.has("wlt dial causal event=admitted", "dial_attempt_id=") ||
		!logger.has("wlt dial causal event=settled", "outcome=published", "cause=none") {
		t.Fatalf("missing privacy-safe service records: %+v", logger.lines)
	}
}

func TestDialCausalFinalDeadlinePrecedesObservedCarrierError(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	logger := &dialCorrelationLogger{ContextLogger: s.logger}
	s.logger = logger
	entered := make(chan struct{})
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		close(entered)
		<-ctx.Done()
		// The carrier reports a real error concurrently with the caller's
		// deadline. Service returns its terminal deadline by existing policy.
		return nil, io.ErrClosedPipe
	}
	caller, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := dialAsync(s, caller)
	awaitLifecycleEvent(t, entered, "OPEN")
	result := awaitDialResult(t, done)
	if result.stream != nil || !errors.Is(result.err, context.DeadlineExceeded) || errors.Is(result.err, io.ErrClosedPipe) {
		t.Fatalf("service return precedence changed: %+v", result)
	}
	if !logger.has("wlt dial causal event=settled", "outcome=failed", "cause=deadline", "observed_carrier_error=true") {
		t.Fatalf("final cause misreported: %+v", logger.lines)
	}
	var admitted, settled int
	for _, line := range logger.lines {
		if strings.Contains(line, "wlt dial causal event=admitted") {
			admitted++
		}
		if strings.Contains(line, "wlt dial causal event=settled") {
			settled++
		}
	}
	if admitted != 1 || settled != 1 {
		t.Fatalf("admitted/settled records=%d/%d, want 1/1", admitted, settled)
	}
	assertNoDialLeases(t, s)
}

func TestTimedOutCausalDrainStillRejectsLatePublication(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	entered, release := make(chan struct{}), make(chan struct{})
	late := &dialTestStream{}
	s.dialStreamHook = func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error) {
		close(entered)
		<-release
		return late, nil
	}
	done := dialAsync(s, context.Background())
	awaitLifecycleEvent(t, entered, "OPEN")
	drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.QuiescePendingDials(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain error=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	result := awaitDialResult(t, done)
	if result.stream != nil || !errors.Is(result.err, context.Canceled) || late.aborted.Load() != 1 {
		t.Fatalf("late OPEN published after timeout/close: %+v, aborts=%d", result, late.aborted.Load())
	}
	assertNoDialLeases(t, s)
}
