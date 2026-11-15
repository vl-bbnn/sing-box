//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/log"
)

type dialCorrelationLogger struct {
	log.ContextLogger
	mu    sync.Mutex
	lines []string
}

func (l *dialCorrelationLogger) Info(args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprint(args...))
	l.mu.Unlock()
}

func (l *dialCorrelationLogger) has(parts ...string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		matched := true
		for _, part := range parts {
			matched = matched && strings.Contains(line, part)
		}
		if matched {
			return true
		}
	}
	return false
}

func (l *dialCorrelationLogger) correlationLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var lines []string
	for _, line := range l.lines {
		if strings.Contains(line, "wlt dial correlation event=") {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestDialCorrelationRecordsRetiredCallerOutcome(t *testing.T) {
	for _, outcome := range []string{"published", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			s, old := newPendingDialTestService(t)
			logger := &dialCorrelationLogger{ContextLogger: s.logger}
			s.logger = logger
			entered := make(chan struct{}, 1)
			s.dialStreamHook = func(ctx context.Context, carrier *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
				if carrier != old {
					return &dialTestStream{}, nil
				}
				entered <- struct{}{}
				<-ctx.Done()
				if outcome == "failed" {
					return nil, io.ErrClosedPipe
				}
				return nil, context.Cause(ctx)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resultCh := dialAsync(s, ctx)
			<-entered
			s.access.RLock()
			var firstAttemptID uint64
			for lease := range s.pendingDialLeases {
				firstAttemptID = lease.attemptID
			}
			s.access.RUnlock()
			if firstAttemptID == 0 {
				t.Fatal("initial dial attempt missing")
			}
			s.NetworkUnavailable()
			if outcome == "published" {
				publishDialReplacement(t, s, &wltpkg.Carrier{})
			}
			result := awaitDialResult(t, resultCh)
			if (result.err == nil) != (outcome == "published") {
				t.Fatalf("result=%+v", result)
			}
			logicalID := fmt.Sprint("logical_dial_id=", firstAttemptID)
			if !logger.has("event=retired", logicalID, fmt.Sprint("dial_attempt_id=", firstAttemptID), "retry="+map[bool]string{true: "true", false: "false"}[outcome == "published"]) || !logger.has("event=final", logicalID, "outcome="+outcome) {
				t.Fatalf("missing correlation outcome: %+v", logger.lines)
			}
			for _, line := range logger.correlationLines() {
				t.Logf("GO_CAPTURE_SERVICE %s", line)
			}
			assertNoDialLeases(t, s)
		})
	}
}

func TestDialCorrelationCallerCancellationAfterLocalRetirement(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	logger := &dialCorrelationLogger{ContextLogger: s.logger}
	s.logger = logger
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- struct{}{}
		<-ctx.Done()
		<-release
		return nil, context.Cause(ctx)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := dialAsync(s, ctx)
	<-entered
	s.NetworkUnavailable()
	cancel()
	close(release)
	result := awaitDialResult(t, resultCh)
	if result.stream != nil || !errors.Is(result.err, context.Canceled) {
		t.Fatalf("caller cancellation outcome=%+v", result)
	}
	if !logger.has("event=retired", "retry=false") || !logger.has("event=final", "outcome=failed") {
		t.Fatalf("caller cancellation misclassified: %+v", logger.lines)
	}
	for _, line := range logger.correlationLines() {
		t.Logf("GO_CAPTURE_SERVICE %s", line)
	}
	assertNoDialLeases(t, s)
}
