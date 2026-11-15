//go:build with_wlt

package wlt

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type closeDiagnosticConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
	aborted atomic.Bool
	closed  atomic.Bool
}

func (c *closeDiagnosticConn) Close() error {
	close(c.started)
	<-c.release
	c.closed.Store(true)
	return c.Conn.Close()
}
func (c *closeDiagnosticConn) Abort() error {
	c.aborted.Store(true)
	c.closed.Store(true)
	return c.Conn.Close()
}
func (c *closeDiagnosticConn) FlowID() uint16 { return 42 }
func (c *closeDiagnosticConn) DiagnosticFields() []any {
	if !c.closed.Load() {
		return []any{"kcp_wait_snd", 3, "kcp_underlying_pending", 2, "kcp_read_errors", int64(1),
			"kcp_close_reason", "read_error:eof", "mux_flow_close_reason", "remote_close"}
	}
	return []any{"kcp_close_ms", int64(80), "kcp_wait_snd", 0, "kcp_underlying_pending", 0,
		"kcp_drain_calls", int64(1), "kcp_drain_ms", int64(70), "kcp_drain_result", "acked",
		"mux_close_ms", int64(10), "mux_close_wait_ms", int64(7),
		"mux_close_control_ms", int64(3), "mux_close_control_error", int64(0),
		"mux_flow_close_reason", "local_close_notified"}
}

func closeLifecycleTestStream(t *testing.T) (*Carrier, *carrierConn, *closeDiagnosticConn, *[]string) {
	t.Helper()
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.closeLifecycleDiagnostics = true
	lines := new([]string)
	var mu sync.Mutex
	carrier.logf = func(format string, args ...any) {
		if !carrier.streamsMu.TryLock() {
			t.Error("logger called while registry lock held")
		} else {
			carrier.streamsMu.Unlock()
		}
		mu.Lock()
		*lines = append(*lines, formatWLTCloseTest(format, args...))
		mu.Unlock()
	}
	left, right := net.Pipe()
	t.Cleanup(func() { _ = right.Close() })
	underlying := &closeDiagnosticConn{Conn: left, started: make(chan struct{}), release: make(chan struct{})}
	stream := &carrierConn{Conn: underlying, carrier: carrier, releaseActive: func() { release(carrier.activeSlots); carrier.activeStreams.Add(-1) }}
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	if !carrier.registerStream(stream) {
		t.Fatal("register stream")
	}
	return carrier, stream, underlying, lines
}

func formatWLTCloseTest(format string, args ...any) string { return fmt.Sprintf(format, args...) }

func TestCloseLifecycleRealPermitReleaseAfterDelayedClose(t *testing.T) {
	carrier, stream, underlying, lines := closeLifecycleTestStream(t)
	done := make(chan error, 1)
	go func() { done <- stream.Close() }()
	select {
	case <-underlying.started:
	case <-time.After(time.Second):
		t.Fatal("underlying close not reached")
	}
	if len(*lines) != 2 || !strings.Contains((*lines)[0], "phase=start event=1") ||
		!strings.Contains((*lines)[0], "flow_id=42") || !strings.Contains((*lines)[0], "kcp_wait_snd=3 mux_pending=2 kcp_read_errors=1 kcp_close_reason=read_error:eof mux_close_reason=remote_close") ||
		!strings.Contains((*lines)[0], "draining=1 drain_slots=1 hard_ceiling=2 permit_owner=drain") ||
		!strings.Contains((*lines)[1], "WLT close drain ownership event=1 live=0 draining=1 drain_capacity=1 total_ceiling=2") {
		t.Fatalf("missing start diagnostic: %q", *lines)
	}
	if carrier.activeStreams.Load() != 0 || len(carrier.activeSlots) != 0 ||
		carrier.drainingStreams.Load() != 1 || len(carrier.drainSlots) != 1 || !stream.admissionClosing.Load() {
		t.Fatal("drain ownership transition missing")
	}
	close(underlying.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(*lines) != 3 || !strings.Contains((*lines)[2], "phase=end event=1") ||
		!strings.Contains((*lines)[2], "kcp_drain_result=acked") ||
		!strings.Contains((*lines)[2], "mux_close_control_ms=3") ||
		!strings.Contains((*lines)[2], "active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=2 retired_owner=drain") {
		t.Fatalf("missing end diagnostic: %q", *lines)
	}
	if carrier.activeStreams.Load() != 0 || len(carrier.activeSlots) != 0 ||
		carrier.drainingStreams.Load() != 0 || len(carrier.drainSlots) != 0 || carrier.closedStreams.Load() != 1 {
		t.Fatal("permit accounting")
	}
	if err := stream.Close(); err != nil || len(*lines) != 3 {
		t.Fatal("close emitted twice")
	}
}

func TestCloseLifecycleAbortImmediateAndOnlyOnce(t *testing.T) {
	carrier, stream, underlying, lines := closeLifecycleTestStream(t)
	if err := stream.Abort(); err != nil {
		t.Fatal(err)
	}
	if !underlying.aborted.Load() || len(*lines) != 2 || !strings.Contains((*lines)[0], "mode=abort") ||
		!strings.Contains((*lines)[1], "mode=abort") || carrier.activeStreams.Load() != 0 {
		t.Fatalf("abort diagnostics %q", *lines)
	}
	if err := stream.Close(); err != nil || len(*lines) != 2 {
		t.Fatal("abort/close emitted twice")
	}
}

func TestCloseLifecycleOptInAndBound(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	var markers []string
	c.logf = func(format string, args ...any) { markers = append(markers, fmt.Sprintf(format, args...)) }
	if c.reserveCloseLifecycleEvent() != 0 {
		t.Fatal("diagnostic emitted without enablement")
	}
	c.closeLifecycleDiagnostics = true
	for i := uint64(1); i <= maxCloseLifecycleEvents; i++ {
		if got := c.reserveCloseLifecycleEvent(); got != i {
			t.Fatalf("event %d got %d", i, got)
		}
	}
	if c.reserveCloseLifecycleEvent() != 0 {
		t.Fatal("diagnostic cap exceeded")
	}
	if c.reserveCloseLifecycleEvent() != 0 || len(markers) != 1 ||
		!strings.Contains(markers[0], "phase=cap_exhausted event=4097") ||
		!strings.Contains(markers[0], "cap=4096") {
		t.Fatalf("cap marker count/content: %q", markers)
	}
}

func TestCloseLifecycleConcurrentCapMarkerOnce(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.closeLifecycleDiagnostics = true
	c.closeLifecycleEvents.Store(maxCloseLifecycleEvents)
	var mu sync.Mutex
	var markers []string
	c.logf = func(format string, args ...any) {
		mu.Lock()
		markers = append(markers, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	var workers sync.WaitGroup
	for range 100 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if event := c.reserveCloseLifecycleEvent(); event != 0 {
				t.Errorf("overflow event=%d", event)
			}
		}()
	}
	workers.Wait()
	if len(markers) != 1 || !strings.Contains(markers[0], "phase=cap_exhausted event=4097") {
		t.Fatalf("cap marker count/content: %q", markers)
	}
}

func TestCloseLifecycleUnknownDiagnosticStringsAreNotCopied(t *testing.T) {
	fields := []any{"kcp_drain_result", "token=SECRET_CANARY", "mux_flow_close_reason", "peer=SECRET_CANARY", "kcp_close_reason", "read_error:SECRET_CANARY", "kcp_wait_snd", -7}
	if got := closeDiagnosticEnum(fields, "kcp_drain_result", "acked", "timeout"); got != "other" {
		t.Fatal(got)
	}
	if got := closeDiagnosticEnum(fields, "mux_flow_close_reason", "local_close_notified"); got != "other" {
		t.Fatal(got)
	}
	if got := closeDiagnosticEnum(fields, "kcp_close_reason", "read_error:eof"); got != "other" {
		t.Fatal(got)
	}
	if got := closeDiagnosticNumber(fields, "kcp_wait_snd"); got != 0 {
		t.Fatal(got)
	}
}
