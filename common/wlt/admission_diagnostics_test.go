//go:build with_wlt

package wlt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestActiveAdmissionDiagnosticSampleBuckets(t *testing.T) {
	now := time.Now()
	carrier := newTestCarrier(3, 1, 2, time.Second, time.Second)
	carrier.pressureIdleTimeout = 20 * time.Second
	fresh := &carrierConn{}
	fresh.lastActivity.Store(now.Add(-time.Second).UnixNano())
	idle := &carrierConn{}
	idle.lastActivity.Store(now.Add(-25 * time.Second).UnixNano())
	closing := &carrierConn{}
	closing.lastActivity.Store(now.Add(-30 * time.Second).UnixNano())
	closing.admissionClosing.Store(true)
	carrier.streams[fresh] = struct{}{}
	carrier.streams[idle] = struct{}{}
	carrier.streams[closing] = struct{}{}
	for range 3 {
		carrier.activeSlots <- struct{}{}
	}
	sample := carrier.sampleActiveAdmission(now)
	if sample.activeSlotsUsed != 3 || sample.registered != 3 || sample.fresh != 1 ||
		sample.pressureEligible != 1 || sample.closing != 1 || sample.oldestActivityMillis != 30000 {
		t.Fatalf("unexpected admission sample: %+v", sample)
	}
}

func TestActiveAdmissionDiagnosticBoundAndNilLogger(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.logActiveAdmissionDiagnostic("saturated", 0)
	if carrier.admissionDiagnosticEvents.Load() != 0 {
		t.Fatal("nil logger consumed diagnostic budget")
	}
	var mu sync.Mutex
	var lines []string
	carrier.logf = func(format string, args ...any) {
		mu.Lock()
		lines = append(lines, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	var callers sync.WaitGroup
	for range 128 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			carrier.logActiveAdmissionDiagnostic("saturated", 0)
		}()
	}
	callers.Wait()
	if len(lines) != 64 || carrier.admissionDiagnosticEvents.Load() != 64 {
		t.Fatalf("logged %d events; counter=%d, want 64", len(lines), carrier.admissionDiagnosticEvents.Load())
	}
	seen := make(map[int]bool)
	for _, line := range lines {
		var event int
		if _, err := fmt.Sscanf(line, "WLT active admission stage=saturated event=%d", &event); err != nil {
			t.Fatalf("missing event number: %q: %v", line, err)
		}
		if event < 1 || event > 64 || seen[event] {
			t.Fatalf("duplicate or invalid event number %d", event)
		}
		seen[event] = true
	}
}

func TestActiveAdmissionDiagnosticTimeoutOmitsTarget(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, 10*time.Millisecond)
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	var lines []string
	carrier.logf = func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}
	const secret = "private-target.example:443"
	_, err := carrier.acquireActiveSlot(context.Background(), "private-route", secret)
	if err == nil || carrier.rejectedActive.Load() != 1 || len(lines) != 2 {
		t.Fatalf("timeout err=%v rejected=%d events=%d", err, carrier.rejectedActive.Load(), len(lines))
	}
	if !strings.Contains(lines[0], "stage=saturated") || !strings.Contains(lines[1], "stage=wait_timeout") {
		t.Fatalf("unexpected diagnostic stages: %q", lines)
	}
	for _, line := range lines {
		if strings.Contains(line, secret) || strings.Contains(line, "private-route") ||
			strings.Contains(line, "target=") || strings.Contains(line, "route=") {
			t.Fatalf("private dial metadata in diagnostic: %q", line)
		}
	}
}

func TestActiveAdmissionDiagnosticCountsBlockedClose(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	left, right := net.Pipe()
	defer right.Close()
	started := make(chan struct{})
	releaseClose := make(chan struct{})
	stream := &carrierConn{
		Conn:          &blockingCloseConn{Conn: left, started: started, release: releaseClose},
		carrier:       carrier,
		releaseActive: func() { release(carrier.activeSlots); carrier.activeStreams.Add(-1) },
	}
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	if !carrier.registerStream(stream) {
		t.Fatal("failed to register test stream")
	}
	done := make(chan error, 1)
	go func() { done <- stream.Close() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Close did not enter underlying transport")
	}
	sample := carrier.sampleActiveAdmission(time.Now())
	if sample.registered != 1 || sample.closing != 1 || sample.activeSlotsUsed != 1 {
		t.Fatalf("blocked Close not visible: %+v", sample)
	}
	close(releaseClose)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(carrier.activeSlots) != 0 || carrier.activeStreams.Load() != 0 || carrier.closedStreams.Load() != 1 {
		t.Fatal("Close did not preserve active-slot release")
	}
}
