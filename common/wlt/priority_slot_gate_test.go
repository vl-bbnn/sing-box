//go:build with_wlt

package wlt

import (
	"context"
	"testing"
	"time"
)

func TestDNSOpenTargetClassification(t *testing.T) {
	tests := map[string]bool{
		"10.255.255.1:53":       true,
		"dns.example.com:853":   true,
		"[2001:db8::53]:53":     true,
		"www.example.com:443":   false,
		"10.255.255.1:5353":     false,
		"missing-port.example":  false,
		" malformed.example:53": true,
	}
	for target, want := range tests {
		if got := isDNSOpenTarget(target); got != want {
			t.Errorf("isDNSOpenTarget(%q)=%t, want %t", target, got, want)
		}
	}
}

func TestPrioritySlotGateUnusedReserveIsBorrowable(t *testing.T) {
	gate := newPrioritySlotGate(4, 1)
	for index := 0; index < 4; index++ {
		if !gate.tryAcquire(false) {
			t.Fatalf("normal acquisition %d unexpectedly blocked", index+1)
		}
	}
	if gate.tryAcquire(false) {
		t.Fatal("acquisition above total capacity succeeded")
	}
	assertPrioritySlotGateState(t, gate, 4, 0, 4, 0)
	for index := 0; index < 4; index++ {
		gate.release(false)
	}
}

func TestPrioritySlotGateNormalSaturationDoesNotBlockPriority(t *testing.T) {
	gate := newPrioritySlotGate(4, 1)
	for index := 0; index < 4; index++ {
		if !gate.tryAcquire(false) {
			t.Fatalf("normal acquisition %d unexpectedly blocked", index+1)
		}
	}

	priorityDone := make(chan error, 1)
	go func() {
		priorityDone <- gate.acquire(context.Background(), true)
	}()
	waitForPrioritySlotWaiters(t, gate, 1)
	gate.release(false)
	select {
	case err := <-priorityDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("priority acquisition did not receive the next released slot")
	}
	assertPrioritySlotGateState(t, gate, 4, 1, 3, 0)
	gate.release(true)
	for index := 0; index < 3; index++ {
		gate.release(false)
	}
}

func TestPrioritySlotGateGuaranteesBothClassesUnderPressure(t *testing.T) {
	gate := newPrioritySlotGate(4, 1)
	for index := 0; index < 4; index++ {
		if !gate.tryAcquire(false) {
			t.Fatalf("normal acquisition %d unexpectedly blocked", index+1)
		}
	}

	normalDone := make(chan error, 1)
	priorityDone := make(chan error, 1)
	go func() { normalDone <- gate.acquire(context.Background(), false) }()
	waitForPrioritySlotWaiters(t, gate, 1)
	go func() { priorityDone <- gate.acquire(context.Background(), true) }()
	waitForPrioritySlotWaiters(t, gate, 2)

	gate.release(false)
	select {
	case err := <-priorityDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("priority class did not reclaim its reserved slot")
	}
	select {
	case <-normalDone:
		t.Fatal("normal waiter acquired before its guaranteed share had room")
	default:
	}

	gate.release(false)
	select {
	case err := <-normalDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("normal class starved after its guaranteed share had room")
	}
	assertPrioritySlotGateState(t, gate, 4, 1, 3, 0)

	gate.release(true)
	for index := 0; index < 3; index++ {
		gate.release(false)
	}
}

func TestPrioritySlotGateCancellationRemovesWaiterAndPreservesCapacity(t *testing.T) {
	gate := newPrioritySlotGate(2, 1)
	if !gate.tryAcquire(false) || !gate.tryAcquire(false) {
		t.Fatal("failed to borrow the full gate for normal work")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := gate.acquire(ctx, true); err == nil {
		t.Fatal("expected canceled priority acquisition")
	}
	assertPrioritySlotGateState(t, gate, 2, 0, 2, 0)
	gate.release(false)
	gate.release(false)
	if !gate.tryAcquire(true) || !gate.tryAcquire(false) {
		t.Fatal("canceled waiter leaked capacity")
	}
	gate.release(true)
	gate.release(false)
}

func assertPrioritySlotGateState(t *testing.T, gate *prioritySlotGate, total int, priority int, normal int, waiters int) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.total != total || gate.priority != priority || gate.normal != normal || len(gate.waiters) != waiters {
		t.Fatalf("gate state total=%d priority=%d normal=%d waiters=%d, want %d/%d/%d/%d", gate.total, gate.priority, gate.normal, len(gate.waiters), total, priority, normal, waiters)
	}
}

func waitForPrioritySlotWaiters(t *testing.T, gate *prioritySlotGate, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		got := len(gate.waiters)
		gate.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d gate waiters", want)
}
