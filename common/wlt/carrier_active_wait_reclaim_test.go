//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func occupiedPressureSlot(t *testing.T, c *Carrier) (*carrierConn, net.Conn) {
	t.Helper()
	left, right := net.Pipe()
	c.activeSlots <- struct{}{}
	c.activeStreams.Add(1)
	stream := &carrierConn{
		Conn: left, carrier: c, routeClass: "direct",
		releaseActive: func() { release(c.activeSlots); c.activeStreams.Add(-1) },
	}
	if !c.registerStream(stream) {
		t.Fatal("failed to register occupied stream")
	}
	return stream, right
}

func TestActiveWaitTakesFreeSlotBeforeReclaim(t *testing.T) {
	c := newTestCarrier(2, 1, 1, time.Second, time.Second)
	c.pressureIdleTimeout = 50 * time.Millisecond
	stale, peer := occupiedPressureSlot(t, c)
	defer peer.Close()
	defer stale.Close()
	stale.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())

	// The other slot represents a concurrent normal close while a dial waits.
	// A reclaim-first retry would abort stale even though this slot is free.
	if !c.tryAcquireOrReclaimActiveSlot("eu") {
		t.Fatal("failed to take already-free active slot")
	}
	defer release(c.activeSlots)
	if stats := c.Stats(); stats.PressureIdleReclaims != 0 || stats.ActiveStreams != 1 {
		t.Fatalf("free slot caused unnecessary reclaim: %+v", stats)
	}
	if err := stale.Conn.SetReadDeadline(time.Now()); err != nil {
		t.Fatalf("stale stream was closed despite free slot: %v", err)
	}
}

func TestActiveWaitReclaimsStreamThatBecomesIdle(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, 300*time.Millisecond)
	c.pressureIdleTimeout = 70 * time.Millisecond
	first, peer := occupiedPressureSlot(t, c)
	defer peer.Close()
	defer first.Close()
	result := make(chan error, 1)
	go func() {
		releaseActive, err := c.acquireActiveSlot(context.Background(), "eu", "two.example:443")
		if err == nil {
			releaseActive()
		}
		result <- err
	}()
	waitForCarrierStat(t, c, func(s CarrierStats) bool { return s.PendingDials == 1 })
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("idle stream did not admit queued dial: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("queued dial missed pressure-idle eligibility")
	}
	stats := c.Stats()
	if stats.PressureIdleReclaims != 1 || stats.RejectedActive != 0 || stats.ActiveStreams != 0 {
		t.Fatalf("unexpected pressure admission stats: %+v", stats)
	}
}

func TestActiveWaitPreservesRecentlyActiveStream(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, 125*time.Millisecond)
	c.pressureIdleTimeout = 70 * time.Millisecond
	first, peer := occupiedPressureSlot(t, c)
	defer peer.Close()
	defer first.Close()
	stop := make(chan struct{})
	var activity sync.WaitGroup
	activity.Add(1)
	go func() {
		defer activity.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				first.markActivity()
			case <-stop:
				return
			}
		}
	}()
	_, err := c.acquireActiveSlot(context.Background(), "eu", "two.example:443")
	close(stop)
	activity.Wait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recent stream wait error=%v, want queue deadline", err)
	}
	if err := first.Conn.SetReadDeadline(time.Now()); err != nil {
		t.Fatalf("recent stream was closed: %v", err)
	}
	stats := c.Stats()
	if stats.PressureIdleReclaims != 0 || stats.RejectedActive != 1 || stats.ActiveStreams != 1 {
		t.Fatalf("recent stream stats: %+v", stats)
	}
}

func TestActiveWaitCancellationAndDeadlineDoNotReclaim(t *testing.T) {
	for _, tc := range []struct {
		name   string
		queue  time.Duration
		cancel bool
		want   error
	}{
		{name: "caller_cancel", queue: 200 * time.Millisecond, cancel: true, want: context.Canceled},
		{name: "queue_deadline", queue: 25 * time.Millisecond, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCarrier(1, 1, 1, time.Second, tc.queue)
			c.pressureIdleTimeout = 100 * time.Millisecond
			first, peer := occupiedPressureSlot(t, c)
			defer peer.Close()
			defer first.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				time.AfterFunc(20*time.Millisecond, cancel)
			}
			_, err := c.acquireActiveSlot(ctx, "eu", "two.example:443")
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v, want %v", err, tc.want)
			}
			stats := c.Stats()
			if stats.PressureIdleReclaims != 0 || stats.RejectedActive != 1 || stats.ActiveStreams != 1 {
				t.Fatalf("canceled/deadline stats: %+v", stats)
			}
		})
	}
}

func TestActiveWaitConcurrentReclaimOnce(t *testing.T) {
	c := newTestCarrier(1, 1, 12, time.Second, 250*time.Millisecond)
	c.pressureIdleTimeout = 60 * time.Millisecond
	first, peer := occupiedPressureSlot(t, c)
	defer peer.Close()
	defer first.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			releaseActive, err := c.acquireActiveSlot(ctx, "eu", "two.example:443")
			if err == nil {
				releaseActive()
			}
		}()
	}
	wg.Wait()
	if stats := c.Stats(); stats.PressureIdleReclaims != 1 || stats.ActiveStreams != 0 {
		t.Fatalf("concurrent reclaim stats: %+v", stats)
	}
}
