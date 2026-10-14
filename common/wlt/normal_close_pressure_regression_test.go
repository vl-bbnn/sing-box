//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockedNormalCloseFixture struct {
	carrier *Carrier
	stream  *carrierConn
	release func()
	done    <-chan error
}

func newBlockedNormalCloseFixture(t *testing.T) blockedNormalCloseFixture {
	t.Helper()
	carrier := newTestCarrier(1, 1, 1, time.Second, 200*time.Millisecond)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	left, right := net.Pipe()
	started := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	done := make(chan error, 1)
	stream := &carrierConn{
		Conn:          &blockingCloseConn{Conn: left, started: started, release: unblock},
		carrier:       carrier,
		routeClass:    "direct",
		releaseActive: func() { release(carrier.activeSlots); carrier.activeStreams.Add(-1) },
	}
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	if !carrier.registerStream(stream) {
		t.Fatal("failed to register stream")
	}
	stream.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	go func() { done <- stream.Close() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(unblock)
		t.Fatal("normal Close did not start")
	}
	releaseClose := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(func() {
		releaseClose()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Close failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("blocked Close goroutine did not exit")
		}
		_ = right.Close()
	})
	if stream.idleClosing.Load() || !stream.admissionClosing.Load() {
		t.Fatal("fixture did not isolate ordinary Close in progress")
	}
	return blockedNormalCloseFixture{carrier: carrier, stream: stream, release: releaseClose, done: done}
}

func TestCarrierNormalCloseDoesNotBlockAdmissionDeadline(t *testing.T) {
	f := newBlockedNormalCloseFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		releaseActive, err := f.carrier.acquireActiveSlot(ctx, "eu", "example.com:443")
		if releaseActive != nil {
			releaseActive()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("admission error=%v, want deadline exceeded", err)
		}
	case <-time.After(150 * time.Millisecond):
		// Always unblock and join the baseline's stuck dial before failing.
		f.release()
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("dial goroutine did not exit after Close released")
		}
		t.Fatal("admission remained blocked behind ordinary Close after its deadline")
	}
	if got := f.carrier.pressureIdleReclaims.Load(); got != 0 {
		t.Fatalf("normal closing stream was reclaimed: %d", got)
	}
}

func TestCarrierNormalCloseReleaseAndCancellation(t *testing.T) {
	t.Run("release admits", func(t *testing.T) {
		f := newBlockedNormalCloseFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() {
			releaseActive, err := f.carrier.acquireActiveSlot(ctx, "eu", "example.com:443")
			if releaseActive != nil {
				releaseActive()
			}
			result <- err
		}()
		time.Sleep(20 * time.Millisecond)
		f.release()
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("released slot was not admitted: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("dial did not wake on normal Close release")
		}
		if got := f.carrier.pressureIdleReclaims.Load(); got != 0 {
			t.Fatalf("normal closing stream was reclaimed: %d", got)
		}
	})
	t.Run("cancellation returns", func(t *testing.T) {
		f := newBlockedNormalCloseFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			releaseActive, err := f.carrier.acquireActiveSlot(ctx, "eu", "example.com:443")
			if releaseActive != nil {
				releaseActive()
			}
			result <- err
		}()
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("admission error=%v, want cancellation", err)
			}
		case <-time.After(150 * time.Millisecond):
			f.release()
			select {
			case <-result:
			case <-time.After(time.Second):
				t.Fatal("dial goroutine did not exit after Close released")
			}
			t.Fatal("canceled admission remained blocked behind ordinary Close")
		}
	})
}

func TestCarrierPressureCandidatesExcludeClosingStreams(t *testing.T) {
	f := newBlockedNormalCloseFixture(t)
	if f.carrier.reclaimPressureIdleStream("eu") {
		t.Fatal("ordinary closing stream was selected for reclaim")
	}
	// The existing idle watcher flag remains a separate skip condition.
	f.stream.idleClosing.Store(true)
	if f.carrier.reclaimPressureIdleStream("eu") {
		t.Fatal("idle closing stream was selected for reclaim")
	}
}

func TestCarrierPressureCandidateIdleClosingOnlySkipped(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	stream := &carrierConn{Conn: left, carrier: carrier, routeClass: "direct"}
	if !carrier.registerStream(stream) {
		t.Fatal("failed to register stream")
	}
	stream.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	stream.idleClosing.Store(true)
	if stream.admissionClosing.Load() {
		t.Fatal("test stream unexpectedly entered Close")
	}
	if carrier.reclaimPressureIdleStream("eu") {
		t.Fatal("idle watcher closing stream was selected for reclaim")
	}
}

func TestCarrierPressureCandidateNonclosingStillReclaimable(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	left, right := net.Pipe()
	defer right.Close()
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	stream := &carrierConn{
		Conn: left, carrier: carrier, routeClass: "direct",
		releaseActive: func() { release(carrier.activeSlots); carrier.activeStreams.Add(-1) },
	}
	if !carrier.registerStream(stream) {
		t.Fatal("failed to register stream")
	}
	stream.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	if !carrier.reclaimPressureIdleStream("eu") {
		t.Fatal("old nonclosing stream was not reclaimed")
	}
	if carrier.pressureIdleReclaims.Load() != 1 || carrier.activeStreams.Load() != 0 || len(carrier.activeSlots) != 0 {
		t.Fatal("nonclosing reclaim did not release the active slot")
	}
}

func TestCarrierSelectionThenOrdinaryCloseSkipsReclaimClaim(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, 200*time.Millisecond)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	left, right := net.Pipe()
	defer right.Close()
	started := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	releaseClose := func() { once.Do(func() { close(unblock) }) }
	underlay := &blockingCloseConn{Conn: left, started: started, release: unblock}
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	stream := &carrierConn{
		Conn: underlay, carrier: carrier, routeClass: "direct",
		releaseActive: func() { release(carrier.activeSlots); carrier.activeStreams.Add(-1) },
	}
	if !carrier.registerStream(stream) {
		t.Fatal("registration failed")
	}
	stream.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	selected := carrier.selectPressureIdleStream("eu", nil)
	if selected != stream {
		t.Fatal("old stream was not selected")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- stream.Close() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		releaseClose()
		t.Fatal("normal Close did not enter underlying transport")
	}
	claimDone := make(chan bool, 1)
	go func() { claimDone <- carrier.tryReclaimPressureIdleStream(selected, "eu") }()
	var claimBlocked bool
	select {
	case claimed := <-claimDone:
		if claimed {
			t.Error("reclaim took a stream already inside normal Close")
		}
	case <-time.After(120 * time.Millisecond):
		claimBlocked = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	dialDone := make(chan error, 1)
	go func() {
		releaseActive, err := carrier.acquireActiveSlot(ctx, "eu", "example.com:443")
		if releaseActive != nil {
			releaseActive()
		}
		dialDone <- err
	}()
	var dialBlocked bool
	select {
	case err := <-dialDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("admission error=%v, want deadline exceeded", err)
		}
	case <-time.After(120 * time.Millisecond):
		dialBlocked = true
	}
	releaseClose()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("normal Close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("normal Close leaked")
	}
	if claimBlocked {
		select {
		case <-claimDone:
		case <-time.After(time.Second):
			t.Fatal("claim goroutine leaked")
		}
		t.Error("claim waited behind normal Close")
	}
	if dialBlocked {
		select {
		case <-dialDone:
		case <-time.After(time.Second):
			t.Fatal("dial goroutine leaked")
		}
		t.Error("admission waited past its caller deadline")
	}
	if carrier.pressureIdleReclaims.Load() != 0 || carrier.closedStreams.Load() != 1 || carrier.activeStreams.Load() != 0 || len(carrier.activeSlots) != 0 || len(carrier.streams) != 0 {
		t.Fatalf("unexpected close/reclaim accounting: %+v", carrier.Stats())
	}
}

type trackedAbortConn struct {
	net.Conn
	closes atomic.Int64
	aborts atomic.Int64
}

func (c *trackedAbortConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

func (c *trackedAbortConn) Abort() error {
	c.aborts.Add(1)
	return c.Conn.Close()
}

func TestCarrierReclaimClaimOwnsSingleClose(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	left, right := net.Pipe()
	defer right.Close()
	underlay := &trackedAbortConn{Conn: left}
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	stream := &carrierConn{
		Conn: underlay, carrier: carrier, routeClass: "direct",
		releaseActive: func() { release(carrier.activeSlots); carrier.activeStreams.Add(-1) },
	}
	if !carrier.registerStream(stream) {
		t.Fatal("registration failed")
	}
	stream.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	selected := carrier.selectPressureIdleStream("eu", nil)
	if selected != stream || !carrier.tryReclaimPressureIdleStream(selected, "eu") {
		t.Fatal("old stream was not claimed for reclaim")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if underlay.aborts.Load() != 1 || underlay.closes.Load() != 0 || carrier.closedStreams.Load() != 1 || carrier.activeStreams.Load() != 0 || len(carrier.activeSlots) != 0 || len(carrier.streams) != 0 {
		t.Fatalf("double close or slot leak: aborts=%d closes=%d stats=%+v", underlay.aborts.Load(), underlay.closes.Load(), carrier.Stats())
	}
}

type blockedReclaimAbortConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
}

func (c *blockedReclaimAbortConn) Abort() error {
	close(c.started)
	<-c.release
	return c.Conn.Close()
}

func TestCarrierReclaimOwnerStaysRegisteredUntilAbortFinishes(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	left, right := net.Pipe()
	defer right.Close()
	underlay := &blockedReclaimAbortConn{Conn: left, started: make(chan struct{}), release: make(chan struct{})}
	carrier.activeSlots <- struct{}{}
	carrier.activeStreams.Add(1)
	stream := &carrierConn{Conn: underlay, carrier: carrier, routeClass: "direct",
		releaseActive: func() { release(carrier.activeSlots); carrier.activeStreams.Add(-1) }}
	if !carrier.registerStream(stream) {
		t.Fatal("registration failed")
	}
	stream.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	done := make(chan bool, 1)
	go func() { done <- carrier.tryReclaimPressureIdleStream(stream, "eu") }()
	select {
	case <-underlay.started:
	case <-time.After(time.Second):
		t.Fatal("reclaim never reached underlay")
	}
	carrier.streamsMu.Lock()
	_, registered := carrier.streams[stream]
	carrier.streamsMu.Unlock()
	if !registered || !stream.admissionClosing.Load() || carrier.activeStreams.Load() != 1 {
		close(underlay.release)
		t.Fatal("blocked reclaim lost registered owner or active accounting")
	}
	if carrier.tryReclaimPressureIdleStream(stream, "eu") {
		close(underlay.release)
		t.Fatal("same stream was claimed twice")
	}
	close(underlay.release)
	select {
	case reclaimed := <-done:
		if !reclaimed {
			t.Fatal("reclaim failed")
		}
	case <-time.After(time.Second):
		t.Fatal("reclaim owner did not finish")
	}
	carrier.streamsMu.Lock()
	remaining := len(carrier.streams)
	carrier.streamsMu.Unlock()
	if remaining != 0 || carrier.activeStreams.Load() != 0 || len(carrier.activeSlots) != 0 || carrier.closedStreams.Load() != 1 {
		t.Fatalf("reclaim completion leaked ownership: remaining=%d stats=%+v", remaining, carrier.Stats())
	}
}

func TestCarrierReclaimSkipsBusyCandidatesWithoutWaiting(t *testing.T) {
	carrier := newTestCarrier(2, 1, 1, time.Second, time.Second)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	var peers []net.Conn
	var locked []*carrierConn
	var releaseOnce sync.Once
	releaseLocks := func() {
		releaseOnce.Do(func() {
			for _, stream := range locked {
				stream.closeMu.Unlock()
			}
		})
	}
	defer releaseLocks()
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	for range 2 {
		left, right := net.Pipe()
		peers = append(peers, left, right)
		stream := &carrierConn{Conn: left, carrier: carrier, routeClass: "direct"}
		if !carrier.registerStream(stream) {
			t.Fatal("registration failed")
		}
		stream.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
		stream.closeMu.Lock() // The interval after normal Close claims the lock.
		locked = append(locked, stream)
		carrier.activeSlots <- struct{}{}
		carrier.activeStreams.Add(1)
	}
	done := make(chan bool, 1)
	go func() { done <- carrier.reclaimPressureIdleStream("eu") }()
	timedOut := false
	select {
	case reclaimed := <-done:
		if reclaimed {
			t.Fatal("busy close candidate was reclaimed")
		}
	case <-time.After(150 * time.Millisecond):
		timedOut = true
	}
	if timedOut {
		releaseLocks()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("reclaim goroutine did not exit after locks released")
		}
		t.Fatal("reclaim waited or spun on busy close candidates")
	}
	if carrier.pressureIdleReclaims.Load() != 0 || carrier.activeStreams.Load() != 2 || len(carrier.streams) != 2 {
		t.Fatalf("unexpected reclaim accounting: %+v", carrier.Stats())
	}
}
