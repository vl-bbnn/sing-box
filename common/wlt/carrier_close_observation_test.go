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

type erroringCloseConn struct {
	net.Conn
	err error
}

func (c *erroringCloseConn) Close() error {
	_ = c.Conn.Close()
	return c.err
}

func TestCarrierCloseKeepsParentAliveUntilPublishedStreamDrains(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.done = make(chan struct{})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	carrier.cancel = cancel
	local, peer := net.Pipe()
	defer peer.Close()
	release := make(chan struct{})
	started := make(chan struct{})
	carrier.activeStreams.Store(1)
	carrier.openedStreams.Store(1)
	stream := &carrierConn{Conn: &blockingCloseConn{Conn: local, started: started, release: release},
		carrier: carrier, releaseActive: func() { carrier.activeStreams.Add(-1) }}
	if !carrier.registerStream(stream) {
		t.Fatal("stream registration failed")
	}
	streamDone := make(chan struct{})
	go func() { _ = stream.Close(); close(streamDone) }()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- carrier.Close() }()
	select {
	case <-closeDone:
		close(release)
		t.Fatal("carrier close returned before published stream drain")
	case <-time.After(20 * time.Millisecond):
	}
	if parent.Err() != nil {
		close(release)
		t.Fatal("carrier canceled the live parent before stream drain")
	}
	close(release)
	select {
	case <-streamDone:
	case <-time.After(time.Second):
		t.Fatal("stream close did not finish after release")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("carrier did not finish after stream drain")
	}
	if parent.Err() == nil {
		t.Fatal("carrier did not cancel parent after stream retirement")
	}
	if final := carrier.Stats(); final.ActiveStreams != 0 || final.OpenedStreams-final.ClosedStreams != 0 {
		t.Fatalf("finished stream retained close ownership: %+v", final)
	}
}

type interruptibleCloseConn struct {
	net.Conn
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
	aborted     atomic.Bool
}

func (c *interruptibleCloseConn) Close() error {
	c.startOnce.Do(func() { close(c.started) })
	<-c.release
	_ = c.Conn.Close()
	if c.aborted.Load() {
		return context.Canceled
	}
	return nil
}
func (c *interruptibleCloseConn) InterruptClose() { _ = c.Abort() }

func (c *interruptibleCloseConn) Abort() error {
	c.aborted.Store(true)
	c.releaseOnce.Do(func() { close(c.release) })
	return c.Conn.Close()
}
func TestCarrierCloseDeadlineAbortsAndJoinsEveryPublishedOwner(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.done = make(chan struct{})
	carrier.shutdownDrainTimeout = 25 * time.Millisecond
	local, peer := net.Pipe()
	defer peer.Close()
	transport := &interruptibleCloseConn{Conn: local, started: make(chan struct{}), release: make(chan struct{})}
	stream := &carrierConn{Conn: transport, carrier: carrier}
	if !carrier.registerStream(stream) {
		t.Fatal("stream registration failed")
	}
	done := make(chan error, 1)
	go func() { done <- carrier.Close() }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("published close never started")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, context.Canceled) {
			t.Fatalf("lost drain/abort errors: %v", err)
		}
	case <-time.After(time.Second):
		_ = transport.Abort()
		t.Fatal("shutdown did not abort lost ACK")
	}
	if !transport.aborted.Load() {
		t.Fatal("deadline did not abort transport")
	}
	select {
	case <-stream.closeDone:
	default:
		t.Fatal("carrier returned with a live close owner")
	}
}

func TestCarrierCloseUsesOneGlobalDrainDeadline(t *testing.T) {
	if carrierShutdownDrainTimeout != 12*time.Second {
		t.Fatalf("default drain budget changed: %s", carrierShutdownDrainTimeout)
	}
	carrier := newTestCarrier(2, 1, 1, time.Second, time.Second)
	carrier.done = make(chan struct{})
	carrier.shutdownDrainTimeout = 40 * time.Millisecond
	var peers []net.Conn
	var transports []*interruptibleCloseConn
	for range 2 {
		local, peer := net.Pipe()
		peers = append(peers, peer)
		transport := &interruptibleCloseConn{Conn: local, started: make(chan struct{}), release: make(chan struct{})}
		transports = append(transports, transport)
		if !carrier.registerStream(&carrierConn{Conn: transport, carrier: carrier}) {
			t.Fatal("stream registration failed")
		}
	}
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	started := time.Now()
	err := carrier.Close()
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, context.Canceled) {
		t.Fatalf("global deadline lost error identity: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("sequential stream deadlines extended shutdown: %s", elapsed)
	}
	for _, transport := range transports {
		if !transport.aborted.Load() {
			t.Fatal("global deadline left a published stream alive")
		}
	}
	carrier.streamsMu.Lock()
	remaining := len(carrier.streams)
	carrier.streamsMu.Unlock()
	if remaining != 0 {
		t.Fatalf("carrier returned with %d registered close owners", remaining)
	}
}
func TestCarrierAbortPreemptsGracefulCloseWithoutWaitingForDrainDeadline(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.done = make(chan struct{})
	carrier.shutdownDrainTimeout = time.Minute
	local, peer := net.Pipe()
	defer peer.Close()
	transport := &interruptibleCloseConn{Conn: local, started: make(chan struct{}), release: make(chan struct{})}
	stream := &carrierConn{Conn: transport, carrier: carrier}
	if !carrier.registerStream(stream) {
		t.Fatal("register")
	}
	graceful := make(chan error, 1)
	go func() { graceful <- carrier.Close() }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("graceful close never started")
	}
	abort := make(chan error, 1)
	go func() { abort <- carrier.Abort() }()
	select {
	case err := <-abort:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("abort lost interrupted drain: %v", err)
		}
	case <-time.After(time.Second):
		_ = transport.Abort()
		t.Fatal("immediate abort waited for graceful deadline")
	}
	select {
	case <-graceful:
	case <-time.After(time.Second):
		t.Fatal("original close owner did not complete")
	}
	select {
	case <-stream.closeDone:
	default:
		t.Fatal("stream owner outlived abort")
	}
}

func TestCarrierStreamCloseErrorDoesNotFabricateClosureProof(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	local, peer := net.Pipe()
	defer peer.Close()
	sentinel := errors.New("transport close sentinel")
	carrier.activeStreams.Store(1)
	carrier.openedStreams.Store(1)
	stream := &carrierConn{Conn: &erroringCloseConn{Conn: local, err: sentinel},
		carrier: carrier, releaseActive: func() { carrier.activeStreams.Add(-1) }}
	if !carrier.registerStream(stream) {
		t.Fatal("stream registration failed")
	}
	if got := stream.Close(); got != sentinel {
		t.Fatalf("close error identity changed: %v", got)
	}
	stats := carrier.Stats()
	if stats.ActiveStreams != 0 || stats.OpenedStreams != 1 || stats.ClosedStreams != 1 {
		t.Fatalf("bookkeeping after error: active=%d opened=%d closed=%d", stats.ActiveStreams, stats.OpenedStreams, stats.ClosedStreams)
	}
}
