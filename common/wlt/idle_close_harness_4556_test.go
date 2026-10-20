//go:build with_wlt

package wlt

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The transport can wake its reader before returning to finishClose. The
// existing owner must retain admission until Close itself has completed.
type heldIdleClose4556 struct {
	net.Conn
	entered, release chan struct{}
	calls            atomic.Int64
}

func (c *heldIdleClose4556) Close() error {
	c.calls.Add(1)
	e := c.Conn.Close()
	close(c.entered)
	<-c.release
	return e
}
func TestIdleCloseReadWakePrecedesOwnerCompletion4556(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	wrapped := &heldIdleClose4556{Conn: &deadlineUnsupportedConn{Conn: left}, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(wrapped.release) }) }
	defer release()
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.done = make(chan struct{})
	defer close(carrier.done)
	conn := &carrierConn{Conn: wrapped, carrier: carrier, releaseActive: func() { carrier.activeStreams.Add(-1) }, idleTimeout: 40 * time.Millisecond, routeClass: "eu"}
	carrier.activeStreams.Store(1)
	if !carrier.registerStream(conn) {
		t.Fatal("original admission failed")
	}
	readDone := make(chan error, 1)
	go func() { _, e := conn.Read(make([]byte, 1)); readDone <- e }()
	deadline := time.Now().Add(time.Second)
	select {
	case <-wrapped.entered:
	case <-time.After(time.Until(deadline)):
		t.Fatal("original idle Close did not start")
	}
	select {
	case e := <-readDone:
		if e == nil {
			t.Fatal("closed reader returned nil")
		}
	case <-time.After(time.Until(deadline)):
		t.Fatal("original reader not awakened")
	}
	conn.closeMu.Lock()
	done := conn.closeDone
	conn.closeMu.Unlock()
	if done == nil {
		t.Fatal("original Close owner absent")
	}
	select {
	case <-done:
		t.Fatal("Close completed before original transport returned")
	default:
	}
	if carrier.activeStreams.Load() != 1 {
		t.Fatal("slot released before original Close finished")
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
		t.Fatal("original Close owner did not finish within same budget")
	}
	if carrier.activeStreams.Load() != 0 || wrapped.calls.Load() != 1 {
		t.Fatal("original slot release or Close count changed", carrier.activeStreams.Load(), wrapped.calls.Load())
	}
}
