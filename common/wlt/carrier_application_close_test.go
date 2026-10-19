//go:build with_wlt

package wlt

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type applicationCloseTestConn struct {
	net.Conn
	applicationCalls atomic.Int64
	closeCalls       atomic.Int64
	abortCalls       atomic.Int64
	interruptCalls   atomic.Int64
	entered          chan struct{}
	release          chan struct{}
	interruptOnce    sync.Once
	err              error
}

func (c *applicationCloseTestConn) CloseApplication() error {
	c.applicationCalls.Add(1)
	if c.entered != nil {
		close(c.entered)
		<-c.release
	}
	return c.err
}
func (c *applicationCloseTestConn) Close() error { c.closeCalls.Add(1); return c.err }
func (c *applicationCloseTestConn) Abort() error { c.abortCalls.Add(1); return c.err }
func (c *applicationCloseTestConn) InterruptClose() {
	c.interruptCalls.Add(1)
	c.interruptOnce.Do(func() { close(c.release) })
}

type applicationCloseLegacyTestConn struct {
	net.Conn
	calls atomic.Int64
	err   error
}

func (c *applicationCloseLegacyTestConn) Close() error { c.calls.Add(1); return c.err }

// This transport supports full application close but deliberately has no Abort.
type applicationCloseNoAbortTestConn struct {
	net.Conn
	applicationCalls atomic.Int64
	closeCalls       atomic.Int64
}

func (c *applicationCloseNoAbortTestConn) CloseApplication() error {
	c.applicationCalls.Add(1)
	return nil
}
func (c *applicationCloseNoAbortTestConn) Close() error { c.closeCalls.Add(1); return nil }

func applicationCloseTestResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("stream close did not join")
		return nil
	}
}

func TestCarrierApplicationCloseUsesCapabilityAndKeepsRawError(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := errors.New("original application retirement fault")
	transport := &applicationCloseTestConn{Conn: left, err: raw}
	var released atomic.Int64
	stream := &carrierConn{Conn: transport, releaseActive: func() { released.Add(1) }}
	const callers = 8
	done := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { done <- stream.Close() }()
	}
	for i := 0; i < callers; i++ {
		if err := applicationCloseTestResult(t, done); err != raw {
			t.Fatalf("raw error changed: %v", err)
		}
	}
	if transport.applicationCalls.Load() != 1 || transport.closeCalls.Load() != 0 || transport.abortCalls.Load() != 0 || released.Load() != 1 {
		t.Fatalf("application=%d close=%d abort=%d release=%d", transport.applicationCalls.Load(), transport.closeCalls.Load(), transport.abortCalls.Load(), released.Load())
	}
}

func TestCarrierApplicationCloseKeepsLegacyFallback(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := errors.New("original legacy close fault")
	transport := &applicationCloseLegacyTestConn{Conn: left, err: raw}
	stream := &carrierConn{Conn: transport}
	for i := 0; i < 2; i++ {
		if err := stream.Close(); err != raw {
			t.Fatalf("legacy error changed: %v", err)
		}
	}
	if transport.calls.Load() != 1 {
		t.Fatalf("legacy closes=%d", transport.calls.Load())
	}
}

func TestCarrierApplicationCloseAbortKeepsImmediatePath(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := errors.New("original abort fault")
	transport := &applicationCloseTestConn{Conn: left, err: raw}
	stream := &carrierConn{Conn: transport}
	if err := stream.Abort(); err != raw {
		t.Fatalf("abort error changed: %v", err)
	}
	if transport.abortCalls.Load() != 1 || transport.applicationCalls.Load() != 0 || transport.closeCalls.Load() != 0 {
		t.Fatalf("abort=%d application=%d close=%d", transport.abortCalls.Load(), transport.applicationCalls.Load(), transport.closeCalls.Load())
	}
}

func TestCarrierApplicationCloseAbortFallbackDoesNotDrain(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	transport := &applicationCloseNoAbortTestConn{Conn: left}
	stream := &carrierConn{Conn: transport}
	if err := stream.Abort(); err != nil {
		t.Fatal(err)
	}
	if transport.closeCalls.Load() != 1 || transport.applicationCalls.Load() != 0 {
		t.Fatalf("close=%d application=%d", transport.closeCalls.Load(), transport.applicationCalls.Load())
	}
}

func TestCarrierApplicationCloseConcurrentAbortInterruptsOriginalOwner(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := errors.New("original interrupted close fault")
	transport := &applicationCloseTestConn{Conn: left, err: raw, entered: make(chan struct{}), release: make(chan struct{})}
	defer transport.interruptOnce.Do(func() { close(transport.release) })
	var released atomic.Int64
	stream := &carrierConn{Conn: transport, releaseActive: func() { released.Add(1) }}
	done := make(chan error, 2)
	go func() { done <- stream.Close() }()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("application close was not selected")
	}
	go func() { done <- stream.Abort() }()
	for i := 0; i < 2; i++ {
		if err := applicationCloseTestResult(t, done); err != raw {
			t.Fatalf("owner error changed: %v", err)
		}
	}
	if transport.applicationCalls.Load() != 1 || transport.closeCalls.Load() != 0 || transport.abortCalls.Load() != 0 || transport.interruptCalls.Load() != 1 || released.Load() != 1 {
		t.Fatalf("application=%d close=%d abort=%d interrupt=%d release=%d", transport.applicationCalls.Load(), transport.closeCalls.Load(), transport.abortCalls.Load(), transport.interruptCalls.Load(), released.Load())
	}
}
