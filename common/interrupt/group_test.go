package interrupt

import (
	"net"
	"sync"
	"testing"
	"time"
)

type blockingCloseConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
}

func (c *blockingCloseConn) Close() error {
	close(c.entered)
	<-c.release
	return c.Conn.Close()
}

func waitFor(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestConnectionCloseDoesNotHoldGroupLock(t *testing.T) {
	group := NewGroup()
	left, right := net.Pipe()
	defer right.Close()
	blocked := &blockingCloseConn{Conn: left, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
	t.Cleanup(release)
	conn := group.NewConn(blocked, false)
	closed := make(chan struct{})
	go func() {
		_ = conn.Close()
		close(closed)
	}()
	waitFor(t, blocked.entered, "underlying close")

	newLeft, newRight := net.Pipe()
	defer newRight.Close()
	registered := make(chan net.Conn, 1)
	go func() { registered <- group.NewConn(newLeft, false) }()
	select {
	case newConn := <-registered:
		_ = newConn.Close()
	case <-time.After(time.Second):
		t.Fatal("new connection blocked behind underlying close")
	}
	release()
	waitFor(t, closed, "connection close completion")
}

func TestInterruptDoesNotHoldGroupLockDuringClose(t *testing.T) {
	group := NewGroup()
	left, right := net.Pipe()
	defer right.Close()
	blocked := &blockingCloseConn{Conn: left, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
	t.Cleanup(release)
	group.NewConn(blocked, false)
	interrupted := make(chan struct{})
	go func() {
		group.Interrupt(false)
		close(interrupted)
	}()
	waitFor(t, blocked.entered, "interrupt close")

	newLeft, newRight := net.Pipe()
	defer newRight.Close()
	registered := make(chan net.Conn, 1)
	go func() { registered <- group.NewConn(newLeft, false) }()
	select {
	case newConn := <-registered:
		_ = newConn.Close()
	case <-time.After(time.Second):
		t.Fatal("new connection blocked behind interrupt close")
	}
	release()
	waitFor(t, interrupted, "interrupt completion")
}
