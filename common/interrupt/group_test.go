package interrupt

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type slowCloseConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (c *slowCloseConn) Close() error {
	if c.calls.Add(1) == 1 {
		close(c.entered)
	}
	<-c.release
	return nil
}
func waitCloseTest(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(what)
	}
}
func TestSlowConnectionCloseDoesNotBlockAdmission(t *testing.T) {
	g := NewGroup()
	c := &slowCloseConn{entered: make(chan struct{}), release: make(chan struct{})}
	old := g.NewConn(c, true)
	finished := make(chan struct{})
	go func() { old.Close(); close(finished) }()
	defer func() { close(c.release); <-finished }()
	waitCloseTest(t, c.entered, "close never entered")
	admitted := make(chan struct{})
	go func() { g.NewConn(&slowCloseConn{}, true); g.NewPacketConn(nil, true); close(admitted) }()
	waitCloseTest(t, admitted, "network Close holds group admission lock")
}

func TestInterruptDetachesSnapshotBeforeClosing(t *testing.T) {
	g := NewGroup()
	c := &slowCloseConn{entered: make(chan struct{}), release: make(chan struct{})}
	g.NewConn(c, false)
	finished := make(chan struct{})
	go func() { g.Interrupt(true); close(finished) }()
	defer func() { close(c.release); <-finished }()
	waitCloseTest(t, c.entered, "interrupt did not start close")
	admitted := make(chan struct{})
	go func() { g.NewConn(&slowCloseConn{}, true); close(admitted) }()
	waitCloseTest(t, admitted, "interrupt close holds group admission lock")
	g.access.Lock()
	count := g.connections.Len()
	g.access.Unlock()
	if count != 1 {
		t.Fatalf("interrupted entries retained or new connection removed: %d", count)
	}
}

func TestConcurrentInterruptAndCallerCloseUnderlyingOnce(t *testing.T) {
	g := NewGroup()
	c := &slowCloseConn{entered: make(chan struct{}), release: make(chan struct{})}
	conn := g.NewConn(c, true)
	interrupted := make(chan struct{})
	go func() { g.Interrupt(true); close(interrupted) }()
	waitCloseTest(t, c.entered, "interrupt did not start close")
	caller := make(chan struct{})
	go func() { conn.Close(); close(caller) }()
	close(c.release)
	waitCloseTest(t, caller, "caller stuck")
	waitCloseTest(t, interrupted, "interrupt stuck")
	if c.calls.Load() != 1 {
		t.Fatalf("underlying Close called %d times", c.calls.Load())
	}
}

func TestInterruptRetainsExternalUntilRequested(t *testing.T) {
	g := NewGroup()
	release := make(chan struct{})
	close(release)
	internal := &slowCloseConn{entered: make(chan struct{}), release: release}
	external := &slowCloseConn{entered: make(chan struct{}), release: release}
	g.NewConn(internal, false)
	g.NewConn(external, true)
	g.Interrupt(false)
	if internal.calls.Load() != 1 || external.calls.Load() != 0 {
		t.Fatal("external policy violated")
	}
	g.Interrupt(true)
	if internal.calls.Load() != 1 || external.calls.Load() != 1 {
		t.Fatal("detached connection closed twice or external missed")
	}
}

type slowClosePacketConn struct {
	net.PacketConn
	closer *slowCloseConn
}

func (c *slowClosePacketConn) Close() error { return c.closer.Close() }
func TestSlowPacketCloseDoesNotBlockAdmission(t *testing.T) {
	g := NewGroup()
	c := &slowCloseConn{entered: make(chan struct{}), release: make(chan struct{})}
	conn := g.NewPacketConn(&slowClosePacketConn{closer: c}, true)
	finished := make(chan struct{})
	go func() { conn.Close(); close(finished) }()
	defer func() { close(c.release); <-finished }()
	waitCloseTest(t, c.entered, "packet close never entered")
	admitted := make(chan struct{})
	go func() { g.NewConn(nil, true); close(admitted) }()
	waitCloseTest(t, admitted, "packet close holds admission lock")
}

func TestNestedGroupConnectionsCloseWithoutReentrantDeadlock(t *testing.T) {
	g := NewGroup()
	release := make(chan struct{})
	close(release)
	c := &slowCloseConn{entered: make(chan struct{}), release: release}
	g.NewConn(g.NewConn(c, false), false)
	done := make(chan struct{})
	go func() { g.Interrupt(true); close(done) }()
	waitCloseTest(t, done, "nested grouped close deadlocked")
	if c.calls.Load() != 1 {
		t.Fatal("nested connection closed more than once")
	}
}
