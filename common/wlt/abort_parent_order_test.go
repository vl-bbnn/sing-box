//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type parentRetirementConn struct {
	net.Conn
	parent   context.Context
	blocked  bool
	injected error
}

func (c *parentRetirementConn) Abort() error {
	if c.blocked {
		<-c.parent.Done()
		return c.parent.Err()
	}
	if c.parent.Err() != nil {
		return c.parent.Err()
	}
	_ = c.Conn.Close()
	return c.injected
}

func TestAbortParentRemainsLiveForPublishedStreamRetirement(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.done = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.cancel = cancel
	local, peer := net.Pipe()
	defer peer.Close()
	s := &carrierConn{Conn: &parentRetirementConn{Conn: local, parent: ctx}, carrier: c}
	if !c.registerStream(s) {
		t.Fatal("register")
	}
	if err := c.Abort(); err != nil {
		t.Fatalf("retirement parent canceled before control could be submitted: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("parent not canceled after retirement")
	}
	select {
	case <-s.closeDone:
	default:
		t.Fatal("stream owner not joined")
	}
}

func TestAbortBlockedRetirementStillCancelsAndJoins(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.done = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.cancel = cancel
	local, peer := net.Pipe()
	defer peer.Close()
	defer local.Close()
	s := &carrierConn{Conn: &parentRetirementConn{Conn: local, parent: ctx, blocked: true}, carrier: c}
	if !c.registerStream(s) {
		t.Fatal("register")
	}
	done := make(chan error, 1)
	go func() { done <- c.Abort() }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost blocking retirement error: %v", err)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("blocked stream prevented transport abort")
	}
	select {
	case <-s.closeDone:
	default:
		t.Fatal("abort returned before joining stream")
	}
}

func TestAbortRetirementPreservesInjectedControlError(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.done = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.cancel = cancel
	local, peer := net.Pipe()
	defer peer.Close()
	expected := errors.New("control notification failed")
	s := &carrierConn{Conn: &parentRetirementConn{Conn: local, parent: ctx, injected: expected}, carrier: c}
	if !c.registerStream(s) {
		t.Fatal("register")
	}
	if err := c.Abort(); !errors.Is(err, expected) {
		t.Fatalf("lost actual notification error: %v", err)
	}
}
