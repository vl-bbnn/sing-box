//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type lossRetirementConn struct {
	net.Conn
	parent   context.Context
	injected error
}

func (c *lossRetirementConn) Abort() error {
	if c.parent.Err() == nil {
		return errors.New("known-lost shared parent still live at DATA retirement")
	}
	_ = c.Conn.Close()
	return c.injected
}
func TestNetworkLossPresealBeforePublishedDataRetirement(t *testing.T) {
	for _, injected := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "prior_real_error"}[injected], func(t *testing.T) {
			c := newTestCarrier(2, 2, 2, time.Second, time.Second)
			c.done = make(chan struct{})
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.cancel = cancel
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			real := errors.New("prior real DATA close error")
			f := &lossRetirementConn{Conn: a, parent: parent}
			if injected {
				f.injected = real
			}
			s := &carrierConn{Conn: f, carrier: c}
			if !c.registerStream(s) {
				t.Fatal("register")
			}
			err := c.AbortAfterNetworkLoss()
			if injected && !errors.Is(err, real) {
				t.Fatalf("prior real error erased: %v", err)
			}
			if !injected && err != nil {
				t.Fatalf("notification attempted after known path loss: %v", err)
			}
			select {
			case <-s.closeDone:
			default:
				t.Fatal("owner not joined")
			}
			if c.Stats().ActiveStreams != 0 {
				t.Fatal("active stream retained")
			}
		})
	}
}
func TestNetworkLossLateOpenOwnerIsJoinedWithoutLiveParentNotification(t *testing.T) {
	c := newTestCarrier(2, 2, 2, time.Second, time.Second)
	c.done = make(chan struct{})
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.cancel = cancel
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	c.dialRoute = func(context.Context, string) (net.Conn, error) {
		close(entered)
		<-release
		return &lossRetirementConn{Conn: a, parent: parent}, nil
	}
	result := make(chan error, 1)
	go func() {
		s, e := c.DialStream(context.Background(), "eu", "example.com:443")
		if s != nil {
			_ = s.Close()
		}
		result <- e
	}()
	<-entered
	done := make(chan error, 1)
	go func() { done <- c.AbortAfterNetworkLoss() }()
	<-c.done
	deadline := time.Now().Add(time.Second)
	for parent.Err() == nil {
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("known-lost parent not presealed")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("late stream published: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late owner not joined")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("late local disposal failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("loss abort did not join")
	}
}
