//go:build with_wlt

package wlt

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type deadlinePolicyRecorder4479 struct {
	net.Conn
	mu          sync.Mutex
	read, write time.Time
	calls       int
	fail        error
}

func (c *deadlinePolicyRecorder4479) SetDeadline(d time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.fail != nil {
		return c.fail
	}
	c.read = d
	c.write = d
	return nil
}
func (c *deadlinePolicyRecorder4479) SetReadDeadline(d time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.fail != nil {
		return c.fail
	}
	c.read = d
	return nil
}
func (c *deadlinePolicyRecorder4479) SetWriteDeadline(d time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.fail != nil {
		return c.fail
	}
	c.write = d
	return nil
}
func (c *deadlinePolicyRecorder4479) deadlines() (time.Time, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.read, c.write
}
func TestCarrierApplicationDeadline4479RequestCaps(t *testing.T) {
	recorder := &deadlinePolicyRecorder4479{}
	c := &carrierConn{Conn: recorder, scopedApplicationClose: true, idleTimeout: 30 * time.Second}
	read, write := time.Now().Add(time.Second), time.Now().Add(2*time.Second)
	if e := c.SetReadDeadline(read); e != nil {
		t.Fatal(e)
	}
	if e := c.SetWriteDeadline(write); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5; i++ {
		c.refreshDeadline()
	}
	r, w := recorder.deadlines()
	if !r.Equal(read) || !w.Equal(write) {
		t.Fatal("idle activity replaced explicit request budgets", r, w)
	}
	if e := c.SetReadDeadline(time.Time{}); e != nil {
		t.Fatal(e)
	}
	r, w = recorder.deadlines()
	if !r.Equal(c.applicationDeadline.idle) || !w.Equal(write) {
		t.Fatal("read clear removed idle limit or changed write budget", r, w)
	}
	if e := c.SetDeadline(time.Time{}); e != nil {
		t.Fatal(e)
	}
	r, w = recorder.deadlines()
	if !r.Equal(c.applicationDeadline.idle) || !w.Equal(r) {
		t.Fatal("both clear disabled idle limit")
	}
}
func TestCarrierApplicationDeadline4479IdleAndExplicitReplacement(t *testing.T) {
	recorder := &deadlinePolicyRecorder4479{}
	c := &carrierConn{Conn: recorder, scopedApplicationClose: true, idleTimeout: time.Second}
	c.refreshDeadline()
	idle := c.applicationDeadline.idle
	if e := c.SetDeadline(time.Now().Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	r, w := recorder.deadlines()
	if !r.Equal(idle) || !w.Equal(idle) {
		t.Fatal("long caller deadline disabled idle constraint")
	}
	expired := time.Now().Add(-time.Second)
	if e := c.SetReadDeadline(expired); e != nil {
		t.Fatal(e)
	}
	c.refreshDeadline()
	r, _ = recorder.deadlines()
	if !r.Equal(expired) {
		t.Fatal("expired caller deadline revived")
	}
	fresh := time.Now().Add(100 * time.Millisecond)
	if e := c.SetReadDeadline(fresh); e != nil {
		t.Fatal(e)
	}
	r, _ = recorder.deadlines()
	if !r.Equal(fresh) {
		t.Fatal("explicit new caller deadline not respected")
	}
}
func TestCarrierApplicationDeadline4479OriginalSetterFault(t *testing.T) {
	original := errors.New("original deadline setter error")
	recorder := &deadlinePolicyRecorder4479{fail: original}
	c := &carrierConn{Conn: recorder, scopedApplicationClose: true, idleTimeout: time.Second}
	d := time.Now().Add(time.Millisecond)
	for _, set := range []func(time.Time) error{c.SetDeadline, c.SetReadDeadline, c.SetWriteDeadline} {
		if e := set(d); e != original {
			t.Fatal("original setter error changed", e)
		}
	}
	if !c.applicationDeadline.read.IsZero() || !c.applicationDeadline.write.IsZero() {
		t.Fatal("failed setter acquired authority")
	}
}
func TestCarrierApplicationDeadline4479DefaultForwarding(t *testing.T) {
	recorder := &deadlinePolicyRecorder4479{}
	c := &carrierConn{Conn: recorder, idleTimeout: time.Second}
	read := time.Now().Add(time.Hour)
	write := read.Add(time.Second)
	_ = c.SetReadDeadline(read)
	_ = c.SetWriteDeadline(write)
	r, w := recorder.deadlines()
	if !r.Equal(read) || !w.Equal(write) || recorder.calls != 2 {
		t.Fatal("unselected forwarding changed")
	}
	if c.applicationDeadline != (carrierApplicationDeadlinePolicy{}) {
		t.Fatal("unselected policy activated")
	}
}
func TestCarrierApplicationDeadline4479ConcurrentSettersAndActivity(t *testing.T) {
	recorder := &deadlinePolicyRecorder4479{}
	c := &carrierConn{Conn: recorder, scopedApplicationClose: true, idleTimeout: time.Second}
	var group sync.WaitGroup
	for _, set := range []func(time.Time) error{c.SetDeadline, c.SetReadDeadline, c.SetWriteDeadline} {
		group.Add(1)
		go func(set func(time.Time) error) {
			defer group.Done()
			for i := 0; i < 100; i++ {
				_ = set(time.Now().Add(10 * time.Millisecond))
				c.refreshDeadline()
			}
		}(set)
	}
	group.Wait()
	read := time.Now().Add(20 * time.Millisecond)
	write := read.Add(time.Millisecond)
	_ = c.SetReadDeadline(read)
	_ = c.SetWriteDeadline(write)
	c.refreshDeadline()
	r, w := recorder.deadlines()
	if !r.Equal(read) || !w.Equal(write) {
		t.Fatal("final explicit budgets lost after concurrent activity")
	}
}
