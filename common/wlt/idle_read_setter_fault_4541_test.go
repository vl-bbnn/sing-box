//go:build with_wlt

package wlt

import (
	"errors"
	"net"
	"testing"
	"time"
)

type idleRebalanceFault4541 struct {
	net.Conn
	read, write time.Time
	failWrite   error
	writeCalls  int
}

func (c *idleRebalanceFault4541) SetDeadline(d time.Time) error     { c.read = d; c.write = d; return nil }
func (c *idleRebalanceFault4541) SetReadDeadline(d time.Time) error { c.read = d; return nil }
func (c *idleRebalanceFault4541) SetWriteDeadline(d time.Time) error {
	c.writeCalls++
	if c.failWrite != nil {
		return c.failWrite
	}
	c.write = d
	return nil
}
func TestIdleReadCallerAuthorityAfterWriteRebalanceFault4541(t *testing.T) {
	recorder := &idleRebalanceFault4541{}
	c := &carrierConn{Conn: recorder, scopedApplicationClose: true, idleTimeout: time.Second}
	c.refreshDeadline()
	original := errors.New("original write-only idle rebalance failure")
	recorder.failWrite = original
	caller := time.Now().Add(time.Hour)
	if e := c.SetDeadline(caller); e != original {
		t.Fatal("raw write-only setter error changed", e)
	}
	if !recorder.read.Equal(caller) || !c.applicationDeadline.read.Equal(caller) || !c.applicationDeadline.write.Equal(caller) {
		t.Fatal("successful original caller SetDeadline authority discarded after later failure")
	}
	c.refreshDeadline()
	if !recorder.read.Equal(caller) {
		t.Fatal("idle activity changed read cap after original write setter fault")
	}
}
