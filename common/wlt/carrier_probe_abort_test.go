//go:build with_wlt

package wlt

import (
	"net"
	"sync/atomic"
	"testing"
)

type carrierProbeAbortConn struct {
	net.Conn
	aborted atomic.Bool
}

func (c *carrierProbeAbortConn) Abort() error {
	c.aborted.Store(true)
	return c.Conn.Close()
}

func TestCarrierConnAbortUsesPerStreamAbort(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	underlying := &carrierProbeAbortConn{Conn: local}
	carrier := &Carrier{streams: make(map[*carrierConn]struct{}), activeSlots: make(chan struct{}, 1)}
	carrier.activeSlots <- struct{}{}
	var released atomic.Int32
	stream := &carrierConn{
		Conn: underlying, carrier: carrier, idleStop: make(chan struct{}),
		releaseActive: func() { released.Add(1) },
	}
	carrier.streams[stream] = struct{}{}

	if err := stream.Abort(); err != nil {
		t.Fatal(err)
	}
	if !underlying.aborted.Load() || released.Load() != 1 || carrier.closedStreams.Load() != 1 {
		t.Fatalf("abort did not release one carrier stream: aborted=%v released=%d closed=%d", underlying.aborted.Load(), released.Load(), carrier.closedStreams.Load())
	}
	if _, exists := carrier.streams[stream]; exists {
		t.Fatal("aborted stream remained registered")
	}
	if err := stream.Abort(); err != nil || released.Load() != 1 || carrier.closedStreams.Load() != 1 {
		t.Fatalf("abort was not idempotent: err=%v released=%d closed=%d", err, released.Load(), carrier.closedStreams.Load())
	}
}
