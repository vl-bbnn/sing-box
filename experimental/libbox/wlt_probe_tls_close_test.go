//go:build with_wlt

package libbox

import (
	"crypto/tls"
	"errors"
	"github.com/sagernet/sing-vmess/vless"
	M "github.com/sagernet/sing/common/metadata"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func tlsVLESSProbeWrapper(t *testing.T, conn net.Conn) net.Conn {
	t.Helper()
	client, err := vless.NewClient("00000000-0000-4000-8000-000000000001", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := client.DialEarlyConn(tls.Client(conn, &tls.Config{ServerName: "example.com", MinVersion: tls.VersionTLS12}), M.ParseSocksaddr("example.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	return outer
}
func TestProbeTLSVLESSCloseReachesScopedAbort(t *testing.T) {
	target := &wltProbeAbortConn{wltProbeErrorConn: &wltProbeErrorConn{}}
	sibling := &wltProbeAbortConn{wltProbeErrorConn: &wltProbeErrorConn{}}
	if err := closeWLTProbeConn(tlsVLESSProbeWrapper(t, target)); err != nil {
		t.Fatal(err)
	}
	if !target.aborted.Load() || target.closed.Load() || sibling.aborted.Load() || sibling.closed.Load() {
		t.Fatalf("TLS/VLESS wrapper missed scoped abort: target_aborted=%t target_closed=%t sibling_aborted=%t sibling_closed=%t", target.aborted.Load(), target.closed.Load(), sibling.aborted.Load(), sibling.closed.Load())
	}
}

type probeTLSCloseOwnerGate struct {
	*wltProbeErrorConn
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
	aborted    atomic.Bool
	abortError error
}

func (c *probeTLSCloseOwnerGate) Close() error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return c.wltProbeErrorConn.Close()
}
func (c *probeTLSCloseOwnerGate) Abort() error {
	c.aborted.Store(true)
	c.once.Do(func() { close(c.release) })
	return c.abortError
}
func TestProbeTLSVLESSAbortInterruptsExistingCloseOwner(t *testing.T) {
	injected := errors.New("retained real abort failure")
	target := &probeTLSCloseOwnerGate{wltProbeErrorConn: &wltProbeErrorConn{}, entered: make(chan struct{}, 2), release: make(chan struct{}), abortError: injected}
	defer target.once.Do(func() { close(target.release) })
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- target.Close() }()
	select {
	case <-target.entered:
	case <-time.After(time.Second):
		t.Fatal("close owner not entered")
	}
	probeDone := make(chan error, 1)
	go func() { probeDone <- closeWLTProbeConn(tlsVLESSProbeWrapper(t, target)) }()
	select {
	case err := <-probeDone:
		if !errors.Is(err, injected) || !target.aborted.Load() {
			t.Fatalf("abort error/ownership lost: %v", err)
		}
	case <-time.After(time.Second):
		target.once.Do(func() { close(target.release) })
		<-probeDone
		<-ownerDone
		t.Fatal("owned TLS/VLESS close waited for graceful owner instead of interrupting")
	}
	select {
	case <-ownerDone:
	case <-time.After(time.Second):
		t.Fatal("old owner not joined")
	}
}
func TestProbeTLSWrapperWithoutAbortKeepsNormalClose(t *testing.T) {
	target := &wltProbeErrorConn{}
	if err := closeWLTProbeConn(tlsVLESSProbeWrapper(t, target)); err != nil {
		t.Fatal(err)
	}
	if !target.closed.Load() {
		t.Fatal("normal no-abort TLS close skipped")
	}
}
