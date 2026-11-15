package group

import (
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	tun "github.com/sagernet/sing-tun"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// These doubles never open a socket. Only Close is used by the group wrapper.
type r35Conn struct {
	net.Conn
	onClose func()
}

func (c *r35Conn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

type r35PacketConn struct{ net.PacketConn }

func (*r35PacketConn) Close() error { return nil }

type r35Outbound struct {
	tag             string
	networkCallback func()
}

func (*r35Outbound) Type() string  { return "test" }
func (o *r35Outbound) Tag() string { return o.tag }
func (o *r35Outbound) Network() []string {
	if o.networkCallback != nil {
		o.networkCallback()
	}
	return []string{N.NetworkTCP, N.NetworkUDP}
}
func (*r35Outbound) Dependencies() []string { return nil }
func (*r35Outbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return &r35Conn{}, nil
}
func (*r35Outbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return &r35PacketConn{}, nil
}
func (*r35Outbound) NewDirectRouteConnection(adapter.InboundContext, tun.DirectRouteContext, time.Duration) (tun.DirectRouteDestination, error) {
	return nil, nil
}

// Concurrent publishers and every production selection read path. History
// replacement intentionally overlaps publication as it does after health probes.
func TestURLTestSelectionConcurrentPathsR35(t *testing.T) {
	for _, prefer := range []bool{false, true} {
		name := "latency"
		if prefer {
			name = "priority"
		}
		t.Run(name, func(t *testing.T) {
			a, b := &r35Outbound{tag: "primary"}, &r35Outbound{tag: "secondary"}
			history := urltest.NewHistoryStorage()
			history.StoreURLTestHistory(b.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
			g := &URLTestGroup{outbounds: []adapter.Outbound{a, b}, history: history, preferFirstAvailable: prefer, interruptGroup: interrupt.NewGroup()}
			g.performUpdateCheck()
			o := &URLTest{group: g, logger: log.NewNOPFactory().Logger()}
			ctx := context.Background()
			dest := M.ParseSocksaddr("example.com:443")
			readPaths := []func(){
				func() {
					if tag := o.Now(); tag != a.Tag() && tag != b.Tag() {
						t.Errorf("invalid status tag %q", tag)
					}
				},
				func() {
					for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
						if selected, ok := g.Select(network); !ok || (selected != a && selected != b) {
							t.Error("invalid health selection")
						}
					}
				},
				func() {
					for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
						conn, err := o.DialContext(ctx, network, dest)
						if err != nil {
							t.Error(err)
						} else {
							conn.Close()
						}
					}
				},
				func() {
					conn, err := o.ListenPacket(ctx, dest)
					if err != nil {
						t.Error(err)
					} else {
						conn.Close()
					}
				},
				func() {
					_, err := o.NewDirectRouteConnection(adapter.InboundContext{Network: N.NetworkTCP}, nil, time.Second)
					if err != nil {
						t.Error(err)
					}
				},
			}
			start := make(chan struct{})
			var workers sync.WaitGroup
			for p := 0; p < 2; p++ {
				workers.Add(1)
				go func(offset int) {
					defer workers.Done()
					<-start
					for i := 0; i < 1000; i++ {
						if (i+offset)%2 == 0 {
							history.StoreURLTestHistory(a.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
						} else {
							history.DeleteURLTestHistory(a.Tag())
						}
						g.performUpdateCheck()
						runtime.Gosched()
					}
				}(p)
			}
			for _, read := range readPaths {
				workers.Add(1)
				go func(read func()) {
					defer workers.Done()
					<-start
					for i := 0; i < 1000; i++ {
						read()
						runtime.Gosched()
					}
				}(read)
			}
			close(start)
			workers.Wait()
		})
	}
}

// Outbound callbacks and interrupted-connection Close can re-enter status reads;
// publication must not hold the selection snapshot lock across either callback.
func TestURLTestSelectionCallbacksOutsideSnapshotLockR35(t *testing.T) {
	a, b := &r35Outbound{tag: "primary"}, &r35Outbound{tag: "secondary"}
	history := urltest.NewHistoryStorage()
	history.StoreURLTestHistory(b.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
	g := &URLTestGroup{outbounds: []adapter.Outbound{a, b}, history: history, preferFirstAvailable: true, interruptGroup: interrupt.NewGroup()}
	g.performUpdateCheck()
	o := &URLTest{group: g}
	a.networkCallback = func() { _ = o.Now() }
	callbackDone := make(chan struct{}, 1)
	g.interruptGroup.NewConn(&r35Conn{onClose: func() { _ = o.Now(); callbackDone <- struct{}{} }}, false)
	history.StoreURLTestHistory(a.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
	done := make(chan struct{})
	go func() { g.performUpdateCheck(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("publication deadlocked in callback")
	}
	select {
	case <-callbackDone:
	default:
		t.Fatal("selection change did not interrupt prior connection")
	}
	if o.Now() != a.Tag() {
		t.Fatal("new selection not visible after publication")
	}
}
