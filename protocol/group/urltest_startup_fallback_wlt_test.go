//go:build with_wlt

package group

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// A saved merged profile can receive traffic before its first HTTP health
// probe finishes. Failure of its preferred route must not prohibit trying the
// explicitly configured fallback just because its history is still empty.
func TestURLTestMergedStartupWithoutHealthHistory(t *testing.T) {
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP, "packet"} {
		t.Run(network, func(t *testing.T) {
			primary := &startupFallbackOutbound{tag: "ordinary", err: errors.New("gateway unreachable")}
			fallback := &startupFallbackOutbound{tag: "saved-carrier"}
			g := &URLTestGroup{outbounds: []adapter.Outbound{primary, fallback}, history: urltest.NewHistoryStorage(), preferFirstAvailable: true, interruptGroup: interrupt.NewGroup()}
			// The previous full health batch selected the first member with no history.
			g.performUpdateCheck()
			o := &URLTest{group: g, logger: log.NewNOPFactory().Logger()}
			var err error
			if network == "packet" {
				var c net.PacketConn
				c, err = o.ListenPacket(context.Background(), M.ParseSocksaddr("example.com:443"))
				if c != nil {
					c.Close()
				}
			} else {
				var c net.Conn
				c, err = o.DialContext(context.Background(), network, M.ParseSocksaddr("example.com:443"))
				if c != nil {
					c.Close()
				}
			}
			if err != nil {
				t.Fatalf("saved fallback was not attempted without a completed health probe: %v", err)
			}
			if primary.calls != 1 || fallback.calls != 1 {
				t.Fatalf("attempts primary=%d fallback=%d", primary.calls, fallback.calls)
			}
			if g.history.LoadURLTestHistory(fallback.Tag()) != nil {
				t.Fatal("a connection attempt must not fabricate HTTP health history")
			}
		})
	}
}

type startupFallbackOutbound struct {
	tag     string
	err     error
	calls   int
	cancel  context.CancelFunc
	onClose func()
}

func (o *startupFallbackOutbound) Type() string           { return "test" }
func (o *startupFallbackOutbound) Tag() string            { return o.tag }
func (o *startupFallbackOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o *startupFallbackOutbound) Dependencies() []string { return nil }
func (o *startupFallbackOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.calls++
	if o.cancel != nil {
		o.cancel()
	}
	if o.err != nil {
		return nil, o.err
	}
	return &r35Conn{onClose: o.onClose}, nil
}
func (o *startupFallbackOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	o.calls++
	if o.cancel != nil {
		o.cancel()
	}
	if o.err != nil {
		return nil, o.err
	}
	return &r35PacketConn{}, nil
}

func TestURLTestMergedStartupFallbackBoundaries(t *testing.T) {
	for _, test := range []struct {
		name                              string
		prefer, cancel, healthy, bothFail bool
	}{
		{name: "latency-group-unchanged"},
		{name: "cancelled-request", prefer: true, cancel: true},
		{name: "known-healthy-preferred", prefer: true, healthy: true},
		{name: "bounded-both-fail", prefer: true, bothFail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := &startupFallbackOutbound{tag: "ordinary", err: errors.New("unavailable")}
			second := &startupFallbackOutbound{tag: "saved-carrier"}
			third := &startupFallbackOutbound{tag: "healthy"}
			history := urltest.NewHistoryStorage()
			if test.cancel {
				first.cancel = cancel
				history.StoreURLTestHistory(first.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
			}
			if test.healthy {
				history.StoreURLTestHistory(third.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
			}
			if test.bothFail {
				second.err = errors.New("fallback unavailable")
			}
			g := &URLTestGroup{outbounds: []adapter.Outbound{first, second, third}, history: history, preferFirstAvailable: test.prefer, selectedOutboundTCP: first, interruptGroup: interrupt.NewGroup()}
			o := &URLTest{group: g, logger: log.NewNOPFactory().Logger()}
			c, err := o.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
			if c != nil {
				c.Close()
			}
			if test.healthy {
				if err != nil || second.calls != 0 || third.calls != 1 {
					t.Fatalf("healthy preference: err=%v unknown=%d healthy=%d", err, second.calls, third.calls)
				}
			} else {
				if err == nil {
					t.Fatal("expected failure")
				}
				want := 0
				if test.bothFail {
					want = 1
				}
				if second.calls != want || third.calls != 0 {
					t.Fatalf("unexpected retries: second=%d third=%d", second.calls, third.calls)
				}
			}
			if test.cancel && history.LoadURLTestHistory(first.Tag()) == nil {
				t.Fatal("request cancellation erased healthy route history")
			}
			if first.calls != 1 {
				t.Fatalf("primary retried: %d", first.calls)
			}
		})
	}
}

type startupAliasOutbound struct {
	*startupFallbackOutbound
	selected string
}

func (o *startupAliasOutbound) Now() string   { return o.selected }
func (o *startupAliasOutbound) All() []string { return []string{o.selected} }

func TestURLTestMergedFallbackDoesNotRepeatAliasedRoute(t *testing.T) {
	first := &startupAliasOutbound{startupFallbackOutbound: &startupFallbackOutbound{tag: "first", err: errors.New("unavailable")}, selected: "same-gateway"}
	alias := &startupAliasOutbound{startupFallbackOutbound: &startupFallbackOutbound{tag: "alias"}, selected: "same-gateway"}
	fallback := &startupFallbackOutbound{tag: "saved-carrier"}
	g := &URLTestGroup{outbounds: []adapter.Outbound{first, alias, fallback}, history: urltest.NewHistoryStorage(), preferFirstAvailable: true, interruptGroup: interrupt.NewGroup()}
	g.performUpdateCheck()
	o := &URLTest{group: g, logger: log.NewNOPFactory().Logger()}
	c, err := o.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
	if c != nil {
		c.Close()
	}
	if err != nil || alias.calls != 0 || fallback.calls != 1 {
		t.Fatalf("err=%v repeated=%d fallback=%d", err, alias.calls, fallback.calls)
	}
}

func TestURLTestMergedFallbackSurvivesOwnHealthPublication(t *testing.T) {
	first := &startupFallbackOutbound{tag: "ordinary", err: errors.New("unreachable")}
	closed := false
	fallback := &startupFallbackOutbound{tag: "saved-carrier", onClose: func() { closed = true }}
	history := urltest.NewHistoryStorage()
	g := &URLTestGroup{outbounds: []adapter.Outbound{first, fallback}, history: history, preferFirstAvailable: true, interruptGroup: interrupt.NewGroup()}
	g.performUpdateCheck()
	o := &URLTest{group: g, logger: log.NewNOPFactory().Logger()}
	c, err := o.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if history.LoadURLTestHistory(fallback.Tag()) != nil {
		t.Fatal("dial must not invent HTTP health")
	}
	history.StoreURLTestHistory(fallback.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
	g.performUpdateCheck()
	if closed {
		t.Fatal("fallback was interrupted by its own first HTTP health publication")
	}
	if o.Now() != fallback.Tag() {
		t.Fatal("successful fallback not selected")
	}
}
