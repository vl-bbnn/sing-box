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

type failoverTestOutbound struct {
	tag       string
	dialError error
	dialCount int
	peer      net.Conn
}

func (o *failoverTestOutbound) Type() string           { return "test" }
func (o *failoverTestOutbound) Tag() string            { return o.tag }
func (o *failoverTestOutbound) Network() []string      { return []string{N.NetworkTCP} }
func (o *failoverTestOutbound) Dependencies() []string { return nil }

func (o *failoverTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.dialCount++
	if o.dialError != nil {
		return nil, o.dialError
	}
	conn, peer := net.Pipe()
	o.peer = peer
	return conn, nil
}

func (o *failoverTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("not implemented")
}

func TestURLTestDialContextRetriesHealthValidatedAlternative(t *testing.T) {
	direct := &failoverTestOutbound{tag: "direct", dialError: errors.New("network unreachable")}
	fallback := &failoverTestOutbound{tag: "fallback"}
	history := urltest.NewHistoryStorage()
	history.StoreURLTestHistory(direct.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
	history.StoreURLTestHistory(fallback.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
	group := &URLTestGroup{
		outbounds:           []adapter.Outbound{direct, fallback},
		history:             history,
		selectedOutboundTCP: direct,
		interruptGroup:      interrupt.NewGroup(),
	}
	outbound := &URLTest{
		logger: log.NewNOPFactory().Logger(),
		group:  group,
	}

	conn, err := outbound.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if fallback.peer != nil {
		fallback.peer.Close()
	}
	if direct.dialCount != 1 {
		t.Fatalf("direct dial count=%d, want 1", direct.dialCount)
	}
	if fallback.dialCount != 1 {
		t.Fatalf("fallback dial count=%d, want 1", fallback.dialCount)
	}
	if group.selectedOutboundTCP != fallback {
		t.Fatalf("selected outbound=%v, want fallback", group.selectedOutboundTCP)
	}
	if history.LoadURLTestHistory(direct.Tag()) != nil {
		t.Fatal("unavailable outbound history was not invalidated")
	}
}

func TestURLTestPreferFirstAvailableIgnoresLatency(t *testing.T) {
	direct := &failoverTestOutbound{tag: "direct"}
	fallback := &failoverTestOutbound{tag: "fallback"}
	history := urltest.NewHistoryStorage()
	history.StoreURLTestHistory(direct.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 500})
	history.StoreURLTestHistory(fallback.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 5})
	group := &URLTestGroup{
		outbounds:            []adapter.Outbound{direct, fallback},
		history:              history,
		preferFirstAvailable: true,
	}

	selected, available := group.Select(N.NetworkTCP)
	if !available || selected != direct {
		t.Fatalf("selected outbound=%v available=%v, want first healthy outbound", selected, available)
	}

	history.DeleteURLTestHistory(direct.Tag())
	selected, available = group.Select(N.NetworkTCP)
	if !available || selected != fallback {
		t.Fatalf("selected outbound=%v available=%v, want healthy fallback", selected, available)
	}
}
