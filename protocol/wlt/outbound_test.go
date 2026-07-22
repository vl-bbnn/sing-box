//go:build with_wlt

package wlt

import (
	"context"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestWLTOutboundRejectsPacketMode(t *testing.T) {
	outbound := &Outbound{
		Adapter: outbound.NewAdapter(C.TypeWLT, "wlt-eu", []string{N.NetworkTCP}, nil),
		logger:  log.NewNOPFactory().Logger(),
		route:   "eu",
	}
	_, err := outbound.ListenPacket(context.Background(), M.ParseSocksaddrHostPort("example.com", 443))
	if err == nil || !strings.Contains(err.Error(), "does not support packet") {
		t.Fatalf("err=%v, want packet unsupported error", err)
	}
}

func TestWLTStreamTargetPreservesDestinationFromDetouredOutbound(t *testing.T) {
	gateway := M.ParseSocksaddrHostPort("gateway.example.com", 443)
	dns := M.ParseSocksaddrHostPort("10.255.255.1", 53)
	ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{
		Destination: dns,
	})
	if got := wltStreamTarget(ctx, gateway); got != dns.String() {
		t.Fatalf("stream target=%q, want inherited destination %q", got, dns.String())
	}
}

func TestWLTStreamTargetFallsBackWithoutInheritedDestination(t *testing.T) {
	gateway := M.ParseSocksaddrHostPort("gateway.example.com", 443)
	if got := wltStreamTarget(context.Background(), gateway); got != gateway.String() {
		t.Fatalf("stream target=%q, want fallback %q", got, gateway.String())
	}
}
