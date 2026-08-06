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

func TestWLTAdmissionTargetPreservesNestedDNSDestination(t *testing.T) {
	dnsServer := M.ParseSocksaddrHostPort("10.255.255.1", 53)
	proxyServer := M.ParseSocksaddrHostPort("proxy.example.com", 443)
	ctx := adapter.WithContext(context.Background(), &adapter.InboundContext{Destination: dnsServer})

	if target := wltAdmissionTarget(ctx, proxyServer); target != dnsServer {
		t.Fatalf("admission target=%s, want original DNS destination %s", target, dnsServer)
	}
}

func TestWLTAdmissionTargetFallsBackToImmediateDestination(t *testing.T) {
	proxyServer := M.ParseSocksaddrHostPort("proxy.example.com", 443)

	if target := wltAdmissionTarget(context.Background(), proxyServer); target != proxyServer {
		t.Fatalf("admission target=%s, want immediate destination %s", target, proxyServer)
	}
}

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
