//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type unavailableCarrierService struct {
	waitCalled bool
}

func (*unavailableCarrierService) Carrier() *wltpkg.Carrier { return nil }
func (s *unavailableCarrierService) WaitCarrier(context.Context) (*wltpkg.Carrier, error) {
	s.waitCalled = true
	return nil, context.DeadlineExceeded
}
func (*unavailableCarrierService) InterfaceUpdated() {}

type directFallbackDialer struct {
	destination M.Socksaddr
}

func (d *directFallbackDialer) DialContext(_ context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	d.destination = destination
	left, right := net.Pipe()
	_ = right.Close()
	return left, nil
}

func (*directFallbackDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("not implemented")
}

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

func TestWLTOutboundUsesDirectFallbackWithoutWaitingForCarrier(t *testing.T) {
	service := &unavailableCarrierService{}
	fallback := &directFallbackDialer{}
	proxyServer := M.ParseSocksaddrHostPort("proxy.example.com", 443)
	outbound := &Outbound{
		Adapter:        outbound.NewAdapter(C.TypeWLT, "wlt-eu", []string{N.NetworkTCP}, nil),
		logger:         log.NewNOPFactory().Logger(),
		carrier:        service,
		route:          "eu",
		directFallback: true,
		fallbackDialer: fallback,
	}
	connection, err := outbound.DialContext(context.Background(), N.NetworkTCP, proxyServer)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
	if service.waitCalled {
		t.Fatal("direct fallback waited for unavailable carrier")
	}
	if fallback.destination != proxyServer {
		t.Fatalf("fallback destination=%s, want %s", fallback.destination, proxyServer)
	}
}
