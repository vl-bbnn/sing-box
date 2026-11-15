package tun

import (
	"context"
	"net/netip"
	"testing"
	"time"

	T "github.com/sagernet/sing-tun"
	M "github.com/sagernet/sing/common/metadata"
)

type tcpNATDestinationHandler struct{ T.Handler }

func (*tcpNATDestinationHandler) PrepareConnection(string, M.Socksaddr, M.Socksaddr, T.DirectRouteContext, time.Duration) (T.DirectRouteDestination, error) {
	return nil, nil
}

// Android can reuse a local TCP port for a different remote destination.
// Keep this at the core boundary so a dependency downgrade cannot silently
// restore the source-only NAT key even if the dependency's tests are skipped.
func TestTCPNATKeepsReusedSourceDestinationsSeparate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nat := T.NewNat(ctx, time.Minute)
	source := netip.MustParseAddrPort("192.0.2.2:49803")
	first := netip.MustParseAddrPort("198.51.100.10:443")
	second := netip.MustParseAddrPort("203.0.113.20:443")
	handler := new(tcpNATDestinationHandler)
	a, err := nat.Lookup(source, first, handler)
	if err != nil {
		t.Fatal(err)
	}
	b, err := nat.Lookup(source, second, handler)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("different TCP destinations share one reverse NAT entry")
	}
	if nat.LookupBack(a).Destination != first || nat.LookupBack(b).Destination != second {
		t.Fatal("reverse NAT destination differs from original TCP tuple")
	}
	again, err := nat.Lookup(source, first, handler)
	if err != nil || again != a {
		t.Fatal("same TCP tuple lost its stable NAT mapping")
	}
}
