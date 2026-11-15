//go:build with_wlt

package wlt

import (
	"context"
	"strings"
	"testing"

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
