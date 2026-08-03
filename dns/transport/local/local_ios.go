//go:build ios

package local

import (
	"context"

	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	N "github.com/sagernet/sing/common/network"
)

// iOS does not allow applications or packet-tunnel extensions to bind the
// DHCP client port. Use the native resolver fallback instead of starting a
// DHCP discovery that can only fail on cellular and contend during interface
// changes.
func newDHCPTransport(transportAdapter dns.TransportAdapter, ctx context.Context, dialer N.Dialer, logger log.ContextLogger) dhcpTransport {
	return nil
}
