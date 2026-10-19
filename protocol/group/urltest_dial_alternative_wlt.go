//go:build with_wlt

package group

import (
	"context"
	"errors"

	"github.com/sagernet/sing-box/adapter"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing/common"
	N "github.com/sagernet/sing/common/network"
)

// The request context can still be live when an inner carrier service stops
// during network recovery. Respect its terminal result through every enclosing
// group: trying an ordinary upstream here would bypass WLT's recovery boundary.
// A route's ordinary connect timeout can still try another configured member
// within the caller's budget; a typed recovery failure cannot.
func dialFailureAllowsAlternative(ctx context.Context, err error) bool {
	var recovery *wltpkg.CarrierRecoveryError
	return ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.As(err, &recovery)
}

// A merged group must be usable before its first HTTP health check completes.
// Prefer a health-validated alternative, then allow one configured candidate
// for this connection only. This never publishes fabricated health history or
// changes manual selector policy. The caller performs at most one retry.
func (g *URLTestGroup) dialAlternative(ctx context.Context, network string, unavailable adapter.Outbound) adapter.Outbound {
	if ctx.Err() != nil {
		return nil
	}
	if selected := g.invalidateAndSelectAlternative(network, unavailable); selected != nil {
		return selected
	}
	if g.wltBodyProbe != nil {
		return nil
	}
	if !g.preferFirstAvailable {
		return nil
	}
	failedTag := RealTag(unavailable)
	for _, candidate := range g.outbounds {
		if candidate == unavailable || RealTag(candidate) == failedTag || !common.Contains(candidate.Network(), network) {
			continue
		}
		return candidate
	}
	return nil
}

// Publish the usable fallback before registering its connection. HTTP health
// selection covers every network supported by a member, so publish the same
// pending choice for each supported network without displacing a healthy one.
// Otherwise the later health update could interrupt the fallback itself when
// it changes the other network's selection. No health result is fabricated.
func (g *URLTestGroup) fallbackConnected(_ string, fallback adapter.Outbound) {
	if !g.preferFirstAvailable {
		return
	}
	g.selectionUpdateAccess.Lock()
	tcp, tcpHealthy := g.Select(N.NetworkTCP)
	udp, udpHealthy := g.Select(N.NetworkUDP)
	networks := fallback.Network()
	publishTCP := common.Contains(networks, N.NetworkTCP) && (!tcpHealthy || tcp == fallback)
	publishUDP := common.Contains(networks, N.NetworkUDP) && (!udpHealthy || udp == fallback)
	g.selectedAccess.Lock()
	var updated bool
	if publishTCP {
		updated = g.selectedOutboundTCP != nil && g.selectedOutboundTCP != fallback
		g.selectedOutboundTCP = fallback
	}
	if publishUDP {
		updated = updated || (g.selectedOutboundUDP != nil && g.selectedOutboundUDP != fallback)
		g.selectedOutboundUDP = fallback
	}
	g.selectedAccess.Unlock()
	g.selectionUpdateAccess.Unlock()
	if updated {
		g.interruptGroup.Interrupt(g.interruptExternalConnections)
	}
}
