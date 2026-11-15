//go:build !with_wlt

package group

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
)

func dialFailureAllowsAlternative(_ context.Context, _ error) bool { return true }

func (g *URLTestGroup) dialAlternative(_ context.Context, network string, unavailable adapter.Outbound) adapter.Outbound {
	return g.invalidateAndSelectAlternative(network, unavailable)
}

func (g *URLTestGroup) fallbackConnected(_ string, _ adapter.Outbound) {}
