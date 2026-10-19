//go:build with_wlt

package vless

import (
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing/common"
	N "github.com/sagernet/sing/common/network"
)

// Only an exact WLT detour with a negotiated same-carrier hold qualifies.
// A mixed selector or other transport does not inherit this authority.
func retainedWLTDetour(d N.Dialer) bool {
	detour, ok := common.Cast[*dialer.DetourDialer](d)
	if !ok {
		return false
	}
	upstream, err := detour.Dialer()
	if err != nil {
		return false
	}
	owner, ok := upstream.(interface{ WLTNetworkResetRetained() bool })
	return ok && owner.WLTNetworkResetRetained()
}

func (h *Outbound) preserveWLTNetworkReset() bool {
	return h.transport == nil && retainedWLTDetour(h.dialer)
}
