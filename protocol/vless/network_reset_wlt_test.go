//go:build with_wlt

package vless

import (
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	"testing"
)

type resetHoldOutbound struct {
	adapter.Outbound
	retained bool
}

func (o *resetHoldOutbound) WLTNetworkResetRetained() bool { return o.retained }

type resetHoldManager struct {
	adapter.OutboundManager
	outbound adapter.Outbound
}

func (m *resetHoldManager) Outbound(string) (adapter.Outbound, bool) {
	return m.outbound, m.outbound != nil
}

func TestVLESSNetworkResetRetainsOnlyExactQualifiedWLTDetour(t *testing.T) {
	wlt := &resetHoldOutbound{retained: true}
	manager := &resetHoldManager{outbound: wlt}
	detour := dialer.NewDetour(manager, "wlt-eu", false)
	h := &Outbound{dialer: detour}
	if !h.preserveWLTNetworkReset() {
		t.Fatal("qualified original WLT detour reset")
	}
	wlt.retained = false
	if h.preserveWLTNetworkReset() {
		t.Fatal("expired/legacy hold preserved")
	}
	if (&Outbound{}).preserveWLTNetworkReset() {
		t.Fatal("ordinary outbound preserved")
	}
	mixed := dialer.NewDetour(&resetHoldManager{outbound: &resetHoldOutbound{}}, "mixed-selector", false)
	if (&Outbound{dialer: mixed}).preserveWLTNetworkReset() {
		t.Fatal("mixed/unqualified detour preserved")
	}
	absent := dialer.NewDetour(&resetHoldManager{}, "missing", false)
	if (&Outbound{dialer: absent}).preserveWLTNetworkReset() {
		t.Fatal("unresolved detour preserved")
	}
}
