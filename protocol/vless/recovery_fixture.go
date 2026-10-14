//go:build with_wlt && wlt_recovery_fixture

package vless

import (
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/mux"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	vmessvless "github.com/sagernet/sing-vmess/vless"
	M "github.com/sagernet/sing/common/metadata"
)

type recoveryFixtureManager struct {
	adapter.OutboundManager
	upstream adapter.Outbound
}

func (m *recoveryFixtureManager) Outbound(string) (adapter.Outbound, bool) { return m.upstream, true }

// Constructor only: real VLESS DialContext and InterfaceUpdated, real sing-mux.
func AdoptNestedRecoveryFixture(upstream adapter.Outbound) (*Outbound, error) {
	logger := log.NewNOPFactory().Logger()
	h := &Outbound{Adapter: outbound.NewAdapter("vless", "fixture-vless", []string{"tcp"}, []string{"fixture-wlt"}),
		dialer: dialer.NewDetour(&recoveryFixtureManager{upstream: upstream}, "fixture-wlt", false),
		logger: logger, serverAddr: M.ParseSocksaddr("192.0.2.1:443")}
	var err error
	h.client, err = vmessvless.NewClient("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "", logger)
	if err != nil {
		return nil, err
	}
	h.multiplexDialer, err = mux.NewClientWithOptions((*vlessDialer)(h), logger,
		option.OutboundMultiplexOptions{Enabled: true, Protocol: "h2mux", MaxConnections: 1})
	return h, err
}
func (h *Outbound) RecoveryFixtureSessions() []any {
	return h.multiplexDialer.RecoveryFixtureSessions()
}
