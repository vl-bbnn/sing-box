//go:build with_wlt

package vless

import (
	"context"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
	"testing"
)

type traceManager struct {
	adapter.OutboundManager
	target adapter.Outbound
}

func (m *traceManager) Outbound(string) (adapter.Outbound, bool) { return m.target, m.target != nil }

type traceOutbound struct {
	adapter.Outbound
	kind string
}

func (o *traceOutbound) Type() string { return o.kind }
func TestPayloadTraceRequiresActualWLTDetour(t *testing.T) {
	ctx := context.Background()
	if wltFirstPayloadTrace(ctx, "", logger.NOP()) != nil {
		t.Fatal("empty detour traced")
	}
	opt := wltFirstPayloadTrace(ctx, "arbitrary-label", logger.NOP())
	if opt.Enabled() {
		t.Fatal("missing manager accepted")
	}
	for _, kind := range []string{C.TypeDirect, C.TypeVLESS, C.TypeWLT} {
		manager := &traceManager{target: &traceOutbound{kind: kind}}
		with := service.ContextWith[adapter.OutboundManager](ctx, manager)
		got := wltFirstPayloadTrace(with, "arbitrary-label", logger.NOP()).Enabled()
		if got != (kind == C.TypeWLT) {
			t.Fatalf("kind %s enabled %v", kind, got)
		}
	}
}
