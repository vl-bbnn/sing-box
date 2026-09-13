package route

import (
	"context"
	"reflect"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/control"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

type lifecycleOutbound struct {
	adapter.Outbound
	events []string
}

func (*lifecycleOutbound) PriorityInterfaceUpdate() {}
func (o *lifecycleOutbound) InterfaceUpdated()      { o.events = append(o.events, "return") }
func (o *lifecycleOutbound) NetworkUnavailable()    { o.events = append(o.events, "missing") }

func TestNotifyInterfaceUpdateForwardsStartupLossAndReturnOnlyWithWLT(t *testing.T) {
	ctx := pause.WithDefaultManager(context.Background())
	listener := &lifecycleOutbound{}
	manager := &NetworkManager{
		pauseManager: service.FromContext[pause.Manager](ctx),
		logger:       log.NewNOPFactory().Logger(),
		outbound:     &staticOutboundManager{outbounds: []adapter.Outbound{listener}},
	}
	// Exercise the monitor callback itself while PostStart has not run. This
	// is the window in which a WLT service's Initialize stage may be starting.
	manager.notifyInterfaceUpdate(nil, 0)
	if !manager.pauseManager.IsNetworkPaused() {
		t.Fatal("missing default interface did not pause network")
	}
	manager.notifyInterfaceUpdate(&control.Interface{Index: 10, Name: "cellular0"}, 0)
	if manager.pauseManager.IsNetworkPaused() {
		t.Fatal("default interface return did not wake network")
	}
	var want []string
	if notifyInterfaceListenersBeforeConnectionClose {
		want = []string{"missing", "return"}
	}
	if !reflect.DeepEqual(listener.events, want) {
		t.Fatalf("startup events=%v, want %v", listener.events, want)
	}
}

type forbiddenLifecycleOutboundManager struct{ adapter.OutboundManager }

func (*forbiddenLifecycleOutboundManager) Outbounds() []adapter.Outbound {
	panic("default build touched WLT-only outbound snapshot")
}

func TestDefaultMissingInterfaceDoesNotAcquireOutboundSnapshot(t *testing.T) {
	if notifyInterfaceListenersBeforeConnectionClose {
		t.Skip("default-build path")
	}
	ctx := pause.WithDefaultManager(context.Background())
	manager := &NetworkManager{
		pauseManager: service.FromContext[pause.Manager](ctx),
		logger:       log.NewNOPFactory().Logger(),
		outbound:     &forbiddenLifecycleOutboundManager{},
	}
	manager.notifyInterfaceUpdate(nil, 0)
	manager.notifyInterfaceUpdate(&control.Interface{Index: 10, Name: "cellular0"}, 0)
	manager.started = true
	manager.notifyInterfaceUpdate(nil, 0)
	if manager.interfaceResetGeneration.Load() != 0 {
		t.Fatal("default build changed interface reset generation")
	}
}
