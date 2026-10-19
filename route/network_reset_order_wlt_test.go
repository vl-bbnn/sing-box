//go:build with_wlt

package route

import (
	"reflect"
	"testing"

	"github.com/sagernet/sing-box/adapter"
)

func TestResetNetworkNotifiesListenersBeforeClosingConnections(t *testing.T) {
	if !listenerNotifiedBeforeConnectionCloseCompletes(t) {
		t.Fatal("WLT build did not notify interface listener before tracked connection close completed")
	}
}

func TestNetworkUnavailableNotifiesOnlyStructurallyMarkedWLTListener(t *testing.T) {
	ordinary := &networkUnavailableOutbound{}
	priority := &priorityNetworkUnavailableOutbound{}
	notifyPriorityNetworkUnavailableListeners([]adapter.Outbound{ordinary, priority})
	if ordinary.notified != 0 {
		t.Fatalf("ordinary listener notifications=%d, want 0", ordinary.notified)
	}
	if priority.notified != 1 {
		t.Fatalf("priority WLT listener notifications=%d, want 1", priority.notified)
	}
}

func TestResetNetworkNotifiesPriorityListenerBeforeEarlierListenerCompletes(t *testing.T) {
	if !priorityListenerNotifiedBeforeEarlierListenerCompletes(t) {
		t.Fatal("WLT build did not notify priority listener before an earlier listener completed")
	}
}

func TestResetNetworkPrioritizesWLTOnceAndRetainsOtherListenerSequence(t *testing.T) {
	want := []string{"priority", "endpoint", "inbound", "ordinary"}
	if got := interfaceUpdateSequence(); !reflect.DeepEqual(got, want) {
		t.Fatalf("WLT listener sequence=%v, want %v", got, want)
	}
}
