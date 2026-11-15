//go:build !with_wlt

package route

import (
	"reflect"
	"testing"
)

func TestResetNetworkClosesConnectionsBeforeNotifyingListeners(t *testing.T) {
	if listenerNotifiedBeforeConnectionCloseCompletes(t) {
		t.Fatal("default build notified interface listener before tracked connection close completed")
	}
}

func TestResetNetworkRetainsOutboundListenerOrder(t *testing.T) {
	if priorityListenerNotifiedBeforeEarlierListenerCompletes(t) {
		t.Fatal("default build reordered a priority-marked interface listener")
	}
}

func TestResetNetworkRetainsDefaultListenerSequence(t *testing.T) {
	want := []string{"endpoint", "inbound", "ordinary", "priority"}
	if got := interfaceUpdateSequence(); !reflect.DeepEqual(got, want) {
		t.Fatalf("default listener sequence=%v, want %v", got, want)
	}
}
