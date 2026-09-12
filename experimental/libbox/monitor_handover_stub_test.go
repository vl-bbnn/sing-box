//go:build !darwin || !with_wlt

package libbox

import (
	"reflect"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/control"
)

func TestEarlyHandoverDisabledWithoutDarwinWLT(t *testing.T) {
	var events []string
	next := &control.Interface{Index: 12, Name: "new"}
	manager := &lossPriorityManager{
		refresh: func() error { events = append(events, "refresh"); return nil },
		finder:  &lossPriorityFinder{next: next, events: &events},
	}
	monitor := &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{
			networkManager:   manager,
			defaultInterface: &control.Interface{Index: 9, Name: "old"},
		},
		logger: log.NewNOPFactory().Logger(),
	}
	monitor.RegisterCallback(func(iface *control.Interface, _ int) {
		if iface == nil {
			events = append(events, "unexpected-loss")
		} else {
			events = append(events, "available")
		}
	})
	monitor.updateDefaultInterface(next.Name, int32(next.Index), false, false)
	if want := []string{"refresh", "find", "available"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v, want %v", events, want)
	}
}
