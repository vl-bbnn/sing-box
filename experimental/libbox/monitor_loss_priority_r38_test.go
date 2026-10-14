package libbox

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/control"
)

type lossPriorityManager struct {
	adapter.NetworkManager
	refresh func() error
	finder  control.InterfaceFinder
}

func (m *lossPriorityManager) UpdateInterfaces() error                  { return m.refresh() }
func (m *lossPriorityManager) InterfaceFinder() control.InterfaceFinder { return m.finder }

type lossPriorityFinder struct {
	control.InterfaceFinder
	next   *control.Interface
	events *[]string
}

func (f *lossPriorityFinder) ByIndex(index int) (*control.Interface, error) {
	*f.events = append(*f.events, "find")
	return f.next, nil
}

func TestPlatformLossCancelsBeforeBlockedInterfaceRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	manager := &lossPriorityManager{refresh: func() error { close(entered); <-release; return nil }}
	monitor := &platformDefaultInterfaceMonitor{platformInterfaceWrapper: &platformInterfaceWrapper{networkManager: manager, defaultInterface: &control.Interface{Index: 7, Name: "cellular-test"}}, logger: log.NewNOPFactory().Logger()}
	monitor.RegisterCallback(func(iface *control.Interface, flags int) {
		if iface != nil {
			t.Error("loss callback returned an interface")
		}
		if monitor.DefaultInterface() != nil {
			t.Error("obsolete default interface visible during loss callback")
		}
		cancel()
	})
	go func() { monitor.updateDefaultInterface("", -1, false, false); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("refresh never entered")
	}
	canceledAtRefresh := ctx.Err() != nil
	// A packet writer admitted while the platform refresh is blocked must
	// already observe cancellation, without changing write-error accounting.
	admittedPacketWrites := 0
	if ctx.Err() == nil {
		admittedPacketWrites++
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not return")
	}
	if !canceledAtRefresh || admittedPacketWrites != 0 {
		t.Fatalf("loss delayed by interface refresh: canceled=%v admitted packet writes=%d", canceledAtRefresh, admittedPacketWrites)
	}
}

func TestPlatformLossRefreshFailureStillPublishesAbsenceFirst(t *testing.T) {
	var events []string
	manager := &lossPriorityManager{refresh: func() error {
		events = append(events, "refresh")
		return errors.New("injected platform refresh failure")
	}}
	monitor := &platformDefaultInterfaceMonitor{platformInterfaceWrapper: &platformInterfaceWrapper{networkManager: manager}, logger: log.NewNOPFactory().Logger()}
	monitor.RegisterCallback(func(iface *control.Interface, flags int) { events = append(events, "loss") })
	monitor.updateDefaultInterface("", -1, false, false)
	if !reflect.DeepEqual(events, []string{"loss", "refresh"}) {
		t.Fatalf("events=%v", events)
	}
}

func TestPlatformAvailableInterfaceStillRefreshesBeforeLookupAndNotify(t *testing.T) {
	var events []string
	next := &control.Interface{Index: 8, Name: "wifi-test"}
	manager := &lossPriorityManager{refresh: func() error { events = append(events, "refresh"); return nil }, finder: &lossPriorityFinder{next: next, events: &events}}
	monitor := &platformDefaultInterfaceMonitor{platformInterfaceWrapper: &platformInterfaceWrapper{networkManager: manager}, logger: log.NewNOPFactory().Logger()}
	monitor.RegisterCallback(func(iface *control.Interface, flags int) {
		if iface != next {
			t.Error("wrong new interface")
		}
		events = append(events, "available")
	})
	monitor.updateDefaultInterface("wifi-test", 8, false, false)
	if !reflect.DeepEqual(events, []string{"refresh", "find", "available"}) {
		t.Fatalf("events=%v", events)
	}
	events = nil
	monitor.updateDefaultInterface("wifi-test", 8, false, false)
	if !reflect.DeepEqual(events, []string{"refresh", "find"}) {
		t.Fatalf("duplicate interface notification: %v", events)
	}
}
