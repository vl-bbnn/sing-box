//go:build with_wlt

package libbox

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/control"
)

type coordinatorTestManager struct {
	adapter.NetworkManager
	finder   control.InterfaceFinder
	priority atomic.Int32
}

func (m *coordinatorTestManager) UpdateInterfaces() error                  { return nil }
func (m *coordinatorTestManager) InterfaceFinder() control.InterfaceFinder { return m.finder }
func (m *coordinatorTestManager) NotifyPhysicalNetworkUnavailable()        { m.priority.Add(1) }

type coordinatorTestFinder struct {
	control.InterfaceFinder
	iface *control.Interface
}

func (f coordinatorTestFinder) ByIndex(int) (*control.Interface, error) {
	return copyPhysicalInterface(f.iface), nil
}

func newCoordinatorTestMonitor(m *coordinatorTestManager, iface *control.Interface) *platformDefaultInterfaceMonitor {
	return &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{networkManager: m, defaultInterface: copyPhysicalInterface(iface)},
		logger:                   log.NewNOPFactory().Logger(),
	}
}

func lossObservation(c *physicalLinkCoordinator, deleted bool) physicalLinkObservation {
	_, revision := c.snapshot()
	return physicalLinkObservation{InterfaceRevision: revision, InterfaceIndex: 9, InterfaceName: "rmnet_data0", CurrentMatch: true, CandidateLoss: true, Deleted: deleted}
}

func TestPhysicalCoordinatorRepeatedLossPreemptsBlockedCallback(t *testing.T) {
	iface := &control.Interface{Index: 9, Name: "rmnet_data0"}
	m := &coordinatorTestManager{finder: coordinatorTestFinder{iface: iface}}
	monitor := newCoordinatorTestMonitor(m, iface)
	c := newPhysicalLinkCoordinator(monitor)
	c.update(iface.Name, int32(iface.Index), false, false)
	entered, release := make(chan struct{}), make(chan struct{})
	monitor.RegisterCallback(func(*control.Interface, int) { close(entered); <-release })
	if !c.forward(lossObservation(c, false)) {
		t.Fatal("first loss was not admitted")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	// The reader must be able to admit a second loss while ordinary delivery is blocked.
	if !c.forward(lossObservation(c, false)) {
		t.Fatal("second loss was blocked by callback")
	}
	if got := m.priority.Load(); got != 2 {
		t.Fatalf("priority cancellations=%d, want 2", got)
	}
	close(release)
	c.close()
}

func TestPhysicalCoordinatorStaleRefreshCannotPublish(t *testing.T) {
	iface := &control.Interface{Index: 9, Name: "rmnet_data0"}
	m := &coordinatorTestManager{finder: coordinatorTestFinder{iface: iface}}
	monitor := newCoordinatorTestMonitor(m, iface)
	c := newPhysicalLinkCoordinator(monitor)
	c.mu.Lock()
	c.revision = 4
	c.selected = copyPhysicalInterface(iface)
	c.mu.Unlock()
	stale := copyPhysicalInterface(iface)
	c.mu.Lock()
	c.revision = 5
	c.selected = &control.Interface{Index: 10, Name: "wlan0"}
	c.mu.Unlock()
	c.refresh(4, stale)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ready != nil {
		t.Fatalf("stale refresh published: %+v", c.ready)
	}
}

func TestPhysicalCoordinatorCloseJoinsWorkersAndSuppressesCallbacks(t *testing.T) {
	iface := &control.Interface{Index: 9, Name: "rmnet_data0"}
	m := &coordinatorTestManager{finder: coordinatorTestFinder{iface: iface}}
	monitor := newCoordinatorTestMonitor(m, iface)
	c := newPhysicalLinkCoordinator(monitor)
	c.update(iface.Name, int32(iface.Index), false, false)
	var callbacks atomic.Int32
	var once sync.Once
	monitor.RegisterCallback(func(*control.Interface, int) {
		callbacks.Add(1)
		once.Do(func() { c.close() })
	})
	c.forward(lossObservation(c, false))
	// Re-entrant close returns, and no callback can be delivered after closure.
	c.close()
	before := callbacks.Load()
	c.forward(lossObservation(c, false))
	time.Sleep(20 * time.Millisecond)
	if callbacks.Load() != before {
		t.Fatalf("callback after close: before=%d after=%d", before, callbacks.Load())
	}
}

func TestPhysicalCoordinatorSameLinkRecoveryUsesRetainedIdentity(t *testing.T) {
	iface := &control.Interface{Index: 9, Name: "rmnet_data0"}
	m := &coordinatorTestManager{finder: coordinatorTestFinder{iface: iface}}
	monitor := newCoordinatorTestMonitor(m, iface)
	c := newPhysicalLinkCoordinator(monitor)
	c.update(iface.Name, int32(iface.Index), false, false)
	if !c.forward(lossObservation(c, false)) {
		t.Fatal("loss was not admitted")
	}
	select {
	case <-time.After(time.Second):
		t.Fatal("loss cancellation did not complete")
	default:
	}
	// A link-up event is admitted only for the retained identity and refreshes
	// that exact index/name; it cannot select a different default.
	if c.forward(physicalLinkObservation{InterfaceRevision: func() uint64 { _, r := c.snapshot(); return r }(), InterfaceIndex: 9, InterfaceName: "rmnet_data0", CurrentMatch: true, AdminUp: true, LowerUp: true}) {
		t.Fatal("link-up was treated as a loss")
	}
	deadline := time.After(time.Second)
	for {
		if got := monitor.DefaultInterface(); samePhysicalInterface(got, iface) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("same-link recovery did not publish retained identity: %+v", monitor.DefaultInterface())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestPhysicalCoordinatorAuthoritativeNilBlocksNativeRestore(t *testing.T) {
	iface := &control.Interface{Index: 9, Name: "rmnet_data0"}
	m := &coordinatorTestManager{finder: coordinatorTestFinder{iface: iface}}
	monitor := newCoordinatorTestMonitor(m, iface)
	c := newPhysicalLinkCoordinator(monitor)
	c.update("", -1, false, false)
	_, revision := c.snapshot()
	if c.forward(physicalLinkObservation{InterfaceRevision: revision, InterfaceIndex: 9, InterfaceName: "rmnet_data0", CurrentMatch: true, AdminUp: true, LowerUp: true}) {
		t.Fatal("authoritative nil selection was restored by netlink")
	}
	if got := monitor.DefaultInterface(); got != nil {
		t.Fatalf("authoritative nil published interface: %+v", got)
	}
}
