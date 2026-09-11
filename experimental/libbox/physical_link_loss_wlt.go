//go:build with_wlt

package libbox

import "github.com/sagernet/sing/common/control"

func (m *platformDefaultInterfaceMonitor) physicalLinkSnapshot() (*control.Interface, uint64) {
	m.defaultInterfaceAccess.Lock()
	defer m.defaultInterfaceAccess.Unlock()
	if m.defaultInterface == nil {
		return nil, m.interfaceRevision
	}
	return &control.Interface{Index: m.defaultInterface.Index, Name: m.defaultInterface.Name}, m.interfaceRevision
}

// Only a kernel event for the still-current interface generation can publish
// absence. Link-up events never select or restore a default network: Android's
// ConnectivityManager owns recovery. Delivery is synchronous before logging.
func (m *platformDefaultInterfaceMonitor) forwardPhysicalLinkLoss(event physicalLinkObservation) bool {
	if !event.CandidateLoss || !event.CurrentMatch || event.CurrentInterfaceIndex <= 0 {
		return false
	}
	// Publish loss without waiting for a potentially blocked platform refresh.
	m.defaultInterfaceAccess.Lock()
	current := m.defaultInterface
	if event.InterfaceRevision != m.interfaceRevision || current == nil ||
		current.Index != event.CurrentInterfaceIndex || current.Name != event.CurrentInterfaceName {
		m.defaultInterfaceAccess.Unlock()
		return false
	}
	for _, name := range m.myInterfaces {
		if name == current.Name {
			m.defaultInterfaceAccess.Unlock()
			return false
		}
	}
	m.defaultInterface = nil
	m.interfaceRevision++
	callbacks := m.callbacks.Array()
	m.defaultInterfaceAccess.Unlock()
	for _, callback := range callbacks {
		callback(nil, 0)
	}
	return true
}
