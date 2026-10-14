package libbox

import (
	tun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/control"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/x/list"
)

var (
	_ tun.DefaultInterfaceMonitor = (*platformDefaultInterfaceMonitor)(nil)
	_ InterfaceUpdateListener     = (*platformDefaultInterfaceMonitor)(nil)
)

type platformDefaultInterfaceMonitor struct {
	*platformInterfaceWrapper
	logger                    logger.Logger
	callbacks                 list.List[tun.DefaultInterfaceUpdateCallback]
	myInterfaces              []string
	closePhysicalLinkObserver func()
	updateGeneration          uint64 // protected by defaultInterfaceAccess
	closed                    bool   // protected by defaultInterfaceAccess
}

func (m *platformDefaultInterfaceMonitor) Start() error {
	err := m.iif.StartDefaultInterfaceMonitor(m)
	if err != nil {
		return err
	}
	closeObserver, observerErr := startPhysicalLinkObserver(m)
	if observerErr != nil {
		// This observer is diagnostic only. ConnectivityManager remains the
		// authoritative source for default-network loss and recovery.
		m.logger.Warn("android physical link observer unavailable diagnostic_only=true: ", observerErr)
	} else {
		m.closePhysicalLinkObserver = closeObserver
	}
	return nil
}

func (m *platformDefaultInterfaceMonitor) Close() error {
	m.invalidateDefaultInterfaceUpdates()
	if m.closePhysicalLinkObserver != nil {
		m.closePhysicalLinkObserver()
		m.closePhysicalLinkObserver = nil
	}
	return m.iif.CloseDefaultInterfaceMonitor(m)
}

func (m *platformDefaultInterfaceMonitor) invalidateDefaultInterfaceUpdates() {
	m.defaultInterfaceAccess.Lock()
	m.closed = true
	m.updateGeneration++
	m.defaultInterfaceAccess.Unlock()
}

func (m *platformDefaultInterfaceMonitor) DefaultInterface() *control.Interface {
	m.defaultInterfaceAccess.Lock()
	defer m.defaultInterfaceAccess.Unlock()
	return m.defaultInterface
}

func (m *platformDefaultInterfaceMonitor) OverrideAndroidVPN() bool {
	return false
}

func (m *platformDefaultInterfaceMonitor) AndroidVPNEnabled() bool {
	return false
}

func (m *platformDefaultInterfaceMonitor) RegisterCallback(callback tun.DefaultInterfaceUpdateCallback) *list.Element[tun.DefaultInterfaceUpdateCallback] {
	m.defaultInterfaceAccess.Lock()
	defer m.defaultInterfaceAccess.Unlock()
	return m.callbacks.PushBack(callback)
}

func (m *platformDefaultInterfaceMonitor) UnregisterCallback(element *list.Element[tun.DefaultInterfaceUpdateCallback]) {
	m.defaultInterfaceAccess.Lock()
	defer m.defaultInterfaceAccess.Unlock()
	m.callbacks.Remove(element)
}

func (m *platformDefaultInterfaceMonitor) UpdateDefaultInterface(interfaceName string, interfaceIndex32 int32, isExpensive bool, isConstrained bool) {
	generation, active := m.beginDefaultInterfaceUpdate(isExpensive, isConstrained)
	if !active {
		return
	}
	if sFixAndroidStack {
		done := make(chan struct{})
		go func() {
			m.publishDefaultInterface(generation, interfaceName, interfaceIndex32)
			close(done)
		}()
		<-done
	} else {
		m.publishDefaultInterface(generation, interfaceName, interfaceIndex32)
	}
}

func (m *platformDefaultInterfaceMonitor) updateDefaultInterface(interfaceName string, interfaceIndex32 int32, isExpensive bool, isConstrained bool) {
	generation, active := m.beginDefaultInterfaceUpdate(isExpensive, isConstrained)
	if active {
		m.publishDefaultInterface(generation, interfaceName, interfaceIndex32)
	}
}

func (m *platformDefaultInterfaceMonitor) beginDefaultInterfaceUpdate(isExpensive bool, isConstrained bool) (uint64, bool) {
	m.defaultInterfaceAccess.Lock()
	defer m.defaultInterfaceAccess.Unlock()
	if m.closed {
		return 0, false
	}
	m.updateGeneration++
	m.isExpensive, m.isConstrained = isExpensive, isConstrained
	return m.updateGeneration, true
}

func (m *platformDefaultInterfaceMonitor) publishDefaultInterface(generation uint64, interfaceName string, interfaceIndex32 int32) {
	// lx:begin interface-loss-priority
	// Network absence is already authoritative. Stop users of the old
	// interface before a platform interface refresh can block this callback.
	if interfaceIndex32 == -1 {
		m.defaultInterfaceAccess.Lock()
		if m.closed || generation != m.updateGeneration {
			m.defaultInterfaceAccess.Unlock()
			return
		}
		m.defaultInterface = nil
		callbacks := m.callbacks.Array()
		m.defaultInterfaceAccess.Unlock()
		for _, callback := range callbacks {
			callback(nil, 0)
		}
		err := m.networkManager.UpdateInterfaces()
		if err != nil {
			m.logger.Error(E.Cause(err, "update interfaces"))
		}
		return
	}
	// lx:end interface-loss-priority
	err := m.networkManager.UpdateInterfaces()
	if err != nil {
		m.logger.Error(E.Cause(err, "update interfaces"))
	}
	// Resolve outside the monitor lock: a platform loss must never wait for
	// this lookup. The epoch check below rejects a stale completion.
	newInterface, err := m.networkManager.InterfaceFinder().ByIndex(int(interfaceIndex32))
	if err != nil {
		m.logger.Error(E.Cause(err, "find updated interface: ", interfaceName))
		return
	}
	if newInterface == nil || newInterface.Index != int(interfaceIndex32) || newInterface.Name != interfaceName {
		return
	}
	m.defaultInterfaceAccess.Lock()
	if m.closed || generation != m.updateGeneration {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	oldInterface := m.defaultInterface
	m.defaultInterface = newInterface
	if oldInterface != nil && oldInterface.Name == newInterface.Name && oldInterface.Index == newInterface.Index {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	callbacks := m.callbacks.Array()
	m.defaultInterfaceAccess.Unlock()
	for _, callback := range callbacks {
		callback(newInterface, 0)
	}
}

func (m *platformDefaultInterfaceMonitor) RegisterMyInterface(interfaceName string) {
	m.defaultInterfaceAccess.Lock()
	defer m.defaultInterfaceAccess.Unlock()
	m.myInterfaces = append(m.myInterfaces, interfaceName)
}

func (m *platformDefaultInterfaceMonitor) MyInterfaces() []string {
	m.defaultInterfaceAccess.Lock()
	defer m.defaultInterfaceAccess.Unlock()
	return m.myInterfaces
}
