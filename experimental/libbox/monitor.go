package libbox

import (
	// lx:begin interface-loss-priority
	"sync/atomic"
	// lx:end interface-loss-priority

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
	// lx:begin interface-loss-priority
	wltHandoverEpoch atomic.Uint64
	// lx:end interface-loss-priority
}

func (m *platformDefaultInterfaceMonitor) Start() error {
	err := m.iif.StartDefaultInterfaceMonitor(m)
	if err != nil {
		return err
	}
	closeObserver, observerErr := startPhysicalLinkObserver(m)
	if observerErr != nil {
		// This observer is diagnostic only. The platform default-path monitor
		// remains authoritative for default-network loss and recovery.
		m.logger.Warn("physical link observer unavailable diagnostic_only=true: ", observerErr)
	} else {
		m.closePhysicalLinkObserver = closeObserver
	}
	return nil
}

func (m *platformDefaultInterfaceMonitor) Close() error {
	if m.closePhysicalLinkObserver != nil {
		m.closePhysicalLinkObserver()
		m.closePhysicalLinkObserver = nil
	}
	return m.iif.CloseDefaultInterfaceMonitor(m)
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
	// lx:begin wlt-ingress-diagnostic
	defer traceWLTPublicInterfaceUpdate(m, interfaceIndex32)()
	// lx:end wlt-ingress-diagnostic
	if sFixAndroidStack {
		done := make(chan struct{})
		go func() {
			m.updateDefaultInterface(interfaceName, interfaceIndex32, isExpensive, isConstrained)
			close(done)
		}()
		<-done
	} else {
		m.updateDefaultInterface(interfaceName, interfaceIndex32, isExpensive, isConstrained)
	}
}

func (m *platformDefaultInterfaceMonitor) updateDefaultInterface(interfaceName string, interfaceIndex32 int32, isExpensive bool, isConstrained bool) {
	// lx:begin interface-loss-priority
	wltEpoch := beginWLTEpoch(m)
	m.isExpensive = isExpensive
	m.isConstrained = isConstrained
	// Network absence is already authoritative. Stop users of the old
	// interface before a platform interface refresh can block this callback.
	if interfaceIndex32 == -1 {
		m.defaultInterfaceAccess.Lock()
		if !wltEpochCurrent(m, wltEpoch) {
			m.defaultInterfaceAccess.Unlock()
			return
		}
		m.defaultInterface = nil
		callbacks := m.callbacks.Array()
		m.defaultInterfaceAccess.Unlock()
		for _, callback := range callbacks {
			if !wltEpochCurrent(m, wltEpoch) {
				break
			}
			callback(nil, 0)
		}
		err := m.networkManager.UpdateInterfaces()
		if err != nil {
			m.logger.Error(E.Cause(err, "update interfaces"))
		}
		return
	}
	prepareWLTEarlyHandover(m, interfaceName, interfaceIndex32, wltEpoch)
	err := m.networkManager.UpdateInterfaces()
	if err != nil {
		m.logger.Error(E.Cause(err, "update interfaces"))
	}
	m.defaultInterfaceAccess.Lock()
	if !wltEpochCurrent(m, wltEpoch) {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	oldInterface := m.defaultInterface
	m.defaultInterfaceAccess.Unlock()
	newInterface, err := m.networkManager.InterfaceFinder().ByIndex(int(interfaceIndex32))
	if err != nil {
		m.logger.Error(E.Cause(err, "find updated interface: ", interfaceName))
		return
	}
	m.defaultInterfaceAccess.Lock()
	if !wltEpochCurrent(m, wltEpoch) {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	m.defaultInterface = newInterface
	if oldInterface != nil && oldInterface.Name == m.defaultInterface.Name && oldInterface.Index == m.defaultInterface.Index {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	callbacks := m.callbacks.Array()
	m.defaultInterfaceAccess.Unlock()
	for _, callback := range callbacks {
		if !wltEpochCurrent(m, wltEpoch) {
			break
		}
		callback(newInterface, 0)
	}
	// lx:end interface-loss-priority
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
