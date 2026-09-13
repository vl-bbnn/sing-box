//go:build android && with_wlt

package libbox

func preparePhysicalLinkMonitor(m *platformDefaultInterfaceMonitor) {
	m.physicalUpdates = newPhysicalLinkCoordinator(m)
}
