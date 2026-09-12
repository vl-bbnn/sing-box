//go:build (!android && !darwin) || !with_wlt

package libbox

func startPhysicalLinkObserver(*platformDefaultInterfaceMonitor) (func(), error) {
	return nil, nil
}
