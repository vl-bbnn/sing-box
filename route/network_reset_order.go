//go:build with_wlt

package route

import "github.com/sagernet/sing-box/adapter"

const notifyInterfaceListenersBeforeConnectionClose = true

type priorityInterfaceUpdateListener interface {
	adapter.InterfaceUpdateListener
	PriorityInterfaceUpdate()
}

type priorityNetworkUnavailableListener interface {
	priorityInterfaceUpdateListener
	NetworkUnavailable()
}

func notifyPriorityInterfaceUpdateListeners(outbounds []adapter.Outbound) {
	for _, outbound := range outbounds {
		listener, isPriority := outbound.(priorityInterfaceUpdateListener)
		if isPriority {
			listener.InterfaceUpdated()
		}
	}
}

func notifyPriorityNetworkUnavailableListeners(outbounds []adapter.Outbound) {
	for _, outbound := range outbounds {
		listener, isPriority := outbound.(priorityNetworkUnavailableListener)
		if isPriority {
			listener.NetworkUnavailable()
		}
	}
}

func isPriorityInterfaceUpdateListener(listener adapter.InterfaceUpdateListener) bool {
	_, isPriority := listener.(priorityInterfaceUpdateListener)
	return isPriority
}
