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

// NotifyPhysicalNetworkUnavailable is the Android WLT priority cancellation
// lane. Ordinary monitor callbacks still perform pause and logging in order.
func (r *NetworkManager) NotifyPhysicalNetworkUnavailable() {
 r.interfaceResetGeneration.Add(1)
 if r.outbound!=nil { notifyPriorityNetworkUnavailableListeners(r.outbound.Outbounds()) }
}
