//go:build !with_wlt

package route

import "github.com/sagernet/sing-box/adapter"

const notifyInterfaceListenersBeforeConnectionClose = false

func notifyPriorityInterfaceUpdateListeners([]adapter.Outbound) {
}

func notifyPriorityNetworkUnavailableListeners([]adapter.Outbound) {
}

func isPriorityInterfaceUpdateListener(adapter.InterfaceUpdateListener) bool {
	return false
}
