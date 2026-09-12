//go:build darwin && with_wlt

package libbox

import (
	"sync/atomic"
	"time"
)

var (
	wltInterfaceCallbackEpoch    = time.Now()
	wltInterfaceCallbackSequence atomic.Uint64
)

// prepareWLTEarlyHandover turns a changed, still-available Darwin interface
// callback into a loss edge before refreshing the platform interface table.
// The normal nil callback remains the sole owner of WLT admission closure and
// carrier cancellation.
func beginWLTEpoch(m *platformDefaultInterfaceMonitor) uint64 {
	m.defaultInterfaceAccess.Lock()
	epoch := m.wltHandoverEpoch.Add(1)
	m.defaultInterfaceAccess.Unlock()
	return epoch
}

func wltEpochCurrent(m *platformDefaultInterfaceMonitor, epoch uint64) bool {
	return m.wltHandoverEpoch.Load() == epoch
}

func prepareWLTEarlyHandover(m *platformDefaultInterfaceMonitor, interfaceName string, interfaceIndex32 int32, epoch uint64) {
	sequence := wltInterfaceCallbackSequence.Add(1)
	ingressElapsed := time.Since(wltInterfaceCallbackEpoch)

	m.defaultInterfaceAccess.Lock()
	if !wltEpochCurrent(m, epoch) {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	oldInterface := m.defaultInterface
	if oldInterface == nil {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	indexChanged := oldInterface.Index != int(interfaceIndex32)
	nameChanged := oldInterface.Name != interfaceName
	if !indexChanged && !nameChanged {
		m.defaultInterfaceAccess.Unlock()
		return
	}
	m.defaultInterface = nil
	callbacks := m.callbacks.Array()
	m.defaultInterfaceAccess.Unlock()

	// Publish cancellation before calling the logger. A synchronous log sink is
	// allowed to block, but it must not keep the old WLT carrier alive.
	for _, callback := range callbacks {
		if !wltEpochCurrent(m, epoch) {
			return
		}
		callback(nil, 0)
	}
	if !wltEpochCurrent(m, epoch) {
		return
	}
	changeElapsed := time.Since(wltInterfaceCallbackEpoch)
	m.logger.Info(
		"wlt default interface handover sequence=", sequence,
		" monotonic_ingress=", ingressElapsed.String(),
		" callback_to_cancellation_published=", changeElapsed-ingressElapsed,
		" index_changed=", indexChanged,
		" name_changed=", nameChanged,
		" cancellation=before_refresh",
	)
}
