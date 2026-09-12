//go:build darwin && with_wlt

package libbox

import (
	"sync/atomic"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
)

// Capture at the public entry before any epoch mutex or platform work; emit
// after the callback returns so diagnostic I/O cannot delay cancellation.
func traceWLTPublicInterfaceUpdate(m *platformDefaultInterfaceMonitor, index int32) func() {
	entered := carriercommon.DarwinUptimeNanos()
	return func() {
		finished := carriercommon.DarwinUptimeNanos()
		if m.logger != nil {
			m.logger.Info("wlt interface public callback index=", index,
				" entry_uptime_ns=", entered, " return_uptime_ns=", finished)
		}
	}
}

var (
	wltInterfaceCallbackEpoch    = time.Now()
	wltInterfaceCallbackSequence atomic.Uint64
)

// prepareWLTEarlyHandover turns a changed, still-available Darwin interface
// callback into a loss edge before refreshing the platform interface table.
// The normal nil callback remains the sole owner of WLT admission closure and
// carrier cancellation.
func beginWLTEpoch(m *platformDefaultInterfaceMonitor) uint64 {
	for {
		m.defaultInterfaceAccess.Lock()
		pending := m.wltKernelLossDone
		if pending == nil {
			epoch := m.wltHandoverEpoch.Add(1)
			m.defaultInterfaceAccess.Unlock()
			return epoch
		}
		m.defaultInterfaceAccess.Unlock()
		<-pending
	}
}

func wltEpochCurrent(m *platformDefaultInterfaceMonitor, epoch uint64) bool {
	return m.wltHandoverEpoch.Load() == epoch
}

// Called with defaultInterfaceAccess held. A stale NWPath snapshot cannot
// reopen a kernel-retired IPv4 interface before its address is added again.
func wltInterfacePublicationBlocked(m *platformDefaultInterfaceMonitor, index int) bool {
	return m.wltRetiredInterfaceIndex == index
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
