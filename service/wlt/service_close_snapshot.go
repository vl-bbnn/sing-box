//go:build with_wlt

package wlt

import (
	"fmt"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
)

// formatWLTServiceCloseSnapshot reports one independent read of carrier stats.
// A returned close call does not prove all transport resources are closed.
func formatWLTServiceCloseSnapshot(phase, mode, result string, generation uint64, stats wltpkg.CarrierStats) string {
	return fmt.Sprint("wlt service close_snapshot schema=2 phase=", phase,
		" close_mode=", mode, " close_result=", result,
		" closure_status=unverified snapshot_consistency=independent_reads carrier_generation=", generation,
		" active=", stats.ActiveStreams, " peak_active=", stats.PeakActiveStreams,
		" draining=", stats.DrainingStreams, " peak_draining=", stats.PeakDrainingStreams,
		" drain_capacity=", stats.DrainCapacity,
		" pending=", stats.PendingDials, " peak_pending=", stats.PeakPendingDials,
		" opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams,
		" queued=", stats.QueuedDials, " dns_open_requests=", stats.DNSOpenRequests,
		" dns_open_queued=", stats.DNSOpenQueued, " dns_open_rejected=", stats.DNSOpenRejected,
		" rejected=", stats.RejectedStreams, " rejected_queue=", stats.RejectedQueue,
		" rejected_active=", stats.RejectedActive, " rejected_open=", stats.RejectedOpen,
		" pressure_reclaims=", stats.PressureIdleReclaims, " failed=", stats.FailedStreams,
		" max_dial_ms=", stats.MaxDialMillis, " reconnect_retries=", stats.ReconnectRetries,
		" reconnect_wait_ms=", stats.ReconnectWaitMillis,
		" reconnects=", stats.Runtime.FullReconnects,
		" last_reconnect=", stats.Runtime.LastReconnectReason)
}

func (s *Service) closeSnapshotStats(carrier *wltpkg.Carrier) wltpkg.CarrierStats {
	if s.closeSnapshotStatsHook != nil {
		return s.closeSnapshotStatsHook(carrier)
	}
	return carrier.Stats()
}
