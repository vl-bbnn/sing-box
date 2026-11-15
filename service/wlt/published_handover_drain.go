//go:build with_wlt

package wlt

import (
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"strings"
	"time"
)

// This experiment never treats the passive underlay observer as authority.
// Only a positive trusted LTE->WiFi callback can retain an existing read. It
// cannot issue new OPENs or replay any application bytes. Loss/stop/stale
// generation invalidates the wait immediately through the existing service gate.
func allowPublishedHandoverDrain(goos, previous, current string, unavailable bool, grace time.Duration) bool {
	if unavailable || grace < time.Second || grace > 5*time.Second || (goos != "ios" && goos != "darwin") || !physicalInterfaceIdentityChanged(previous, current) {
		return false
	}
	old, next := strings.SplitN(previous, "|", 4), strings.SplitN(current, "|", 4)
	return old[2] == "cellular" && next[2] == "wifi"
}

func (s *Service) waitPublishedHandoverDrain(expected *wltpkg.Carrier, generation uint64, deadline time.Time) bool {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.access.RLock()
		current := s.carrier == expected && s.interfaceGeneration == generation && !s.stopped && !s.interfaceUnavailable && s.ctx.Err() == nil
		changed := s.dialStateChanged
		s.access.RUnlock()
		if !current {
			return false
		}
		stats := s.closeSnapshotStats(expected)
		// Draining owners still need the live mux for ACK/FIN; grace is nevertheless
		// bounded by one absolute deadline starting at the physical callback.
		if (stats.ActiveStreams == 0 && stats.DrainingStreams == 0) || !time.Now().Before(deadline) {
			return true
		}
		select {
		case <-s.ctx.Done():
			return false
		case <-changed:
		case <-ticker.C:
		}
	}
}

type publishedHandoverDrainLease struct {
	carrier      *wltpkg.Carrier
	interfaceKey string
	deadline     time.Time
}

// A same-WiFi address callback inherits one carrier's absolute cap. Any
// incompatible callback or actual loss invalidates the lease. New physical
// carriers never inherit a deadline or its old supervisor.
func (s *Service) publishedHandoverDrainDeadline(expected *wltpkg.Carrier, generation uint64, goos, previous, current string, unavailable bool, grace time.Duration) (time.Time, bool, bool) {
	s.access.Lock()
	defer s.access.Unlock()
	if expected == nil || s.carrier != expected || s.interfaceGeneration != generation || s.stopped || s.interfaceUnavailable || s.ctx.Err() != nil {
		return time.Time{}, false, false
	}
	lease := s.publishedHandoverDrain
	if lease != nil {
		if lease.carrier == expected && samePublishedHandoverPhysicalInterface(lease.interfaceKey, current) && !unavailable {
			return lease.deadline, true, false
		}
		s.publishedHandoverDrain = nil
		return time.Time{}, false, lease.carrier == expected
	}
	if !allowPublishedHandoverDrain(goos, previous, current, unavailable, grace) {
		return time.Time{}, false, false
	}
	lease = &publishedHandoverDrainLease{carrier: expected, interfaceKey: current, deadline: time.Now().Add(grace)}
	s.publishedHandoverDrain = lease
	go s.supervisePublishedHandoverCap(lease)
	return lease.deadline, true, false
}

func samePublishedHandoverPhysicalInterface(previous, current string) bool {
	old, next := strings.SplitN(previous, "|", 4), strings.SplitN(current, "|", 4)
	return len(old) == 4 && len(next) == 4 && old[0] == next[0] && old[1] == next[1] && old[2] == "wifi" && next[2] == "wifi"
}

// Independently close the retained old carrier at the cap, even if an address
// callback superseded its first worker. The ordinary serial restart still owns
// replacement allocation. A loss, stop or different carrier makes this a no-op.
func (s *Service) supervisePublishedHandoverCap(lease *publishedHandoverDrainLease) {
	timer := time.NewTimer(time.Until(lease.deadline))
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return
	case <-timer.C:
	}
	s.access.RLock()
	current := s.publishedHandoverDrain == lease && s.carrier == lease.carrier && !s.stopped && !s.interfaceUnavailable
	s.access.RUnlock()
	if current {
		s.logger.Info("wlt service published handover drain absolute deadline reached")
		s.abortRetiredPhysicalCarrier(lease.carrier)
	}
}
