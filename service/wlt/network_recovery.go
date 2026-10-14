//go:build with_wlt

package wlt

import (
	"context"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"time"
)

// This owner exists only when the carrier grants an already negotiated hold.
// Availability callbacks confer no transport loss or authentication authority.
type serviceNetworkHold struct {
	carrier           *wltpkg.Carrier
	generation        uint64
	recovery          wltpkg.NetworkRecovery
	deadline          time.Time
	done              chan struct{}
	completed, failed bool // service.access
}

func (s *Service) suspendNetworkLocked() bool {
	if s.carrier == nil || s.stopped || s.quiescing || s.retentionRejectedGeneration == s.carrierGeneration {
		return false
	}
	var recovery wltpkg.NetworkRecovery
	if s.suspendNetworkHook != nil {
		recovery = s.suspendNetworkHook(s.carrier)
	} else {
		recovery = s.carrier.SuspendNetworkRecovery()
	}
	if recovery == nil || !time.Now().Before(recovery.Deadline()) {
		s.retainedCarrier = nil
		return false
	}
	old := s.networkHold
	if old != nil && old.carrier == s.carrier && old.generation == s.carrierGeneration && old.recovery == recovery && !old.failed && !old.completed {
		return true
	}
	deadline := recovery.Deadline()
	if old != nil && old.deadline.Before(deadline) {
		deadline = old.deadline
	}
	h := &serviceNetworkHold{carrier: s.carrier, generation: s.carrierGeneration, recovery: recovery, deadline: deadline, done: make(chan struct{})}
	s.networkHold = h
	for lease := range s.pendingDialLeases {
		if lease.carrier == h.carrier && lease.generation == h.generation && lease.ctx.Err() == nil {
			lease.retained = h
		}
	}
	if old != nil {
		old.failed = true
		closeSignal(old.done)
	}
	go s.watchNetworkHold(h)
	return true
}

func (s *Service) watchNetworkHold(h *serviceNetworkHold) {
	timer := time.NewTimer(time.Until(h.deadline))
	defer timer.Stop()
	select {
	case <-h.done:
		return
	case <-s.ctx.Done():
		return
	case <-timer.C:
	}
	s.failNetworkHold(h, 0)
}

// An older resume callback cannot fail/reopen a newer interface notification.
func (s *Service) failNetworkHold(h *serviceNetworkHold, resumeGeneration uint64) {
	s.access.Lock()
	if s.networkHold != h || s.carrier != h.carrier || s.carrierGeneration != h.generation || s.stopped || s.quiescing || h.completed || h.failed || (resumeGeneration != 0 && s.interfaceGeneration != resumeGeneration) {
		s.access.Unlock()
		return
	}
	h.failed = true
	s.networkHold = nil
	s.retainedCarrier = nil
	s.retentionRejectedGeneration = h.generation
	closeSignal(h.done)
	// Retained OPENs must fail as the original operation, never replay on B.
	for lease := range s.pendingDialLeases {
		if lease.retained == h {
			lease.cancel(errDialCarrierReplaced)
		}
	}
	s.notifyDialStateLocked()
	generation, unavailable := s.interfaceGeneration, s.interfaceUnavailable
	parentCancel := s.carrierStartCancel
	s.carrierStartCancel = nil
	s.access.Unlock()
	_ = s.abortCarrier(h.carrier)
	if parentCancel != nil {
		parentCancel()
	}
	if !unavailable {
		s.restartCarrier(h.carrier, "retained network recovery failed", generation)
		s.finishInterfaceRecovery(generation)
	}
}

func (s *Service) resumeNetworkHold(carrier *wltpkg.Carrier, generation uint64) bool {
	s.access.Lock()
	h := s.networkHold
	if h == nil || h.carrier != carrier || h.generation != s.carrierGeneration || s.interfaceGeneration != generation || s.interfaceUnavailable || s.stopped || s.quiescing {
		s.access.Unlock()
		return false
	}
	s.access.Unlock()
	go func() {
		ctx, cancel := context.WithDeadline(s.ctx, h.deadline)
		defer cancel()
		err := h.recovery.ResumeNetwork(ctx)
		s.access.Lock()
		current := s.networkHold == h && s.carrier == h.carrier && s.carrierGeneration == h.generation && s.interfaceGeneration == generation && !s.interfaceUnavailable && !s.stopped && !s.quiescing && !h.failed && !h.completed && ctx.Err() == nil && time.Now().Before(h.deadline)
		if current && err == nil {
			h.completed = true
			s.networkHold = nil
			s.retainedCarrier = h.carrier
			s.retainedCarrierGeneration = h.generation
			closeSignal(h.done)
			s.carrierInterfaceKey = s.interfaceKey
			closeSignal(s.interfaceReady)
			s.notifyDialStateLocked()
		}
		s.access.Unlock()
		if current && err != nil {
			s.failNetworkHold(h, generation)
		}
	}()
	return true
}

// Used by exact WLT detours to defer their nested mux reset while this original
// session is held. Service shutdown and hold expiry retain their terminal paths.
func (s *Service) WLTNetworkRecoveryActive() bool {
	s.access.RLock()
	defer s.access.RUnlock()
	h := s.networkHold
	return h != nil && h.carrier == s.carrier && h.generation == s.carrierGeneration && !h.failed && !h.completed && !s.stopped && !s.quiescing && time.Now().Before(h.deadline)
}

func (s *Service) WLTNetworkResetRetained() bool {
	s.access.RLock()
	defer s.access.RUnlock()
	if s.stopped || s.quiescing || s.retentionRejectedGeneration == s.carrierGeneration {
		return false
	}
	h := s.networkHold
	if h != nil {
		return h.carrier == s.carrier && h.generation == s.carrierGeneration && !h.failed && !h.completed && time.Now().Before(h.deadline)
	}
	return s.retainedCarrier != nil && s.retainedCarrier == s.carrier && s.retainedCarrierGeneration == s.carrierGeneration && !s.interfaceUnavailable
}
