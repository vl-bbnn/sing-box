//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
)

var errDialServiceStopped = errors.Join(carriercommon.ErrOpenServiceStopped, context.Canceled)
var errDialCarrierReplaced = errors.Join(errors.New("wlt carrier replaced without a local generation recovery"), context.Canceled)

var nextDialAttemptID atomic.Uint64

// dialLease owns only an unpublished dial. Removing it while holding access is
// the publication point; later network callbacks must never replay that stream.
type dialLease struct {
	attemptID           uint64
	carrier             *wltpkg.Carrier
	generation          uint64
	interfaceGeneration uint64
	ctx                 context.Context
	cancel              context.CancelCauseFunc
	retired             *localDialGenerationRetired
}

type localDialGenerationRetired struct {
	carrier    *wltpkg.Carrier
	generation uint64
	diagnostic *wltpkg.LocalGenerationRetirement
}

func (*localDialGenerationRetired) Error() string {
	return "wlt pending dial carrier generation retired locally"
}
func (e *localDialGenerationRetired) Unwrap() error {
	if e.diagnostic != nil {
		return e.diagnostic
	}
	return context.Canceled
}

// retireDialLeasesLocked runs before carrier Abort, so mux shutdown can preserve
// the exact pending caller's cause. Terminal replacement does not grant retry.
func (s *Service) retireDialLeasesLocked(local bool) {
	for lease := range s.pendingDialLeases {
		if lease.carrier != s.carrier || lease.generation != s.carrierGeneration || lease.ctx.Err() != nil {
			continue
		}
		if local {
			lease.retired = &localDialGenerationRetired{
				carrier: lease.carrier, generation: lease.generation,
				diagnostic: &wltpkg.LocalGenerationRetirement{
					AttemptID: lease.attemptID, CarrierGeneration: lease.generation,
					InterfaceGeneration: lease.interfaceGeneration,
				},
			}
			lease.cancel(lease.retired)
		} else {
			lease.cancel(errDialCarrierReplaced)
		}
	}
}

func (s *Service) notifyDialStateLocked() {
	closeSignal(s.dialStateChanged)
	s.dialStateChanged = make(chan struct{})
}

func (s *Service) acquireDialLease(ctx context.Context, previous *dialLease) (*dialLease, error) {
	for {
		s.access.Lock()
		if err := ctx.Err(); err != nil {
			s.access.Unlock()
			return nil, err
		}
		if s.stopped || s.quiescing || s.ctx.Err() != nil {
			s.access.Unlock()
			return nil, errDialServiceStopped
		}
		ready := s.interfaceReady == nil
		if !ready {
			select {
			case <-s.interfaceReady:
				ready = true
			default:
			}
		}
		if s.carrier != nil && !s.interfaceUnavailable && ready &&
			(previous == nil || (s.carrier != previous.carrier && s.carrierGeneration > previous.generation)) {
			attemptCtx, cancel := context.WithCancelCause(ctx)
			lease := &dialLease{attemptID: nextDialAttemptID.Add(1), carrier: s.carrier, generation: s.carrierGeneration, interfaceGeneration: s.interfaceGeneration, ctx: attemptCtx, cancel: cancel}
			if s.pendingDialLeases == nil {
				s.pendingDialLeases = make(map[*dialLease]struct{})
			}
			s.pendingDialLeases[lease] = struct{}{}
			s.access.Unlock()
			return lease, nil
		}
		if s.dialStateChanged == nil {
			s.dialStateChanged = make(chan struct{})
		}
		changed := s.dialStateChanged
		s.access.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ctx.Done():
			return nil, errDialServiceStopped
		case <-changed:
		}
	}
}

// waitForEarlyLocalRetirement bridges only a failed, unpublished transport
// OPEN with an exact typed local peer-loss witness and a subsequent real
// local generation notification. An unrelated or mixed EOF cannot acquire
// retry authority merely because a later network callback happens. The original
// transport error remains an error observation; it is never called a local
// cancellation. No returned stream or application payload is retried.
func (s *Service) waitForEarlyLocalRetirement(ctx context.Context, lease *dialLease, logicalDialID uint64, original error) bool {
	if !(errors.Is(original, io.EOF) || errors.Is(original, io.ErrUnexpectedEOF) ||
		errors.Is(original, io.ErrClosedPipe) || errors.Is(original, net.ErrClosed)) {
		return false
	}
	proof := carriercommon.PendingOpenPathLossFromError(original)
	if proof == nil || (carriercommon.PeerPathLossFromError(proof.Err) == nil || proof.Diagnostic.LogicalDialID != logicalDialID || proof.Diagnostic.DialAttemptID != lease.attemptID || proof.Diagnostic.CarrierGeneration != lease.generation || proof.Diagnostic.InterfaceGeneration != lease.interfaceGeneration) {
		return false
	}
	s.access.Lock()
	retired := lease.retired
	provedRetirement := proof != nil && ctx.Err() == nil && !s.stopped && !s.quiescing && s.ctx.Err() == nil && retired != nil && retired.carrier == lease.carrier && retired.generation == lease.generation && context.Cause(lease.ctx) == retired
	if provedRetirement {
		s.access.Unlock()
		return true
	}
	eligible := ctx.Err() == nil && lease.ctx.Err() == nil && !s.stopped && !s.quiescing &&
		s.ctx.Err() == nil && s.carrier == lease.carrier && s.carrierGeneration == lease.generation &&
		s.interfaceGeneration == lease.interfaceGeneration && !s.interfaceUnavailable
	s.access.Unlock()
	if !eligible {
		return false
	}
	s.logger.Info("wlt dial causal event=early_failure logical_dial_id=", logicalDialID, " dial_attempt_id=", lease.attemptID,
		" carrier_generation=", lease.generation, " interface_generation=", lease.interfaceGeneration,
		" original_transport_error=true observation_budget_ms=500")
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for {
		s.access.Lock()
		retired := lease.retired
		confirmed := ctx.Err() == nil && !s.stopped && !s.quiescing && s.ctx.Err() == nil &&
			retired != nil && retired.carrier == lease.carrier && retired.generation == lease.generation &&
			context.Cause(lease.ctx) == retired
		terminal := ctx.Err() != nil || s.stopped || s.quiescing || s.ctx.Err() != nil ||
			(lease.ctx.Err() != nil && !confirmed)
		if s.dialStateChanged == nil {
			s.dialStateChanged = make(chan struct{})
		}
		changed := s.dialStateChanged
		s.access.Unlock()
		if confirmed {
			return true
		}
		if terminal {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-s.ctx.Done():
			return false
		case <-lease.ctx.Done():
		case <-changed:
		case <-timer.C:
			return false
		}
	}
}

// DialStream uses one absolute caller/connect deadline across readiness, OPEN
// admission and trusted generation recovery. Only unpublished attempts can move
// to a new carrier; application bytes and returned streams are never replayed.
func (s *Service) DialStream(ctx context.Context, routeClass string, target string) (stream io.ReadWriteCloser, err error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wltpkg.CarrierConnectTimeout(time.Duration(s.options.ConnectTimeout)))
		defer cancel()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(errDialServiceStopped) })
	defer func() { stop(); cancel(nil) }()
	var previous *dialLease
	attempts := 0
	var finalGeneration uint64
	var logicalDialID, finalAttemptID uint64
	observedRetirement := false
	defer func() {
		if observedRetirement {
			outcome := "published"
			if err != nil {
				outcome = "failed"
			}
			s.logger.Info("wlt dial correlation event=final logical_dial_id=", logicalDialID, " final_attempt_id=", finalAttemptID, " final_generation=", finalGeneration, " attempts=", attempts, " outcome=", outcome)
		}
		if previous != nil {
			outcome := "published"
			if err != nil {
				outcome = "failed"
				err = &wltpkg.CarrierRecoveryError{Err: err}
			}
			s.logger.Info("wlt pending dial recovery attempts=", attempts, " retired_generation=", previous.generation, " final_generation=", finalGeneration, " outcome=", outcome, " retired_attempt_id=", previous.attemptID)
		}
	}()
	for {
		var lease *dialLease
		lease, err = s.acquireDialLease(ctx, previous)
		if err != nil {
			return nil, err
		}
		attempts++
		if logicalDialID == 0 {
			logicalDialID = lease.attemptID
		}
		finalAttemptID = lease.attemptID
		finalGeneration = lease.generation
		deadlineRemainingMS := int64(-1)
		if deadline, ok := lease.ctx.Deadline(); ok {
			deadlineRemainingMS = time.Until(deadline).Milliseconds()
		}
		s.logger.Info("wlt dial causal event=admitted logical_dial_id=", logicalDialID,
			" dial_attempt_id=", lease.attemptID, " carrier_generation=", lease.generation,
			" interface_generation=", lease.interfaceGeneration,
			" deadline_remaining_ms=", deadlineRemainingMS)
		diagnosticCtx := carriercommon.WithOpenDiagnostic(lease.ctx, carriercommon.OpenDiagnostic{
			LogicalDialID: logicalDialID, DialAttemptID: lease.attemptID,
			CarrierGeneration: lease.generation, InterfaceGeneration: lease.interfaceGeneration,
		})
		if s.dialStreamHook != nil {
			stream, err = s.dialStreamHook(diagnosticCtx, lease.carrier, routeClass, target)
		} else {
			stream, err = lease.carrier.DialStream(diagnosticCtx, routeClass, target)
		}
		if err != nil {
			tree, nodes, truncated := boundedDialErrorTree(err)
			s.logger.Info("wlt dial causal event=carrier_error logical_dial_id=", logicalDialID,
				" dial_attempt_id=", lease.attemptID, " carrier_generation=", lease.generation,
				" interface_generation=", lease.interfaceGeneration, " tree=", tree,
				" tree_nodes=", nodes, " tree_truncated=", truncated)
		}
		lateLocalRetirement := stream == nil && err != nil && s.waitForEarlyLocalRetirement(ctx, lease, logicalDialID, err)
		s.access.Lock()
		delete(s.pendingDialLeases, lease)
		s.notifyDialStateLocked()
		terminal := ctx.Err()
		if s.stopped || s.ctx.Err() != nil {
			terminal = errDialServiceStopped
		}
		current := s.carrier == lease.carrier && s.carrierGeneration == lease.generation && s.interfaceGeneration == lease.interfaceGeneration && !s.interfaceUnavailable
		retired := lease.retired
		retry := terminal == nil && retired != nil && retired.carrier == lease.carrier && retired.generation == lease.generation &&
			context.Cause(lease.ctx) == retired && (lateLocalRetirement || err == nil || errors.Is(err, retired) || errors.Is(err, context.Canceled))
		if retired != nil {
			observedRetirement = true
		}
		if terminal == nil && err == nil && current && retired == nil && lease.ctx.Err() == nil {
			s.access.Unlock()
			s.logger.Info("wlt dial causal event=settled logical_dial_id=", logicalDialID,
				" dial_attempt_id=", lease.attemptID, " outcome=published cause=none")
			lease.cancel(nil)
			return stream, nil
		}
		s.access.Unlock()
		cause := "unknown"
		switch {
		// Service return precedence is terminal, then trusted retry, then the
		// carrier result. A simultaneous carrier error must not relabel the
		// DeadlineExceeded or cancellation that this function returns.
		case errors.Is(terminal, errDialServiceStopped):
			cause = "service_stop"
		case errors.Is(terminal, context.DeadlineExceeded):
			cause = "deadline"
		case errors.Is(terminal, context.Canceled):
			cause = "caller_canceled"
		case retry && lateLocalRetirement:
			cause = "carrier_failure_then_local_retirement"
		case retry:
			cause = "local_retirement"
		case err == nil:
			cause = "carrier_replaced"
		case errors.Is(err, context.DeadlineExceeded):
			cause = "deadline"
		case errors.Is(err, context.Canceled) && retired != nil && context.Cause(lease.ctx) == retired:
			cause = "local_retirement"
		case errors.Is(err, context.Canceled):
			cause = "unknown"
		default:
			cause = "carrier_failure"
		}
		outcome := "failed"
		if retry {
			outcome = "retired_retry"
		}
		s.logger.Info("wlt dial causal event=settled logical_dial_id=", logicalDialID,
			" dial_attempt_id=", lease.attemptID, " outcome=", outcome, " cause=", cause,
			" observed_carrier_error=", err != nil)
		if retired != nil {
			s.logger.Info("wlt dial correlation event=retired logical_dial_id=", logicalDialID, " dial_attempt_id=", lease.attemptID, " carrier_generation=", lease.generation, " interface_generation=", lease.interfaceGeneration, " retry=", retry)
		}
		// Release a late successful attempt before admitting anything on B. Abort
		// does not flush/replay application payload or wait for an obsolete underlay.
		if stream != nil {
			if aborter, ok := stream.(interface{ Abort() error }); ok {
				_ = aborter.Abort()
			} else {
				_ = stream.Close()
			}
			stream = nil
		}
		lease.cancel(nil)
		if terminal != nil {
			return nil, terminal
		}
		if !retry {
			if err == nil {
				err = errDialCarrierReplaced
			}
			return nil, err
		}
		previous = lease
		if attempts <= 4 || attempts%10 == 0 {
			s.logger.Info("wlt pending dial retired attempt=", attempts, " generation=", lease.generation, " cause=local_interface_generation", " dial_attempt_id=", lease.attemptID, " interface_generation=", lease.interfaceGeneration)
		}
	}
}
