//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"io"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
)

var errDialServiceStopped = errors.Join(errors.New("wlt service is stopped"), context.Canceled)
var errDialCarrierReplaced = errors.Join(errors.New("wlt carrier replaced without a local generation recovery"), context.Canceled)

// dialLease owns only an unpublished dial. Removing it while holding access is
// the publication point; later network callbacks must never replay that stream.
type dialLease struct {
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
}

func (*localDialGenerationRetired) Error() string {
	return "wlt pending dial carrier generation retired locally"
}
func (*localDialGenerationRetired) Unwrap() error { return context.Canceled }

// retireDialLeasesLocked runs before carrier Abort, so mux shutdown can preserve
// the exact pending caller's cause. Terminal replacement does not grant retry.
func (s *Service) retireDialLeasesLocked(local bool) {
	for lease := range s.pendingDialLeases {
		if lease.carrier != s.carrier || lease.generation != s.carrierGeneration || lease.ctx.Err() != nil {
			continue
		}
		if local {
			lease.retired = &localDialGenerationRetired{carrier: lease.carrier, generation: lease.generation}
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
			lease := &dialLease{carrier: s.carrier, generation: s.carrierGeneration, interfaceGeneration: s.interfaceGeneration, ctx: attemptCtx, cancel: cancel}
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
	defer func() {
		if previous != nil {
			outcome := "published"
			if err != nil {
				outcome = "failed"
				err = &wltpkg.CarrierRecoveryError{Err: err}
			}
			s.logger.Info("wlt pending dial recovery attempts=", attempts, " retired_generation=", previous.generation, " final_generation=", finalGeneration, " outcome=", outcome)
		}
	}()
	for {
		var lease *dialLease
		lease, err = s.acquireDialLease(ctx, previous)
		if err != nil {
			return nil, err
		}
		attempts++
		finalGeneration = lease.generation
		if s.dialStreamHook != nil {
			stream, err = s.dialStreamHook(lease.ctx, lease.carrier, routeClass, target)
		} else {
			stream, err = lease.carrier.DialStream(lease.ctx, routeClass, target)
		}
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
			context.Cause(lease.ctx) == retired && (err == nil || errors.Is(err, retired) || errors.Is(err, context.Canceled))
		if terminal == nil && err == nil && current && retired == nil && lease.ctx.Err() == nil {
			s.access.Unlock()
			lease.cancel(nil)
			return stream, nil
		}
		s.access.Unlock()
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
			s.logger.Info("wlt pending dial retired attempt=", attempts, " generation=", lease.generation, " cause=local_interface_generation")
		}
	}
}
