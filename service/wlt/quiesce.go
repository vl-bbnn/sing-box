//go:build with_wlt

package wlt

import (
	"context"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	E "github.com/sagernet/sing/common/exceptions"
	"time"
)

// QuiescePendingDials closes admission and lets already admitted OPENs settle
// while the carrier and caller contexts are still usable. A deadline or actual
// network failure is not erased: the owner then cancels and closes normally.
// Published streams are neither replayed nor held open by this barrier.
func (s *Service) QuiescePendingDials(ctx context.Context) (err error) {
	started := time.Now()
	s.access.Lock()
	initial := len(s.pendingDialLeases)
	s.quiescing = true
	s.notifyDialStateLocked()
	// Retire an obsolete underlay before the owner cancels the instance context.
	// A context watcher can otherwise win closeOnce and start a graceful drain.
	// Only authoritative interface state permits this; ordinary stops still drain.
	var obsolete *wltpkg.Carrier
	var cancelStart context.CancelFunc
	if s.carrier != nil && (s.interfaceUnavailable ||
		interfaceIdentityChanged(s.carrierInterfaceKey, s.interfaceKey) ||
		interfaceIdentityChanged(s.carrierInterfaceKey, s.currentInterfaceKey())) {
		obsolete = s.carrier
		s.carrierRetiredForShutdown = true
		s.retireDialLeasesLocked(true)
		cancelStart = s.carrierStartCancel
		s.carrierStartCancel = nil
	}
	s.access.Unlock()
	var retireErr error
	if obsolete != nil {
		retireErr = s.abortCarrier(obsolete)
		if cancelStart != nil {
			cancelStart()
		}
		s.logger.Info("wlt shutdown obsolete carrier retired abort_completed=true returned_error=", retireErr != nil)
	}
	s.access.Lock()
	defer func() {
		s.access.RLock()
		remaining := len(s.pendingDialLeases)
		s.access.RUnlock()
		s.logger.Info("wlt shutdown dial drain pending=", initial, " remaining=", remaining,
			" elapsed_ms=", time.Since(started).Milliseconds(), " completed=", err == nil)
	}()
	for len(s.pendingDialLeases) > 0 {
		changed := s.dialStateChanged
		s.access.Unlock()
		select {
		case <-ctx.Done():
			return E.Errors(retireErr, ctx.Err())
		case <-s.ctx.Done():
			return E.Errors(retireErr, s.ctx.Err())
		case <-changed:
		}
		s.access.Lock()
	}
	s.access.Unlock()
	return retireErr
}
