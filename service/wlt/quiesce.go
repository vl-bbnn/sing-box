//go:build with_wlt

package wlt

import (
	"context"
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
			return ctx.Err()
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-changed:
		}
		s.access.Lock()
	}
	s.access.Unlock()
	return nil
}
