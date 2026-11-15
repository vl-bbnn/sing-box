//go:build with_wlt

package wlt

import "time"

const maxActiveAdmissionDiagnosticEvents uint64 = 64

// activeAdmissionSample is an approximate concurrent snapshot. A stream can be
// opening or closing while the channel occupancies and registered set are read.
type activeAdmissionSample struct {
	activeSlotsUsed      int
	pendingSlotsUsed     int
	registered           int
	fresh                int
	pressureEligible     int
	closing              int
	oldestActivityMillis int64
}

func (c *Carrier) sampleActiveAdmission(now time.Time) activeAdmissionSample {
	sample := activeAdmissionSample{
		activeSlotsUsed:  len(c.activeSlots),
		pendingSlotsUsed: len(c.pendingSlots),
	}
	cutoff := now.Add(-c.pressureIdleTimeout).UnixNano()
	c.streamsMu.Lock()
	for stream := range c.streams {
		sample.registered++
		lastActivity := stream.lastActivity.Load()
		if lastActivity != 0 {
			ageMillis := now.Sub(time.Unix(0, lastActivity)).Milliseconds()
			if ageMillis > sample.oldestActivityMillis {
				sample.oldestActivityMillis = ageMillis
			}
		}
		if stream.admissionClosing.Load() {
			sample.closing++
			continue
		}
		if lastActivity != 0 && c.pressureIdleTimeout > 0 && lastActivity <= cutoff {
			sample.pressureEligible++
		} else {
			sample.fresh++
		}
	}
	c.streamsMu.Unlock()
	return sample
}

// logActiveAdmissionDiagnostic deliberately emits only aggregate counts and
// timings. The per-carrier limit prevents a busy client from flooding logs.
func (c *Carrier) logActiveAdmissionDiagnostic(stage string, queueElapsed time.Duration) {
	if c == nil || c.logf == nil {
		return
	}
	var event uint64
	for {
		previous := c.admissionDiagnosticEvents.Load()
		if previous >= maxActiveAdmissionDiagnosticEvents {
			return
		}
		if c.admissionDiagnosticEvents.CompareAndSwap(previous, previous+1) {
			event = previous + 1
			break
		}
	}
	now := time.Now()
	sample := c.sampleActiveAdmission(now)
	c.logf("WLT active admission stage=%s event=%d wall_ms=%d queue_elapsed_ms=%d active_capacity=%d pending_capacity=%d active_slots_used=%d pending_slots_used=%d active_count=%d pending_count=%d registered=%d fresh=%d pressure_eligible=%d closing=%d oldest_activity_ms=%d",
		stage, event, now.UnixMilli(), queueElapsed.Milliseconds(), cap(c.activeSlots), cap(c.pendingSlots),
		sample.activeSlotsUsed, sample.pendingSlotsUsed, c.activeStreams.Load(), c.pendingDials.Load(),
		sample.registered, sample.fresh, sample.pressureEligible, sample.closing, sample.oldestActivityMillis)
}
