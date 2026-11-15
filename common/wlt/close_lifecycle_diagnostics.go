//go:build with_wlt

package wlt

import "time"

const maxCloseLifecycleEvents uint64 = 4096

func (c *Carrier) reserveCloseLifecycleEvent() uint64 {
	if c == nil || !c.closeLifecycleDiagnostics || c.logf == nil {
		return 0
	}
	for {
		previous := c.closeLifecycleEvents.Load()
		if previous >= maxCloseLifecycleEvents {
			if c.closeLifecycleCapExhaustedLogged.CompareAndSwap(false, true) {
				c.logf("WLT close lifecycle phase=cap_exhausted event=%d wall_ms=%d cap=%d",
					maxCloseLifecycleEvents+1, time.Now().UnixMilli(), maxCloseLifecycleEvents)
			}
			return 0
		}
		if c.closeLifecycleEvents.CompareAndSwap(previous, previous+1) {
			return previous + 1
		}
	}
}

func closeDiagnosticFields(conn *carrierConn) []any {
	if conn == nil || conn.Conn == nil {
		return nil
	}
	if diagnostic, ok := conn.Conn.(interface{ DiagnosticFields() []any }); ok {
		return diagnostic.DiagnosticFields()
	}
	return nil
}

func closeDiagnosticNumber(fields []any, key string) int64 {
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] != key {
			continue
		}
		var number int64
		switch value := fields[i+1].(type) {
		case int:
			number = int64(value)
		case int64:
			number = value
		case uint16:
			number = int64(value)
		case uint64:
			if value > 1_000_000_000_000 {
				return 0
			}
			number = int64(value)
		default:
			return 0
		}
		if number < 0 || number > 1_000_000_000_000 {
			return 0
		}
		return number
	}
	return 0
}

func closeDiagnosticEnum(fields []any, key string, allowed ...string) string {
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] != key {
			continue
		}
		value, ok := fields[i+1].(string)
		if !ok || value == "" {
			return "none"
		}
		for _, candidate := range allowed {
			if value == candidate {
				return candidate
			}
		}
		return "other"
	}
	return "none"
}

func closeMode(immediate bool) string {
	if immediate {
		return "abort"
	}
	return "graceful"
}

func closePermitOwner(stream *carrierConn) string {
	if stream != nil && stream.drainOwned {
		return "drain"
	}
	return "active"
}

func (c *Carrier) logCloseLifecycleStart(event uint64, stream *carrierConn, immediate bool, started time.Time) {
	fields := closeDiagnosticFields(stream)
	c.logf("WLT close lifecycle phase=start event=%d wall_ms=%d flow_id=%d mode=%s active=%d slots=%d kcp_wait_snd=%d mux_pending=%d kcp_read_errors=%d kcp_close_reason=%s mux_close_reason=%s draining=%d drain_slots=%d hard_ceiling=%d permit_owner=%s",
		event, started.UnixMilli(), stream.FlowID(), closeMode(immediate), c.activeStreams.Load(), len(c.activeSlots),
		closeDiagnosticNumber(fields, "kcp_wait_snd"), closeDiagnosticNumber(fields, "kcp_underlying_pending"),
		closeDiagnosticNumber(fields, "kcp_read_errors"),
		closeDiagnosticEnum(fields, "kcp_close_reason", "local_close", "local_abort", "read_error:eof", "read_error:closed", "read_error:timeout", "write_error:eof", "write_error:closed", "write_error:timeout"),
		closeDiagnosticEnum(fields, "mux_flow_close_reason", "local_close_notified", "local_abort_notified", "remote_close", "remote_close_timeout"),
		c.drainingStreams.Load(), len(c.drainSlots), cap(c.activeSlots)+cap(c.drainSlots), closePermitOwner(stream))
}

func (c *Carrier) logCloseLifecycleEnd(event uint64, stream *carrierConn, immediate bool, started time.Time, closeErr error) {
	fields := closeDiagnosticFields(stream)
	outcome := "returned_nil"
	if closeErr != nil {
		outcome = "returned_error"
	}
	controlError := closeDiagnosticNumber(fields, "mux_close_control_error")
	if controlError > 1 {
		controlError = 1
	}
	c.logf("WLT close lifecycle phase=end event=%d wall_ms=%d flow_id=%d mode=%s duration_ms=%d kcp_close_ms=%d kcp_wait_snd=%d mux_pending=%d kcp_drain_calls=%d kcp_drain_ms=%d kcp_drain_result=%s mux_close_ms=%d mux_close_wait_ms=%d mux_close_control_ms=%d mux_close_control_error=%d mux_close_reason=%s active=%d slots=%d permit_released=1 draining=%d drain_slots=%d hard_ceiling=%d retired_owner=%s close_outcome=%s",
		event, time.Now().UnixMilli(), stream.FlowID(), closeMode(immediate), time.Since(started).Milliseconds(),
		closeDiagnosticNumber(fields, "kcp_close_ms"), closeDiagnosticNumber(fields, "kcp_wait_snd"),
		closeDiagnosticNumber(fields, "kcp_underlying_pending"), closeDiagnosticNumber(fields, "kcp_drain_calls"),
		closeDiagnosticNumber(fields, "kcp_drain_ms"), closeDiagnosticEnum(fields, "kcp_drain_result", "acked", "timeout", "aborted"),
		closeDiagnosticNumber(fields, "mux_close_ms"), closeDiagnosticNumber(fields, "mux_close_wait_ms"),
		closeDiagnosticNumber(fields, "mux_close_control_ms"), controlError,
		closeDiagnosticEnum(fields, "mux_flow_close_reason", "local_close_notified", "local_abort_notified", "remote_close", "remote_close_timeout"),
		c.activeStreams.Load(), len(c.activeSlots), c.drainingStreams.Load(), len(c.drainSlots),
		cap(c.activeSlots)+cap(c.drainSlots), closePermitOwner(stream), outcome)
}
