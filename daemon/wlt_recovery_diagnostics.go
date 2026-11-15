//go:build with_wlt

package daemon

import (
	"regexp"
	"strconv"
	"strings"
)

var wltDiagnosticEnvelope = regexp.MustCompile(`^INFO\[[0-9]{4,}\] (?:\[[0-9]+ [0-9a-z.]+\] )?service/wlt\[wlt-carrier\]: (.+)$`)

// These are complete, credential-free producer forms. A substring match could
// forward a token or error appended to an otherwise safe auth event.
var wltAuthDiagnosticEvents = []*regexp.Regexp{
	regexp.MustCompile(`^WLT carrier auth event=snapshot_loaded source=current remaining_ttl_seconds=-?[0-9]+ persistence=deferred$`),
	regexp.MustCompile(`^WLT startup phase=provider_refresh_ready elapsed_ms=[0-9]+ outcome=(not_needed|cached|refreshed)$`),
	regexp.MustCompile(`^WLT carrier auth snapshot promoted after successful connect$`),
	regexp.MustCompile(`^WLT carrier auth snapshot saved$`),
	regexp.MustCompile(`^WLT carrier auth event=refresh_started source=current remaining_ttl_seconds=-?[0-9]+$`),
	regexp.MustCompile(`^WLT carrier auth event=refresh_succeeded source=current remaining_ttl_seconds=-?[0-9]+$`),
	regexp.MustCompile(`^WLT carrier auth refresh lifecycle event=saved_call_session_accepted category=none$`),
	regexp.MustCompile(`^WLT carrier auth refresh lifecycle event=identity_renewal_not_entered category=none$`),
	regexp.MustCompile(`^WLT carrier auth refresh lifecycle event=saved_call_session_rejected category=rejected$`),
	regexp.MustCompile(`^WLT carrier auth refresh lifecycle event=calls_session_replacement_started category=none$`),
	regexp.MustCompile(`^WLT carrier auth refresh lifecycle event=calls_session_replacement_ready category=none$`),
	regexp.MustCompile(`^WLT carrier auth refresh lifecycle event=replacement_call_session_join_succeeded category=none$`),
	regexp.MustCompile(`^WLT carrier auth event=fallback_started source=current_refreshed$`),
	regexp.MustCompile(`^WLT carrier auth event=fallback_succeeded source=current_refreshed$`),
	regexp.MustCompile(`^WLT carrier auth ring event=replenish_deferred reason=serialized_bearer_identity$`),
	regexp.MustCompile(`^WLT carrier auth ring event=replenish_deferred retry_seconds=[0-9]+ validated=false$`),
}

// Only the first payload markers' fixed numeric/direction fields may reach
// platform diagnostics. Arbitrary WLT startup text can contain private data.
var wltTrafficMilestoneDiagnostics = []*regexp.Regexp{
	regexp.MustCompile(`^WLT startup phase=first_packet elapsed_ms=[0-9]{1,13} direction=(read|write)$`),
	regexp.MustCompile(`^WLT startup phase=traffic_ready elapsed_ms=[0-9]{1,13}$`),
}

var wltCloseSnapshotDiagnostic = regexp.MustCompile(`^wlt service close_snapshot schema=2 phase=(?:pre_close close_mode=(?:graceful|abort) close_result=not_called|post_close close_mode=(?:graceful|abort) close_result=(?:returned_nil|returned_error)) closure_status=unverified snapshot_consistency=independent_reads carrier_generation=[0-9]+ active=[0-9]+ peak_active=[0-9]+ draining=[0-9]+ peak_draining=[0-9]+ drain_capacity=[0-9]+ pending=[0-9]+ peak_pending=[0-9]+ opened=[0-9]+ closed=[0-9]+ queued=[0-9]+ dns_open_requests=[0-9]+ dns_open_queued=[0-9]+ dns_open_rejected=[0-9]+ rejected=[0-9]+ rejected_queue=[0-9]+ rejected_active=[0-9]+ rejected_open=[0-9]+ pressure_reclaims=[0-9]+ failed=[0-9]+ max_dial_ms=[0-9]+ reconnect_retries=[0-9]+ reconnect_wait_ms=[0-9]+ reconnects=[0-9]+ last_reconnect=(|all peers disconnected)$`)

// Only opaque IDs, finite enums and bounded numeric close metrics may enter
// retained device diagnostics. Unknown suffixes and transport errors are denied.
var wltCloseLifecycleDiagnostics = []*regexp.Regexp{
	regexp.MustCompile(`^WLT close lifecycle phase=start event=[0-9]{1,4} wall_ms=[0-9]{13} flow_id=[0-9]{1,5} mode=(?:graceful|abort) active=[0-9]{1,6} slots=[0-9]{1,4} kcp_wait_snd=[0-9]{1,13} mux_pending=[0-9]{1,13} kcp_read_errors=[0-9]{1,13} kcp_close_reason=(?:none|local_close|local_abort|read_error:(?:eof|closed|timeout)|write_error:(?:eof|closed|timeout)|other) mux_close_reason=(?:none|local_close_notified|local_abort_notified|remote_close|remote_close_timeout|other) draining=[0-9]{1,6} drain_slots=[0-9]{1,4} hard_ceiling=[0-9]{1,4} permit_owner=(?:active|drain)$`),
	regexp.MustCompile(`^WLT close lifecycle phase=end event=[0-9]{1,4} wall_ms=[0-9]{13} flow_id=[0-9]{1,5} mode=(?:graceful|abort) duration_ms=[0-9]{1,13} kcp_close_ms=[0-9]{1,13} kcp_wait_snd=[0-9]{1,13} mux_pending=[0-9]{1,13} kcp_drain_calls=[0-9]{1,13} kcp_drain_ms=[0-9]{1,13} kcp_drain_result=(?:none|acked|timeout|aborted|other) mux_close_ms=[0-9]{1,13} mux_close_wait_ms=[0-9]{1,13} mux_close_control_ms=[0-9]{1,13} mux_close_control_error=[01] mux_close_reason=(?:none|local_close_notified|local_abort_notified|remote_close|remote_close_timeout|other) active=[0-9]{1,6} slots=[0-9]{1,4} permit_released=1 draining=[0-9]{1,6} drain_slots=[0-9]{1,4} hard_ceiling=[0-9]{1,4} retired_owner=(?:active|drain)(?: close_outcome=(?:returned_nil|returned_error))?$`),
}
var wltCloseLifecycleCapExhaustedDiagnostic = regexp.MustCompile(`^WLT close lifecycle phase=cap_exhausted event=4097 wall_ms=[0-9]{13} cap=4096$`)

func validWLTCloseLifecycle(event string) bool {
	if len(event) > 512 {
		return false
	}
	if wltCloseLifecycleCapExhaustedDiagnostic.MatchString(event) {
		return true
	}
	for _, pattern := range wltCloseLifecycleDiagnostics {
		if !pattern.MatchString(event) {
			continue
		}
		parts := strings.Split(event, " ")
		eventText, ok := strings.CutPrefix(parts[4], "event=")
		if !ok {
			return false
		}
		sequence, err := strconv.ParseUint(eventText, 10, 64)
		if err != nil || sequence < 1 || sequence > 4096 {
			return false
		}
		flowText, ok := strings.CutPrefix(parts[6], "flow_id=")
		if !ok {
			return false
		}
		flow, err := strconv.ParseUint(flowText, 10, 16)
		return err == nil && flow <= 65535
	}
	return false
}

// Service dial diagnostics carry only wire-local numeric IDs and finite causes.
// Match the full producer form before forwarding to the bounded command log.
var wltDialCausalDiagnostics = []*regexp.Regexp{
	regexp.MustCompile(`^wlt dial causal event=carrier_error logical_dial_id=[0-9]{1,20} dial_attempt_id=[0-9]{1,20} carrier_generation=[0-9]{1,20} interface_generation=[0-9]{1,20} tree=[0-9]{1,2}\.[0-9]{1,2}\.(?:nil|leaf|net_op|syscall_op|errno|errno_other|local_retired|canceled|deadline|eof|unexpected_eof|closed_pipe|net_closed|service_stopped|join|wrap|mux_closed|session_closed|pong_timeout|remote_disconnect)\.[0-9]{1,10}\.[a-f0-9]{16}(?:,[0-9]{1,2}\.[0-9]{1,2}\.(?:nil|leaf|net_op|syscall_op|errno|errno_other|local_retired|canceled|deadline|eof|unexpected_eof|closed_pipe|net_closed|service_stopped|join|wrap|mux_closed|session_closed|pong_timeout|remote_disconnect)\.[0-9]{1,10}\.[a-f0-9]{16}){0,15} tree_nodes=(?:[1-9]|1[0-6]) tree_truncated=(?:true|false)$`),
	regexp.MustCompile(`^wlt dial causal event=early_failure logical_dial_id=[0-9]+ dial_attempt_id=[0-9]+ carrier_generation=[0-9]+ interface_generation=[0-9]+ original_transport_error=true observation_budget_ms=500$`),
	regexp.MustCompile(`^wlt dial causal event=admitted logical_dial_id=[0-9]+ dial_attempt_id=[0-9]+ carrier_generation=[0-9]+ interface_generation=[0-9]+ deadline_remaining_ms=-?[0-9]+$`),
	regexp.MustCompile(`^wlt dial causal event=settled logical_dial_id=[0-9]+ dial_attempt_id=[0-9]+ outcome=published cause=none$`),
	regexp.MustCompile(`^wlt dial causal event=settled logical_dial_id=[0-9]+ dial_attempt_id=[0-9]+ outcome=(?:failed|retired_retry) cause=(?:service_stop|deadline|caller_canceled|local_retirement|carrier_failure_then_local_retirement|carrier_replaced|carrier_failure|unknown) observed_carrier_error=(?:true|false)$`),
	regexp.MustCompile(`^wlt dial correlation event=final logical_dial_id=[0-9]+ final_attempt_id=[0-9]+ final_generation=[0-9]+ attempts=[0-9]+ outcome=(?:published|failed)$`),
	regexp.MustCompile(`^wlt dial correlation event=retired logical_dial_id=[0-9]+ dial_attempt_id=[0-9]+ carrier_generation=[0-9]+ interface_generation=[0-9]+ retry=(?:true|false)$`),
}

// Keep the carrier's 64-event aggregate admission sample intact while
// rejecting every unknown field, oversized number, or appended payload.
var wltActiveAdmissionKeys = []string{
	"event", "wall_ms", "queue_elapsed_ms", "active_capacity", "pending_capacity",
	"active_slots_used", "pending_slots_used", "active_count", "pending_count",
	"registered", "fresh", "pressure_eligible", "closing", "oldest_activity_ms",
}

func validWLTActiveAdmission(event string) bool {
	parts := strings.Split(event, " ")
	if len(parts) != 4+len(wltActiveAdmissionKeys) || parts[0] != "WLT" ||
		parts[1] != "active" || parts[2] != "admission" {
		return false
	}
	if parts[3] != "stage=saturated" && parts[3] != "stage=wait_timeout" {
		return false
	}
	values := make([]int64, len(wltActiveAdmissionKeys))
	for i, key := range wltActiveAdmissionKeys {
		valueText, ok := strings.CutPrefix(parts[i+4], key+"=")
		if !ok || len(valueText) == 0 || len(valueText) > 13 {
			return false
		}
		for _, digit := range valueText {
			if digit < '0' || digit > '9' {
				return false
			}
		}
		value, err := strconv.ParseInt(valueText, 10, 64)
		if err != nil {
			return false
		}
		values[i] = value
	}
	return values[0] >= 1 && values[0] <= 64 &&
		values[1] >= 1_000_000_000_000 && values[1] <= 9_999_999_999_999 &&
		values[2] <= 1_000_000_000_000 &&
		values[3] >= 1 && values[3] <= 4096 &&
		values[4] >= 1 && values[4] <= 4096 &&
		values[5] <= values[3] && values[6] <= values[4] &&
		values[7] <= 1_000_000 && values[8] <= 1_000_000 &&
		values[9] <= 1_000_000 && values[10] <= values[9] &&
		values[11] <= values[9] && values[12] <= values[9] &&
		values[10]+values[11]+values[12] == values[9] &&
		values[13] <= 1_000_000_000_000
}

func forwardWLTBoundedLifecycleDiagnostic(message string) bool {
	if strings.ContainsAny(message, "\r\n") {
		return false
	}
	plain := message
	const coloredInfo = "\x1b[36mINFO\x1b[0m"
	if strings.HasPrefix(plain, coloredInfo) {
		plain = "INFO" + strings.TrimPrefix(plain, coloredInfo)
	}
	if strings.ContainsRune(plain, '\x1b') {
		return false
	}
	match := wltDiagnosticEnvelope.FindStringSubmatch(plain)
	if match == nil {
		return false
	}
	event := match[1]
	if validWLTCloseLifecycle(event) {
		return true
	}
	if validWLTActiveAdmission(event) {
		return true
	}
	if wltCloseSnapshotDiagnostic.MatchString(event) {
		return true
	}
	for _, pattern := range wltDialCausalDiagnostics {
		if pattern.MatchString(event) {
			return true
		}
	}
	for _, pattern := range wltAuthDiagnosticEvents {
		if pattern.MatchString(event) {
			return true
		}
	}
	for _, pattern := range wltTrafficMilestoneDiagnostics {
		if pattern.MatchString(event) {
			return true
		}
	}
	return false
}

// Forward only fixed recovery/statistics messages to the bounded platform sink.
// Do not enable all debug logging or log destinations and credentials here.
func forwardWLTRecoveryDiagnostic(message string) bool {
	if strings.Contains(message, "WLT carrier auth ") ||
		strings.Contains(message, "WLT active admission ") ||
		strings.Contains(message, "WLT close lifecycle ") ||
		strings.Contains(message, "WLT startup phase=provider_refresh_ready") ||
		strings.Contains(message, "WLT startup phase=first_packet") ||
		strings.Contains(message, "WLT startup phase=traffic_ready") ||
		strings.Contains(message, "wlt service close_snapshot ") ||
		strings.Contains(message, "wlt dial causal event=") ||
		strings.Contains(message, "wlt dial correlation event=") {
		return forwardWLTBoundedLifecycleDiagnostic(message)
	}
	if forwardWLTBoundedLifecycleDiagnostic(message) {
		return true
	}
	for _, marker := range []string{
		"updated default interface ", "missing default interface",
		"wlt service default interface ", "wlt service ignoring duplicate ",
		"wlt service replacing carrier ", "wlt service starting carrier after ",
		"wlt service restarting carrier ", "wlt service carrier restarted",
		"wlt service old carrier abort", "wlt service stats ",
		"relay client mux diagnostics registered", "relay client mux terminal stats",
		"relay client mux lifetime", "relay client peer write diagnostics registered",
		"relay client mux open locally retired", "tinymux client open trace",
		"relay client peer write terminal stats", "relay client peer write coverage incomplete",
		"peer write failed",
		"wlt payload phase ",
		"memory pressure: ", "memory threshold reached,",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
