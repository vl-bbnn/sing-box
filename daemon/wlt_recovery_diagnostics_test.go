//go:build with_wlt

package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	boxlog "github.com/sagernet/sing-box/log"
)

func TestRecoveryDiagnosticWhitelist(t *testing.T) {
	for _, message := range []string{"ERROR [oom-killer]: memory pressure: critical, usage: 40 MiB, resetting network", "INFO wlt service stats opened=1 failed=0", "INFO relay client mux terminal stats opened=1", "INFO relay client peer write diagnostics registered session_uuid=test mux_diagnostic_id=test:1", "INFO relay client peer write terminal stats dropped=0", "WARN relay client peer write coverage incomplete reason=writer_wait", "WARN peer write failed peer_idx=1", "INFO wlt payload phase trace_id=1 phase=write_completed bytes=1524 error_class=none", "INFO relay client mux open locally retired mux_generation=2 request_id=4 dial_attempt_id=9", "INFO tinymux client open trace mux_generation=2 request_id=4 phase=canceled"} {
		if !forwardWLTRecoveryDiagnostic(message) {
			t.Fatalf("recovery diagnostic rejected: %s", message)
		}
	}
	for _, message := range []string{"outbound connection to example.com:443", "provider auth token=secret", "ordinary debug"} {
		if forwardWLTRecoveryDiagnostic(message) {
			t.Fatal("unrelated log forwarded")
		}
	}
}

func TestTrafficMilestoneDiagnosticExactFormatter(t *testing.T) {
	now := time.Now()
	formatter := boxlog.Formatter{BaseTime: now, DisableLineBreak: true}
	formatted := func(tag, event string) string {
		return formatter.Format(context.Background(), boxlog.LevelInfo, tag, event, now)
	}
	accepted := []string{
		"WLT startup phase=first_packet elapsed_ms=0 direction=write",
		"WLT startup phase=first_packet elapsed_ms=123 direction=read",
		"WLT startup phase=traffic_ready elapsed_ms=124",
	}
	for _, event := range accepted {
		if !forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-carrier]", event)) {
			t.Fatalf("safe traffic milestone rejected: %q", event)
		}
	}
	for _, event := range []string{
		"WLT startup phase=first_packet elapsed_ms=123 direction=read token=SECRET_CANARY",
		"WLT startup phase=traffic_ready elapsed_ms=124 target=SECRET_CANARY",
		"WLT startup phase=first_packet elapsed_ms=123 direction=read wlt service stats opened=1",
		"WLT startup phase=first_packet elapsed_ms=-1 direction=read",
		"WLT startup phase=first_packet elapsed_ms=123 direction=other",
		"WLT startup phase=first_packet elapsed_ms=12345678901234 direction=read",
		"WLT startup phase=traffic_ready elapsed_ms=124\nSECRET_CANARY",
		"WLT startup phase=traffic_ready elapsed_ms=124\x1b[31mSECRET_CANARY",
		"WLT startup phase=first_packet direction=read elapsed_ms=123",
		"WLT startup phase=first_packet elapsed_ms=123",
		"WLT startup phase=traffic_ready elapsed_ms=124 direction=read",
	} {
		if forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-carrier]", event)) {
			t.Fatalf("unsafe traffic milestone forwarded: %q", event)
		}
	}
	for _, event := range accepted {
		if forwardWLTRecoveryDiagnostic(formatted("outbound", event)) {
			t.Fatalf("non-WLT traffic milestone forwarded: %q", event)
		}
		if forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-other]", event)) {
			t.Fatalf("other WLT service traffic milestone forwarded: %q", event)
		}
	}
}

func TestBoundedLifecycleDiagnosticRealFormatter(t *testing.T) {
	now := time.Now()
	formatter := boxlog.Formatter{BaseTime: now, DisableLineBreak: true}
	formatted := func(tag, event string) string {
		return formatter.Format(context.Background(), boxlog.LevelInfo, tag, event, now)
	}
	forwarded := []string{
		"WLT carrier auth event=snapshot_loaded source=current remaining_ttl_seconds=1200 persistence=deferred",
		"WLT startup phase=provider_refresh_ready elapsed_ms=50 outcome=not_needed",
		"WLT carrier auth snapshot promoted after successful connect",
		"WLT carrier auth snapshot saved",
		"WLT carrier auth event=refresh_started source=current remaining_ttl_seconds=-1",
		"WLT carrier auth event=refresh_succeeded source=current remaining_ttl_seconds=1200",
		"WLT carrier auth refresh lifecycle event=saved_call_session_accepted category=none",
		"WLT carrier auth refresh lifecycle event=identity_renewal_not_entered category=none",
		"WLT carrier auth refresh lifecycle event=saved_call_session_rejected category=rejected",
		"WLT carrier auth refresh lifecycle event=calls_session_replacement_started category=none",
		"WLT carrier auth refresh lifecycle event=calls_session_replacement_ready category=none",
		"WLT carrier auth refresh lifecycle event=replacement_call_session_join_succeeded category=none",
		"WLT carrier auth event=fallback_started source=current_refreshed",
		"WLT carrier auth event=fallback_succeeded source=current_refreshed",
		"WLT carrier auth ring event=replenish_deferred reason=serialized_bearer_identity",
		"WLT carrier auth ring event=replenish_deferred retry_seconds=2 validated=false",
	}
	pre := "wlt service close_snapshot schema=2 phase=pre_close close_mode=graceful close_result=not_called closure_status=unverified snapshot_consistency=independent_reads carrier_generation=7 "
	post := "wlt service close_snapshot schema=2 phase=post_close close_mode=abort close_result=returned_error closure_status=unverified snapshot_consistency=independent_reads carrier_generation=7 "
	counters := strings.Join([]string{
		"active=0", "peak_active=1", "draining=0", "peak_draining=1", "drain_capacity=1", "pending=0", "peak_pending=1", "opened=3", "closed=3",
		"queued=0", "dns_open_requests=1", "dns_open_queued=0", "dns_open_rejected=0",
		"rejected=0", "rejected_queue=0", "rejected_active=0", "rejected_open=0",
		"pressure_reclaims=0", "failed=0", "max_dial_ms=50", "reconnect_retries=0",
		"reconnect_wait_ms=0", "reconnects=0", "last_reconnect=",
	}, " ")
	forwarded = append(forwarded, pre+counters, post+counters+"all peers disconnected",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.49.0123456789abcdef tree_nodes=4 tree_truncated=false",
		"wlt dial causal event=early_failure logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 original_transport_error=true observation_budget_ms=500",
		"wlt dial causal event=settled logical_dial_id=4 dial_attempt_id=7 outcome=retired_retry cause=carrier_failure_then_local_retirement observed_carrier_error=true",
		"wlt dial causal event=admitted logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 deadline_remaining_ms=-1",
		"wlt dial causal event=settled logical_dial_id=4 dial_attempt_id=7 outcome=published cause=none",
		"wlt dial causal event=settled logical_dial_id=4 dial_attempt_id=7 outcome=failed cause=deadline observed_carrier_error=true",
		"wlt dial causal event=settled logical_dial_id=4 dial_attempt_id=7 outcome=retired_retry cause=local_retirement observed_carrier_error=false",
		"wlt dial correlation event=final logical_dial_id=4 final_attempt_id=7 final_generation=2 attempts=1 outcome=failed",
		"wlt dial correlation event=retired logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 retry=true")
	for _, event := range forwarded {
		message := formatted("service/wlt[wlt-carrier]", event)
		if !forwardWLTRecoveryDiagnostic(message) {
			t.Fatalf("safe formatted event rejected: %q", event)
		}
		encoded, _ := json.Marshal(message)
		t.Log("WLT_FORWARDED_JSON=" + string(encoded))
	}
	rejected := []string{
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.49.0123456789abcdef tree_nodes=4 tree_truncated=false token=SECRET_CANARY",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.49.0123456789abcdef tree_nodes=4 tree_truncated=false\nSECRET_CANARY",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.49.0123456789abcdef tree_nodes=4 tree_truncated=false\r",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.49.0123456789abcdef tree_nodes=4 tree_truncated=false\x1b[0m",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.UNKNOWN.49.0123456789abcdef tree_nodes=4 tree_truncated=false",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.49.0123456789abcdef tree_nodes=17 tree_truncated=false",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.12345678901.0123456789abcdef tree_nodes=4 tree_truncated=false",
		"wlt dial causal event=carrier_error logical_dial_id=76 dial_attempt_id=76 carrier_generation=2 interface_generation=1 tree=0.0.wrap.0.0123456789abcdef,1.0.net_op.0.0123456789abcdef,2.1.syscall_op.0.0123456789abcdef,3.2.errno.49.0123456789abcdef tree_nodes=4 tree_truncated=SECRET_CANARY",
		"wlt dial causal event=early_failure logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 original_transport_error=true observation_budget_ms=500 token=SECRET_CANARY",
		"wlt dial causal event=early_failure logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 original_transport_error=false observation_budget_ms=500",
		"wlt dial causal event=early_failure logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 original_transport_error=true observation_budget_ms=5000",
		"WLT carrier auth refresh lifecycle event=saved_call_session_accepted category=none token=SECRET_CANARY",
		"WLT carrier auth refresh lifecycle event=saved_call_session_rejected category=authentication_required",
		"WLT carrier auth event=snapshot_loaded source=current remaining_ttl_seconds=1200 persistence=deferred\nsecret=SECRET_CANARY",
		"WLT carrier auth event=snapshot_loaded source=current remaining_ttl_seconds=1200 persistence=deferred\x1b[31mSECRET_CANARY",
		"WLT carrier auth snapshot saved\x1b[0m",
		"WLT carrier auth event=fallback_failed source=current_refreshed error=SECRET_CANARY",
		pre + counters + "error=SECRET_CANARY",
		post + counters + " extra=1",
		strings.Replace(pre+counters, "close_result=not_called", "close_result=returned_nil", 1),
		strings.Replace(post+counters, "closure_status=unverified", "closure_status=verified", 1),
		"wlt service stopped " + counters,
		"wlt dial causal event=admitted logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 deadline_remaining_ms=100 token=SECRET_CANARY",
		"wlt dial causal event=settled logical_dial_id=4 dial_attempt_id=7 outcome=failed cause=SECRET_CANARY observed_carrier_error=true",
		"wlt dial causal event=settled logical_dial_id=4 dial_attempt_id=7 outcome=failed cause=deadline observed_carrier_error=true\nSECRET_CANARY",
		"wlt dial causal event=settled logical_dial_id=4 dial_attempt_id=7 outcome=failed cause=deadline observed_carrier_error=true\x1b[0m",
		"wlt dial correlation event=final logical_dial_id=4 final_attempt_id=7 final_generation=2 attempts=1 outcome=failed token=SECRET_CANARY",
		"wlt dial correlation event=retired logical_dial_id=4 dial_attempt_id=7 carrier_generation=2 interface_generation=3 retry=true token=SECRET_CANARY",
		"WLT carrier auth event=unknown detail=wlt service stats SECRET_CANARY",
	}
	for _, event := range rejected {
		if forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-carrier]", event)) {
			t.Fatal("unsafe formatted event forwarded")
		}
	}
	if forwardWLTRecoveryDiagnostic(formatted("outbound", forwarded[0])) {
		t.Fatal("wrong log tag forwarded")
	}
}

func TestActiveAdmissionDiagnosticRealFormatter(t *testing.T) {
	now := time.Now()
	formatter := boxlog.Formatter{BaseTime: now, DisableLineBreak: true}
	formatted := func(tag, event string) string {
		return formatter.Format(context.Background(), boxlog.LevelInfo, tag, event, now)
	}
	base := "WLT active admission stage=saturated event=1 wall_ms=1790000000000 queue_elapsed_ms=0 active_capacity=56 pending_capacity=40 active_slots_used=56 pending_slots_used=0 active_count=56 pending_count=0 registered=56 fresh=50 pressure_eligible=1 closing=5 oldest_activity_ms=30000"
	for _, event := range []string{
		base,
		strings.Replace(strings.Replace(base, "stage=saturated event=1", "stage=wait_timeout event=64", 1), "queue_elapsed_ms=0", "queue_elapsed_ms=2000", 1),
	} {
		if !forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-carrier]", event)) {
			t.Fatalf("safe active admission sample rejected: %q", event)
		}
	}
	for _, event := range []string{
		base + " target=SECRET_CANARY",
		base + "\nsecret=SECRET_CANARY",
		base + "\x1b[31mSECRET_CANARY",
		strings.Replace(base, "stage=saturated", "stage=unknown", 1),
		strings.Replace(base, "event=1", "event=0", 1),
		strings.Replace(base, "event=1", "event=65", 1),
		strings.Replace(base, "event=1", "event=999999999999999999999", 1),
		strings.Replace(base, "active_slots_used=56", "active_slots_used=57", 1),
		strings.Replace(base, "active_capacity=56", "active_capacity=-1", 1),
		strings.Replace(base, "registered=56", "registered=55", 1),
		strings.Replace(base, "event=1", "event=1=2", 1),
		strings.Replace(base, "pending_count=0", "pending_count=0 wlt service stats SECRET_CANARY", 1),
	} {
		if forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-carrier]", event)) {
			t.Fatalf("unsafe active admission sample forwarded: %q", event)
		}
	}
	if forwardWLTRecoveryDiagnostic(formatted("outbound", base)) {
		t.Fatal("wrong-tag active admission sample forwarded")
	}
}

func TestCloseLifecycleDiagnosticRealFormatter(t *testing.T) {
	now := time.Now()
	formatter := boxlog.Formatter{BaseTime: now, DisableLineBreak: true}
	formatted := func(tag, event string) string {
		return formatter.Format(context.Background(), boxlog.LevelInfo, tag, event, now)
	}
	start := "WLT close lifecycle phase=start event=1 wall_ms=1790000000000 flow_id=42 mode=graceful active=56 slots=56 kcp_wait_snd=3 mux_pending=2 kcp_read_errors=1 kcp_close_reason=read_error:eof mux_close_reason=remote_close draining=1 drain_slots=1 hard_ceiling=64 permit_owner=drain"
	end := "WLT close lifecycle phase=end event=1 wall_ms=1790000000080 flow_id=42 mode=graceful duration_ms=80 kcp_close_ms=75 kcp_wait_snd=0 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=70 kcp_drain_result=acked mux_close_ms=5 mux_close_wait_ms=4 mux_close_control_ms=1 mux_close_control_error=0 mux_close_reason=local_close_notified active=55 slots=55 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain"
	marker := "WLT close lifecycle phase=cap_exhausted event=4097 wall_ms=1790000000090 cap=4096"
	for _, event := range []string{start, end, marker, strings.Replace(start, "mode=graceful", "mode=abort", 1), strings.Replace(start, "event=1", "event=4096", 1)} {
		if !forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-carrier]", event)) {
			t.Fatalf("safe close event rejected: %q", event)
		}
	}
	for _, event := range []string{start + " target=SECRET_CANARY", end + " token=SECRET_CANARY", marker + " token=SECRET_CANARY", strings.Replace(marker, "cap=4096", "cap=4097", 1), strings.Replace(end, "kcp_drain_result=acked", "kcp_drain_result=SECRET_CANARY", 1), strings.Replace(start, "kcp_close_reason=read_error:eof", "kcp_close_reason=read_error:SECRET_CANARY", 1), strings.Replace(start, "mux_close_reason=remote_close", "mux_close_reason=peer:SECRET_CANARY", 1), strings.Replace(end, "mux_close_reason=local_close_notified", "mux_close_reason=peer:SECRET_CANARY", 1), strings.Replace(start, "permit_owner=drain", "permit_owner=SECRET_CANARY", 1), strings.Replace(end, "retired_owner=drain", "retired_owner=SECRET_CANARY", 1), strings.Replace(start, "event=1", "event=4097", 1), strings.Replace(start, "flow_id=42", "flow_id=65536", 1), start + "\nSECRET_CANARY", start + "\x1b[31mSECRET_CANARY"} {
		if forwardWLTRecoveryDiagnostic(formatted("service/wlt[wlt-carrier]", event)) {
			t.Fatalf("unsafe close event forwarded: %q", event)
		}
	}
	if forwardWLTRecoveryDiagnostic(formatted("outbound", start)) {
		t.Fatal("wrong-tag close event forwarded")
	}
}
