//go:build with_wlt

package daemon

import (
	"context"
	"encoding/json"
	boxlog "github.com/sagernet/sing-box/log"
	"testing"
	"time"
)

func TestCloseOutcomeForwardingExactSchema(t *testing.T) {
	var cases []struct {
		Line     string
		Accepted bool
	}
	if err := json.Unmarshal([]byte(`[{"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=aborted mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain close_outcome=returned_nil", "accepted": true}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=aborted mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain close_outcome=returned_error", "accepted": true}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=none mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain", "accepted": true}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=acked mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain", "accepted": true}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=timeout mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain", "accepted": true}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=other mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain", "accepted": true}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=aborted mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain close_outcome=SECRET_CANARY", "accepted": false}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=aborted mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drai", "accepted": false}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=aborted mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain close_outcome=returned_nil address=SECRET_CANARY", "accepted": false}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=aborted mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain close_outcome=returned_error:SECRET_CANARY", "accepted": false}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=aborted mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=2 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain", "accepted": false}, {"line": "WLT close lifecycle phase=end event=1 wall_ms=1791349714164 flow_id=95 mode=graceful duration_ms=4 kcp_close_ms=0 kcp_wait_snd=1 mux_pending=0 kcp_drain_calls=1 kcp_drain_ms=0 kcp_drain_result=SECRET_CANARY mux_close_ms=0 mux_close_wait_ms=0 mux_close_control_ms=0 mux_close_control_error=0 mux_close_reason=other active=0 slots=0 permit_released=1 draining=0 drain_slots=0 hard_ceiling=64 retired_owner=drain", "accepted": false}]`), &cases); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	formatter := boxlog.Formatter{BaseTime: now, DisableLineBreak: true}
	for _, c := range cases {
		message := formatter.Format(context.Background(), boxlog.LevelInfo, "service/wlt[wlt-carrier]", c.Line, now)
		if actual := forwardWLTRecoveryDiagnostic(message); actual != c.Accepted {
			t.Errorf("schema mismatch expected=%v: %s", c.Accepted, c.Line)
		}
		wrong := formatter.Format(context.Background(), boxlog.LevelInfo, "outbound", c.Line, now)
		if forwardWLTRecoveryDiagnostic(wrong) {
			t.Fatal("wrong producer admitted")
		}
	}
}
