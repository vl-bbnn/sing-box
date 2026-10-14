//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"io"
	"strings"
	"testing"
)

func TestCloseInputDiagnosticRetainsKnownLegacyInputResult(t *testing.T) {
	var output string
	c := &Carrier{logf: func(f string, a ...any) { output = fmt.Sprintf(f, a...) }}
	c.logStreamCloseDiagnostic("stream_graceful", context.DeadlineExceeded, 37, closeTraceConn{fields: []any{"kcp_drain_result", "legacy_input_closed"}})
	if !strings.Contains(output, "kcp_drain_result=legacy_input_closed") {
		t.Fatalf("known_legacy_result_masked: %s", output)
	}
}
func TestCloseInputDiagnosticBoundNumbersAndEnums(t *testing.T) {
	var output string
	c := &Carrier{logf: func(f string, a ...any) { output = fmt.Sprintf(f, a...) }}
	c.logStreamCloseDiagnostic("stream_graceful", context.DeadlineExceeded, 37, closeTraceConn{fields: []any{
		"kcp_lifetime_id", int64(812), "kcp_lifetime_process_id", int64(617), "kcp_lifetime_started_unix_ms", int64(12345), "kcp_drain_snapshot_observed", int64(1), "kcp_drain_snapshot_call", int64(2), "kcp_drain_snapshot_unix_ms", int64(54321), "kcp_drain_input_ended", int64(0), "kcp_drain_remote_data_close", int64(1), "kcp_drain_pending_mux", int64(3), "kcp_drain_pending_kcp", int64(4), "kcp_drain_protocol", "legacy", "kcp_drain_input_error", "open",
	}})
	for _, x := range []string{"kcp_lifetime_id=812", "kcp_lifetime_process_id=617", "kcp_lifetime_started_unix_ms=12345", "kcp_drain_snapshot_observed=1", "kcp_drain_snapshot_call=2", "kcp_drain_snapshot_unix_ms=54321", "kcp_drain_input_ended=0", "kcp_drain_remote_data_close=1", "kcp_drain_pending_mux=3", "kcp_drain_pending_kcp=4", "kcp_drain_protocol=legacy", "kcp_drain_input_error=open"} {
		if !strings.Contains(output, x) {
			t.Fatalf("missing bounded field %s: %s", x, output)
		}
	}
	secret := "private-value-must-not-appear"
	c.logStreamCloseDiagnostic("stream_graceful", context.DeadlineExceeded, 37, closeTraceConn{fields: []any{"kcp_lifetime_id", secret, "kcp_drain_snapshot_call", int64(-1), "kcp_drain_protocol", secret, "kcp_drain_input_error", secret, "kcp_drain_result", secret}})
	if strings.Contains(output, secret) || !strings.Contains(output, "kcp_drain_protocol=unknown") || !strings.Contains(output, "kcp_drain_input_error=unknown") || !strings.Contains(output, "kcp_drain_result=unknown") {
		t.Fatal("unsafe or incorrectly accepted diagnostic", output)
	}
}

func TestCloseInputDiagnosticRetainsKnownWholeDataInputResult(t *testing.T) {
	var output string
	c := &Carrier{logf: func(f string, a ...any) { output = fmt.Sprintf(f, a...) }}
	c.logStreamCloseDiagnostic("stream_graceful", context.DeadlineExceeded, 37, closeTraceConn{fields: []any{"kcp_drain_result", "data_input_closed"}})
	if !strings.Contains(output, "kcp_drain_result=data_input_closed") {
		t.Fatalf("known_whole_data_result_masked: %s", output)
	}
}

func TestCloseInputDiagnosticExplicitLossPreservesJoinedLeaves(t *testing.T) {
	if got := closeErrorClasses(carriercommon.ErrDataDrainUnacknowledged); got != "data_unacknowledged" {
		t.Fatalf("incomplete_loss_not_typed_%s", got)
	}
	if got := closeErrorClasses(errors.Join(carriercommon.ErrDataDrainUnacknowledged, io.EOF, context.Canceled)); got != "context_canceled,data_unacknowledged,eof" {
		t.Fatalf("joined loss suppressed: %s", got)
	}
}
