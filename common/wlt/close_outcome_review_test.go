//go:build with_wlt

package wlt

import (
	"errors"
	"strings"
	"testing"
)

type pendingRetirementDiagnostic struct {
	*closeDiagnosticConn
	result error
}

func (c *pendingRetirementDiagnostic) DiagnosticFields() []any {
	return []any{"kcp_wait_snd", 1, "kcp_underlying_pending", 0, "kcp_drain_calls", int64(1), "kcp_drain_result", "aborted", "mux_close_control_error", int64(0)}
}
func (c *pendingRetirementDiagnostic) Close() error {
	c.closed.Store(true)
	return errors.Join(c.result, c.Conn.Close())
}
func (c *pendingRetirementDiagnostic) Abort() error { return c.Close() }
func TestCloseOutcomeRetainsUnackedRetirementAndActualReturn(t *testing.T) {
	for _, abort := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			t.Run(strings.Join([]string{map[bool]string{false: "graceful", true: "abort"}[abort], map[bool]string{false: "nil", true: "error"}[failed]}, "/"), func(t *testing.T) {
				carrier, stream, conn, lines := closeLifecycleTestStream(t)
				sentinel := errors.New("SECRET_CANARY_real_close_failure")
				wrapped := &pendingRetirementDiagnostic{closeDiagnosticConn: conn}
				if failed {
					wrapped.result = sentinel
				}
				stream.Conn = wrapped
				var err error
				if abort {
					err = stream.Abort()
				} else {
					err = stream.Close()
				}
				if failed {
					if !errors.Is(err, sentinel) {
						t.Fatalf("lost actual failure: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				last := (*lines)[len(*lines)-1]
				want := map[bool]string{false: "returned_nil", true: "returned_error"}[failed]
				if !strings.Contains(last, "close_outcome="+want) || !strings.Contains(last, "kcp_drain_result=aborted") || !strings.Contains(last, "kcp_wait_snd=1 mux_pending=0") {
					t.Fatalf("lost pending/cause/return diagnostic: %s", last)
				}
				if strings.Contains(last, "SECRET_CANARY") || strings.Contains(last, "kcp_drain_result=acked") {
					t.Fatal("leaked error or fabricated ACK")
				}
				if len(carrier.activeSlots) != 0 || len(carrier.drainSlots) != 0 || carrier.activeStreams.Load() != 0 || carrier.drainingStreams.Load() != 0 {
					t.Fatal("permit leak")
				}
			})
		}
	}
}
