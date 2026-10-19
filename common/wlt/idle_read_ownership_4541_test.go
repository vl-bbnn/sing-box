//go:build with_wlt && wlt_deadline_fixture

package wlt_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/vl-bbnn/wlt-carrier/pkg/recoveryfixture"
	kcp "github.com/xtaci/kcp-go/v5"
)

func idleApp4541(t *testing.T, idle time.Duration, initialRead time.Time) (net.Conn, net.Conn, net.Conn, *observed4479, *wlt.Carrier, net.Conn) {
	t.Helper()
	ck, sk := recoveryfixture.DeadlinePair4479(t)
	if !initialRead.IsZero() {
		if e := ck.SetReadDeadline(initialRead); e != nil {
			t.Fatal(e)
		}
	}
	o := &observed4479{Conn: ck}
	carrier, stream := wlt.DeadlineFixtureCarrier4479(t, o, idle)
	client, e := vless.NewClient("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "", logger.NOP())
	if e != nil {
		t.Fatal(e)
	}
	app, e := client.DialEarlyConn(stream, M.Socksaddr{Fqdn: "example.com", Port: 443})
	if e != nil {
		t.Fatal(e)
	}
	return app, ck, sk, o, carrier, stream
}
func idleReadActive4541(t *testing.T, ck net.Conn) {
	t.Helper()
	end := time.Now().Add(time.Second)
	for time.Now().Before(end) {
		n, _, _ := ck.(interface{ ScopedApplicationReadOwnershipStats() (int, bool, error) }).ScopedApplicationReadOwnershipStats()
		if n == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("original managed Read not admitted")
}
func idleNumbers4541(ck net.Conn) map[string]int64 {
	out := map[string]int64{}
	f := ck.(interface{ DiagnosticFields() []any }).DiagnosticFields()
	for i := 0; i+1 < len(f); i += 2 {
		if k, ok := f[i].(string); ok {
			if v, ok := f[i+1].(int64); ok {
				out[k] = v
			}
		}
	}
	return out
}
func TestIdleReadOwnership4541(t *testing.T) {
	const baselineExpected = false
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("iteration-%02d", i), func(t *testing.T) {
			const idle = 200 * time.Millisecond
			app, ck, sk, o, carrier, stream := idleApp4541(t, idle, time.Time{})
			server := server4479(t, sk, []byte("prefix"))
			if _, e := app.Write([]byte("original application request")); e != nil {
				t.Fatal(e)
			}
			body := make([]byte, 6)
			if _, e := io.ReadFull(app, body); e != nil || !bytes.Equal(body, []byte("prefix")) {
				t.Fatal("original reply changed", e)
			}
			var callerDeadline time.Time
			callerSetter := "none"
			if i%3 == 1 {
				callerDeadline = time.Now().Add(time.Hour)
				callerSetter = "SetReadDeadline"
				if e := app.SetReadDeadline(callerDeadline); e != nil {
					t.Fatal(e)
				}
			}
			if i%3 == 2 {
				callerDeadline = time.Now().Add(time.Hour)
				callerSetter = "SetDeadline"
				if e := app.SetDeadline(callerDeadline); e != nil {
					t.Fatal(e)
				}
			}
			originalIdleDeadline, release := wlt.DeadlineFixtureHoldIdleWatch4541(t, carrier, stream)
			done := make(chan error, 1)
			go func() { _, e := app.Read(make([]byte, 1)); done <- e }()
			idleReadActive4541(t, ck)
			// Delay only the existing watchdog owner so the old competing KCP read
			// timer has a witnessed opportunity to win. The configured expiry and all
			// production code stay unchanged in the baseline control.
			var original error
			returned := false
			select {
			case original = <-done:
				returned = true
			case <-time.After(idle + 20*time.Millisecond):
			}
			if returned != baselineExpected {
				t.Fatal("internal read timer competed with original idle owner", returned, original)
			}
			if baselineExpected {
				var ne net.Error
				if !errors.As(original, &ne) || !ne.Timeout() || prior4479(ck) != original {
					t.Fatal("real baseline idle timeout missing", original)
				}
			}
			release()
			if !returned {
				original = wait4479(t, "idle-owned original application Read", done)
				if !errors.Is(original, kcp.ErrApplicationReadsRetired) {
					t.Fatal("original owned handoff missing", original)
				}
			}
			closeErr := app.Close()
			if baselineExpected {
				if !errors.Is(closeErr, original) {
					t.Fatal("baseline prior timeout not retained", closeErr)
				}
			} else if closeErr != nil || prior4479(ck) != nil {
				t.Fatal("idle owner created a prior or Close error", closeErr, prior4479(ck))
			}
			if e := wait4479(t, "actual peer retirement", server); e != nil {
				t.Fatal(e)
			}
			parts, first, reader, join := ck.(interface {
				ApplicationCloseErrorParts() (bool, error, error, error)
			}).ApplicationCloseErrorParts()
			if !parts || first != nil || reader != io.EOF || join != nil {
				t.Fatal("original retirement parts changed", parts, first, reader, join)
			}
			stats := idleNumbers4541(ck)
			if baselineExpected {
				if stats["kcp_prior_read_start_setter_origin"] != 3 || stats["kcp_prior_read_return_setter_origin"] != 3 || stats["kcp_prior_read_start_deadline_unix_ns"] != stats["kcp_prior_read_return_deadline_unix_ns"] {
					t.Fatal("not actual field idle prior shape", stats)
				}
			} else {
				for _, e := range o.snapshot() {
					if e.Kind == "SetDeadline" || e.Kind == "SetReadDeadline" {
						if callerSetter != "none" && e.Kind == callerSetter && e.Deadline == callerDeadline.UnixNano() {
							continue
						}
						t.Fatal("internal activity or Close changed shared read deadline", e)
					}
				}
			}
			record4479(t, fmt.Sprintf("idle-owner-%02d", i), o, map[string]any{"baseline_expected_timeout": baselineExpected, "caller_setter": callerSetter, "original_idle_deadline_unix_ns": originalIdleDeadline.UnixNano(), "original_idle_owner_not_reconfigured": true, "same_real_VLESS_KCP_PeerConn_and_unchanged_retirement": true, "caller_deadline_not_cleared_to_make_test_pass": true, "retirement_join_complete": true, "first_prior_numbers": stats, "whole_field_stability_not_qualified": true})
		})
	}
}
func TestIdleReadUnknownInitialCallerCap4541(t *testing.T) {
	deadline := time.Now().Add(250 * time.Millisecond)
	app, ck, sk, o, _, _ := idleApp4541(t, time.Second, deadline)
	server := server4479(t, sk, []byte("prefix"))
	if _, e := app.Write([]byte("request")); e != nil {
		t.Fatal(e)
	}
	if _, e := io.ReadFull(app, make([]byte, 6)); e != nil {
		t.Fatal(e)
	}
	start := time.Now()
	_, original := app.Read(make([]byte, 1))
	var ne net.Error
	if !errors.As(original, &ne) || !ne.Timeout() || time.Since(start) > 500*time.Millisecond || prior4479(ck) != original {
		t.Fatal("unknown initial real caller deadline changed", original, time.Since(start))
	}
	if e := app.Close(); !errors.Is(e, original) {
		t.Fatal("original caller timeout hidden", e)
	}
	if e := wait4479(t, "unknown initial caller peer retirement", server); e != nil {
		t.Fatal(e)
	}
	record4479(t, "unknown-initial-caller", o, map[string]any{"genuine_pre_activation_read_cap_preserved": true, "original_deadline_unix_ns": deadline.UnixNano(), "prior_raw_timeout_preserved_in_Close": true})
}
