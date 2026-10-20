//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

type privateTimeoutLeaf bool

func (privateTimeoutLeaf) Error() string   { panic("private error text must never be inspected") }
func (e privateTimeoutLeaf) Timeout() bool { return bool(e) }

type recursiveTimeoutWrapper struct{}

func (*recursiveTimeoutWrapper) Error() string   { panic("private wrapper") }
func (e *recursiveTimeoutWrapper) Unwrap() error { return e }
func (*recursiveTimeoutWrapper) Timeout() bool   { panic("recursive wrapper Timeout must not be called") }

type observedApplicationCloseConn struct {
	closeTraceConn
	count  int64
	joined bool
}

func (c observedApplicationCloseConn) ApplicationReadRetirementStats() (int64, bool, string) {
	return c.count, c.joined, "private-auth-provider-error"
}

func TestCloseTimeoutObservationPreservesExistingClasses(t *testing.T) {
	cases := []struct {
		name              string
		err               error
		timeout, complete bool
		classes           string
	}{
		{"nil", nil, false, true, "none"},
		{"timeout", privateTimeoutLeaf(true), true, true, "other"},
		{"not-timeout", privateTimeoutLeaf(false), false, true, "other"},
		{"joined-mixed", errors.Join(context.Canceled, privateTimeoutLeaf(true), errors.New("private-unknown")), true, true, "context_canceled,other"},
		{"deadline", context.DeadlineExceeded, true, true, "deadline"},
		{"cycle", &recursiveTimeoutWrapper{}, false, false, "other"},
		{"empty-join", emptyCloseJoin{}, false, false, "other"},
	}
	for _, row := range cases {
		t.Run(row.name, func(t *testing.T) {
			seen, complete := closeTimeoutObservation(row.err)
			if seen != row.timeout || complete != row.complete || closeErrorClasses(row.err) != row.classes {
				t.Fatalf("incorrect observation: seen=%v complete=%v", seen, complete)
			}
		})
	}
	deep := error(privateTimeoutLeaf(true))
	for i := 0; i < 150; i++ {
		deep = &privateSingleWrapper{deep}
	}
	if _, complete := closeTimeoutObservation(deep); complete {
		t.Fatal("deep error falsely complete")
	}
	children := make([]error, 300)
	for i := range children {
		children[i] = privateTimeoutLeaf(true)
	}
	if _, complete := closeTimeoutObservation(errors.Join(children...)); complete {
		t.Fatal("wide error falsely complete")
	}
}

type privateSingleWrapper struct{ child error }

func (*privateSingleWrapper) Error() string   { panic("private wrapper text") }
func (e *privateSingleWrapper) Unwrap() error { return e.child }

func TestCloseObservationBindsRetirementWithoutPrivateText(t *testing.T) {
	var output string
	c := &Carrier{logf: func(format string, args ...any) { output = fmt.Sprintf(format, args...) }}
	conn := observedApplicationCloseConn{closeTraceConn: closeTraceConn{fields: []any{"kcp_retirement_result", "bilateral"}}, count: 45056, joined: true}
	c.logStreamCloseDiagnostic("stream_graceful", privateTimeoutLeaf(true), 27, conn)
	for _, want := range []string{"flow_id=27", "error_classes=other", "returned_error=true", "error_timeout_observed=true", "error_timeout_walk_complete=true", "kcp_retirement_result=bilateral", "application_read_retirement_observed=true", "application_read_retired_bytes_valid=true", "application_read_retired_bytes=45056", "application_read_retirement_joined=true"} {
		if !strings.Contains(output, want) {
			t.Fatal("missing bounded observation", want)
		}
	}
	if strings.Contains(output, "private") {
		t.Fatal("private value leaked")
	}
	conn.fields = []any{"kcp_retirement_result", "private-secret"}
	conn.count = -1
	c.logStreamCloseDiagnostic("stream_graceful", privateTimeoutLeaf(false), 27, conn)
	for _, want := range []string{"returned_error=true", "error_timeout_observed=false", "kcp_retirement_result=unknown", "application_read_retired_bytes_valid=false", "application_read_retired_bytes=0"} {
		if !strings.Contains(output, want) {
			t.Fatal("invalid observation admitted", want)
		}
	}
	if strings.Contains(output, "private") {
		t.Fatal("private malformed value leaked")
	}
	c.logStreamCloseDiagnostic("stream_graceful", nil, 27, nil)
	if !strings.Contains(output, "returned_error=false") || !strings.Contains(output, "application_read_retirement_observed=false") || !strings.Contains(output, "kcp_retirement_result=unknown") {
		t.Fatal("missing observer invented")
	}
}

func TestCloseRetirementEnumsAreFinite(t *testing.T) {
	for _, v := range []string{"owner_admission_rejected", "expired", "abort_or_error", "bilateral", "session_closed", "awaiting_peer_fin", "aborted"} {
		if closeRetirementResult(v) != v {
			t.Fatal("known enum lost", v)
		}
	}
	for _, v := range []any{nil, 42, "", "private-error", true} {
		if closeRetirementResult(v) != "unknown" {
			t.Fatal("unbounded enum admitted")
		}
	}
}

func TestCloseObservationActualKCPReadTimeout(t *testing.T) {
	packet, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer packet.Close()
	session, err := kcp.NewConn2(packet.LocalAddr(), nil, 0, 0, packet)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err = session.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	_, original := session.Read(make([]byte, 1))
	if original == nil {
		t.Fatal("real KCP deadline did not fail")
	}
	var timeout net.Error
	if !errors.As(original, &timeout) || !timeout.Timeout() {
		t.Fatal("actual error is not a network timeout")
	}
	seen, complete := closeTimeoutObservation(original)
	if !seen || !complete || closeErrorClasses(original) != "other" {
		t.Fatal("real KCP timeout lost or old classification changed")
	}
	var output string
	c := &Carrier{logf: func(format string, args ...any) { output = fmt.Sprintf(format, args...) }}
	c.logStreamCloseDiagnostic("stream_graceful", original, 2, nil)
	for _, want := range []string{"returned_error=true", "error_classes=other", "error_timeout_observed=true", "error_timeout_walk_complete=true"} {
		if !strings.Contains(output, want) {
			t.Fatal("actual KCP timeout observation missing", want)
		}
	}
}
