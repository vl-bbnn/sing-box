//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
)

type closeTraceConn struct {
	net.Conn
	fields []any
}

func (c closeTraceConn) DiagnosticFields() []any { return c.fields }

func TestCloseStreamDiagnosticBindsRealFlowAbortAndPendingTail(t *testing.T) {
	var output string
	c := &Carrier{logf: func(format string, args ...any) { output = fmt.Sprintf(format, args...) }, abortRequested: make(chan struct{})}
	conn := closeTraceConn{fields: []any{"kcp_wait_snd", 3, "kcp_drain_calls", int64(1), "kcp_drain_ms", int64(10000), "kcp_drain_result", "timeout", "private_credential", "must-never-appear"}}
	c.logStreamCloseDiagnostic("stream_graceful", context.DeadlineExceeded, 42, conn)
	for _, want := range []string{"flow_id=42", "carrier_abort_requested=false", "error_classes=deadline", "returned_error=true", "transport_diagnostics=true", "kcp_wait_snd=3", "kcp_drain_calls=1", "kcp_drain_ms=10000", "kcp_drain_result=timeout"} {
		if !strings.Contains(output, want) {
			t.Fatal(want)
		}
	}
	if strings.Contains(output, "private") || strings.Contains(output, "must-never") {
		t.Fatal("unlisted field leaked")
	}
	close(c.abortRequested)
	c.logStreamCloseDiagnostic("stream_graceful", errors.Join(context.Canceled, errors.New("private-secret")), 42, conn)
	if !strings.Contains(output, "carrier_abort_requested=true") || !strings.Contains(output, "error_classes=context_canceled,other") || strings.Contains(output, "private") {
		t.Fatal("incomplete or unsafe abort evidence")
	}
}

func TestCloseDiagnosticMissingAndMalformedFieldsStayUnknown(t *testing.T) {
	var output string
	c := &Carrier{logf: func(format string, args ...any) { output = fmt.Sprintf(format, args...) }}
	c.logStreamCloseDiagnostic("stream_graceful", context.Canceled, 2, nil)
	if !strings.Contains(output, "transport_diagnostics=false") || !strings.Contains(output, "kcp_drain_result=unknown") {
		t.Fatal("invented transport evidence")
	}
	conn := closeTraceConn{fields: []any{"kcp_drain_result", "private-secret", "kcp_wait_snd", "private-secret", "kcp_drain_ms", int64(-1), "dangling"}}
	c.logStreamCloseDiagnostic("stream_graceful", context.Canceled, 2, conn)
	if strings.Contains(output, "private") || !strings.Contains(output, "kcp_drain_result=unknown") || !strings.Contains(output, "kcp_wait_snd=0") {
		t.Fatal("unsafe malformed evidence")
	}
}
