//go:build with_wlt

package wlt

import (
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
)

type closeCauseObservationConn struct {
	closeTraceConn
	observed             bool
	original, read, join error
}

func (c closeCauseObservationConn) ApplicationCloseErrorParts() (bool, error, error, error) {
	return c.observed, c.original, c.read, c.join
}

func TestApplicationCloseCausePartsRemainPrivateAndSeparate(t *testing.T) {
	conn := closeCauseObservationConn{observed: true, read: privateTimeoutLeaf(true), join: io.EOF}
	suffix := applicationCloseCauseSuffix(conn, nil)
	for _, want := range []string{"application_close_parts_observed=true", "application_original_close_error_classes=none", "application_original_close_timeout=false", "application_retirement_read_error_classes=other", "application_retirement_read_timeout=true", "application_retirement_join_error_classes=eof", "application_retirement_join_timeout=false"} {
		if !strings.Contains(suffix, want) {
			t.Fatal("original error part lost", want)
		}
	}
	if strings.Contains(suffix, "private") {
		t.Fatal("private error value leaked")
	}
	conn.original = &recursiveTimeoutWrapper{}
	suffix = applicationCloseCauseSuffix(conn, nil)
	if !strings.Contains(suffix, "application_original_close_walk_complete=false") || !strings.Contains(suffix, "application_original_close_error_classes=other") {
		t.Fatal("cycle falsely complete")
	}
}

func TestApplicationCloseCauseUnobservedNeverInventsSuccess(t *testing.T) {
	for _, conn := range []any{nil, closeCauseObservationConn{read: privateTimeoutLeaf(true)}} {
		suffix := applicationCloseCauseSuffix(conn, nil)
		for _, want := range []string{"application_close_parts_observed=false", "application_original_close_error_classes=unobserved", "application_retirement_read_error_classes=unobserved", "application_retirement_join_error_classes=unobserved", "application_retirement_read_walk_complete=false"} {
			if !strings.Contains(suffix, want) {
				t.Fatal("unobserved phase fabricated", want)
			}
		}
	}
}

func TestApplicationCloseCauseLogBoundAndOriginalFields(t *testing.T) {
	numbers := map[string]int64{}
	addApplicationReadDeadlineKeys(numbers)
	fields := []any{"private-unrecognized", "private-credential"}
	for name := range numbers {
		value := int64(math.MaxInt64)
		if strings.HasSuffix(name, "_observed") || strings.HasSuffix(name, "_valid") || strings.HasSuffix(name, "_set") {
			value = 1
		}
		fields = append(fields, name, value)
	}
	conn := closeCauseObservationConn{closeTraceConn: closeTraceConn{fields: fields}, observed: true, read: privateTimeoutLeaf(true)}
	var output string
	c := &Carrier{logf: func(format string, args ...any) { output = fmt.Sprintf(format, args...) }}
	c.logStreamCloseDiagnostic("stream_graceful", privateTimeoutLeaf(true), 65535, conn)
	for _, want := range []string{"flow_id=65535", "returned_error=true", "error_classes=other", "application_retirement_read_timeout=true", "kcp_application_read_start_deadline_unix_ns=9223372036854775807", "kcp_application_read_deadline_observed=1"} {
		if !strings.Contains(output, want) {
			t.Fatal("source-bound field lost", want)
		}
	}
	if strings.Contains(output, "private") || len(output) > 3800 {
		t.Fatal("privacy or Android line budget violated", len(output))
	}
	conn.fields = []any{"kcp_application_read_start_deadline_unix_ns", int64(-1), "kcp_application_read_deadline_observed", int64(17)}
	c.logStreamCloseDiagnostic("stream_graceful", privateTimeoutLeaf(true), 2, conn)
	if !strings.Contains(output, "kcp_application_read_start_deadline_unix_ns=0") || !strings.Contains(output, "kcp_application_read_deadline_observed=0") {
		t.Fatal("invalid deadline field admitted")
	}
}
