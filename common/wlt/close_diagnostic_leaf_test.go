//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
)

type cyclicCloseError struct{}

func (*cyclicCloseError) Error() string   { return "private-cycle" }
func (e *cyclicCloseError) Unwrap() error { return e }

type emptyCloseJoin struct{}

func (emptyCloseJoin) Error() string   { return "private-empty" }
func (emptyCloseJoin) Unwrap() []error { return []error{nil} }

func TestCloseDiagnosticMixedUnknownLeafCannotAppearCanceledOnly(t *testing.T) {
	err := errors.Join(fmt.Errorf("private-known: %w", context.Canceled), fmt.Errorf("private-unknown: %w", errors.New("private-root")))
	if got := closeErrorClasses(err); got != "context_canceled,other" {
		t.Fatalf("incomplete close classes: %s", got)
	}
}

func TestCloseDiagnosticLeafClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "none"},
		{"wrapped", fmt.Errorf("private: %w", context.Canceled), "context_canceled"},
		{"nested join", errors.Join(errors.Join(io.EOF, context.Canceled), errors.Join(net.ErrClosed, context.DeadlineExceeded, io.ErrClosedPipe)), "context_canceled,deadline,closed_pipe,net_closed,eof"},
		{"mixed deadline", errors.Join(context.DeadlineExceeded, errors.New("private")), "deadline,other"},
		{"unknown wrapped", fmt.Errorf("private: %w", errors.New("private-leaf")), "other"},
		{"unknown", errors.New("private"), "other"},
		{"empty", emptyCloseJoin{}, "other"},
		{"cycle", &cyclicCloseError{}, "other"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := closeErrorClasses(c.err); got != c.want {
				t.Fatalf("got=%s want=%s", got, c.want)
			}
		})
	}
	deep := error(context.Canceled)
	for i := 0; i < 150; i++ {
		deep = fmt.Errorf("private-deep: %w", deep)
	}
	if got := closeErrorClasses(deep); got != "other" {
		t.Fatalf("incomplete budget must remain unknown: %s", got)
	}
	var output string
	carrier := &Carrier{logf: func(format string, args ...any) { output = fmt.Sprintf(format, args...) }}
	carrier.logCloseDiagnostic("stream_graceful", errors.Join(context.Canceled, errors.New("private-secret")))
	if strings.Contains(output, "private") || !strings.Contains(output, "error_classes=context_canceled,other") || !strings.Contains(output, "returned_error=true") {
		t.Fatal("unsafe/incomplete output")
	}
}
