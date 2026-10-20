//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestCloseDiagnosticPreservesWrappedClassesWithoutPrivateText(t *testing.T) {
	secret := "credential-must-never-appear"
	actual := errors.Join(fmt.Errorf("%s: %w", secret, context.Canceled), fmt.Errorf("%s: %w", secret, io.ErrClosedPipe))
	if got := closeErrorClasses(actual); got != "context_canceled,closed_pipe" {
		t.Fatal(got)
	}
	if got := closeErrorClasses(errors.New(secret)); got != "other" {
		t.Fatal(got)
	}
	var output string
	c := &Carrier{logf: func(format string, args ...any) { output = fmt.Sprintf(format, args...) }}
	c.logCloseDiagnostic("engine_abort", actual)
	if strings.Contains(output, secret) || !strings.Contains(output, "returned_error=true") {
		t.Fatal("unsafe or incomplete diagnostic")
	}
	if !errors.Is(actual, context.Canceled) || !errors.Is(actual, io.ErrClosedPipe) {
		t.Fatal("actual error changed")
	}
}
