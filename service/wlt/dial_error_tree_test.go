//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
)

type diagnosticCanaryError struct{}

func (*diagnosticCanaryError) Error() string { panic("arbitrary Error() invoked") }

type diagnosticCycle struct{}

func (e *diagnosticCycle) Unwrap() error { return e }
func (*diagnosticCycle) Error() string   { panic("cyclic Error() invoked") }
func TestBoundedDialErrorTreeActualNetworkJoin(t *testing.T) {
	e := fmt.Errorf("route=SECRET_CANARY target=SECRET_CANARY: %w", errors.Join(&net.OpError{Op: "SECRET_CANARY", Net: "udp", Addr: &net.UDPAddr{}, Err: &os.SyscallError{Syscall: "SECRET_CANARY", Err: syscall.EADDRNOTAVAIL}}, context.Canceled))
	tree, n, truncated := boundedDialErrorTree(e)
	if n != 6 || truncated || strings.Contains(tree, "SECRET") {
		t.Fatalf("unsafe or incomplete tree: %s nodes=%d truncated=%v", tree, n, truncated)
	}
	for _, kind := range []string{".wrap.", ".join.", ".net_op.", ".syscall_op.", ".errno.", ".canceled."} {
		if !strings.Contains(tree, kind) {
			t.Fatal("missing identity", kind)
		}
	}
	if !strings.Contains(tree, fmt.Sprintf(".errno.%d.", syscall.EADDRNOTAVAIL)) {
		t.Fatal("numeric errno identity lost")
	}
}
func TestBoundedDialErrorTreeNeverCallsUnknownError(t *testing.T) {
	tree, n, truncated := boundedDialErrorTree(&diagnosticCanaryError{})
	if n != 1 || truncated || !strings.Contains(tree, ".leaf.") {
		t.Fatal(tree)
	}
}
func TestBoundedDialErrorTreeCycleAndWidth(t *testing.T) {
	tree, n, truncated := boundedDialErrorTree(&diagnosticCycle{})
	if n != 8 || !truncated || len(tree) > 1024 {
		t.Fatal(tree, n, truncated)
	}
	many := make([]error, 1000)
	for i := range many {
		many[i] = io.EOF
	}
	_, n, truncated = boundedDialErrorTree(errors.Join(many...))
	if n != 16 || !truncated {
		t.Fatal(n, truncated)
	}
}
func TestBoundedDialErrorTreeFixedLeafVocabulary(t *testing.T) {
	for _, text := range []string{"mux: closed", "tinymux client session closed", "tinymux pong timeout", "tinymux remote disconnect", "SECRET_CANARY"} {
		tree, n, truncated := boundedDialErrorTree(errors.New(text))
		if n != 1 || truncated || strings.Contains(tree, text) {
			t.Fatal(tree)
		}
	}
}
