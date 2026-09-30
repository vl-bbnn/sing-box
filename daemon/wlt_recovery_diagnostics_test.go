//go:build with_wlt

package daemon

import "testing"

func TestRecoveryDiagnosticWhitelist(t *testing.T) {
	for _, message := range []string{"ERROR [oom-killer]: memory pressure: critical, usage: 40 MiB, resetting network", "INFO wlt service stats opened=1 failed=0", "INFO relay client mux terminal stats opened=1"} {
		if !forwardWLTRecoveryDiagnostic(message) {
			t.Fatalf("recovery diagnostic rejected: %s", message)
		}
	}
	for _, message := range []string{"outbound connection to example.com:443", "provider auth token=secret", "ordinary debug"} {
		if forwardWLTRecoveryDiagnostic(message) {
			t.Fatal("unrelated log forwarded")
		}
	}
}
