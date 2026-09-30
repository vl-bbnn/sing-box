//go:build with_wlt

package daemon

import "strings"

// Forward only fixed recovery/statistics messages to the bounded platform sink.
// Do not enable all debug logging or log destinations and credentials here.
func forwardWLTRecoveryDiagnostic(message string) bool {
	for _, marker := range []string{
		"updated default interface ", "missing default interface",
		"wlt service default interface ", "wlt service ignoring duplicate ",
		"wlt service replacing carrier ", "wlt service starting carrier after ",
		"wlt service restarting carrier ", "wlt service carrier restarted",
		"wlt service old carrier abort", "wlt service stats ",
		"relay client mux diagnostics registered", "relay client mux terminal stats",
		"relay client mux lifetime", "relay client peer write terminal stats",
		"memory pressure: ", "memory threshold reached,",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
