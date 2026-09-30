//go:build !with_wlt

package daemon

func forwardWLTRecoveryDiagnostic(string) bool { return false }
