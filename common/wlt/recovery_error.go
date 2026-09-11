//go:build with_wlt

package wlt

// CarrierRecoveryError retains the terminal error after a logical dial entered
// local generation recovery. Outbounds must not turn that failure into a direct
// connection to the upstream server.
type CarrierRecoveryError struct{ Err error }

func (e *CarrierRecoveryError) Error() string { return e.Err.Error() }
func (e *CarrierRecoveryError) Unwrap() error { return e.Err }
