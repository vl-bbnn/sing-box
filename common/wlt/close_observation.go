//go:build with_wlt

package wlt

// closeTimeoutObservation supplements the existing error classes. Incomplete
// traversal is explicit; a timeout observation never changes a returned error.
// Inspect leaves only: wrapper Timeout methods can recursively inspect children.
func closeTimeoutObservation(err error) (observed, complete bool) {
	complete = true
	pending := []error{err}
	budget := 128
	for len(pending) > 0 {
		if budget == 0 {
			return observed, false
		}
		budget--
		node := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if node == nil {
			continue
		}
		switch wrapped := node.(type) {
		case interface{ Unwrap() []error }:
			actual := false
			for _, child := range wrapped.Unwrap() {
				if child == nil {
					continue
				}
				if len(pending) >= budget {
					return observed, false
				}
				pending = append(pending, child)
				actual = true
			}
			if !actual {
				complete = false
			}
			continue
		case interface{ Unwrap() error }:
			if child := wrapped.Unwrap(); child != nil {
				pending = append(pending, child)
				continue
			}
		}
		if timeout, ok := node.(interface{ Timeout() bool }); ok && timeout.Timeout() {
			observed = true
		}
	}
	return
}

func closeRetirementResult(value any) string {
	result, _ := value.(string)
	switch result {
	case "owner_admission_rejected", "expired", "abort_or_error", "bilateral", "session_closed", "awaiting_peer_fin", "aborted":
		return result
	default:
		return "unknown"
	}
}

type applicationReadRetirementObservation struct {
	observed, bytesValid, joined bool
	bytes                        int64
}

func observeApplicationReadRetirement(conn any) applicationReadRetirementObservation {
	reporter, ok := conn.(interface{ ApplicationReadRetirementStats() (int64, bool, string) })
	if !ok {
		return applicationReadRetirementObservation{}
	}
	// The third value is an arbitrary original error string. Never log it.
	count, joined, _ := reporter.ApplicationReadRetirementStats()
	result := applicationReadRetirementObservation{observed: true, bytesValid: count >= 0, joined: joined}
	if result.bytesValid {
		result.bytes = count
	}
	return result
}
