//go:build with_wlt

package wlt

import "fmt"

var priorReadDiagnosticNames = []string{
	"kcp_prior_read_observed",
	"kcp_prior_read_start_sample_unix_ns", "kcp_prior_read_start_deadline_unix_ns", "kcp_prior_read_start_deadline_observed", "kcp_prior_read_start_deadline_set", "kcp_prior_read_start_setter_certain", "kcp_prior_read_start_setter_origin", "kcp_prior_read_start_setter_sequence", "kcp_prior_read_start_setters_started", "kcp_prior_read_start_setters_active", "kcp_prior_read_start_setter_completed_unix_ns",
	"kcp_prior_read_return_sample_unix_ns", "kcp_prior_read_return_deadline_unix_ns", "kcp_prior_read_return_deadline_observed", "kcp_prior_read_return_deadline_set", "kcp_prior_read_return_setter_certain", "kcp_prior_read_return_setter_origin", "kcp_prior_read_return_setter_sequence", "kcp_prior_read_return_setters_started", "kcp_prior_read_return_setters_active", "kcp_prior_read_return_setter_completed_unix_ns",
}

func applicationPriorReadCauseSuffix(conn any, numbers map[string]int64) string {
	observed := false
	part := applicationClosePartObservation{classes: "unobserved"}
	if reporter, ok := conn.(interface{ ApplicationPriorReadError() (bool, error) }); ok {
		if actual, err := reporter.ApplicationPriorReadError(); actual {
			observed = true
			part = observeApplicationClosePart(err)
		}
	}
	result := fmt.Sprintf(" application_prior_read_observed=%t application_prior_read_error_classes=%s application_prior_read_timeout=%t application_prior_read_walk_complete=%t", observed, part.classes, part.timeout, part.complete)
	for _, name := range priorReadDiagnosticNames {
		value := numbers[name]
		// Inputs are bounded diagnostics, never private text. Invalid enum, boolean,
		// timestamp or counter values remain unavailable, not invented provenance.
		if value < 0 {
			value = 0
		}
		switch name {
		case "kcp_prior_read_observed", "kcp_prior_read_start_deadline_observed", "kcp_prior_read_start_deadline_set", "kcp_prior_read_start_setter_certain", "kcp_prior_read_return_deadline_observed", "kcp_prior_read_return_deadline_set", "kcp_prior_read_return_setter_certain":
			if value != 1 {
				value = 0
			}
		case "kcp_prior_read_start_setter_origin", "kcp_prior_read_return_setter_origin":
			if value > 3 {
				value = 0
			}
		}
		result += fmt.Sprintf(" %s=%d", name, value)
	}
	return result
}
