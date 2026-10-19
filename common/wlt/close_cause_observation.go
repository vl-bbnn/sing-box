//go:build with_wlt

package wlt

import "fmt"

type applicationClosePartObservation struct {
	classes           string
	timeout, complete bool
}

func observeApplicationClosePart(err error) applicationClosePartObservation {
	seen, complete := closeTimeoutObservation(err)
	return applicationClosePartObservation{closeErrorClasses(err), seen, complete}
}

func applicationCloseCauseSuffix(conn any, numbers map[string]int64) string {
	unobserved := applicationClosePartObservation{classes: "unobserved"}
	original, reader, retirement := unobserved, unobserved, unobserved
	observed := false
	if reporter, ok := conn.(interface {
		ApplicationCloseErrorParts() (bool, error, error, error)
	}); ok {
		if actual, a, b, d := reporter.ApplicationCloseErrorParts(); actual {
			observed = true
			original, reader, retirement = observeApplicationClosePart(a), observeApplicationClosePart(b), observeApplicationClosePart(d)
		}
	}
	flag := func(name string) int64 {
		if numbers[name] == 1 {
			return 1
		}
		return 0
	}
	return fmt.Sprintf(" application_close_parts_observed=%t application_original_close_error_classes=%s application_original_close_timeout=%t application_original_close_walk_complete=%t application_retirement_read_error_classes=%s application_retirement_read_timeout=%t application_retirement_read_walk_complete=%t application_retirement_join_error_classes=%s application_retirement_join_timeout=%t application_retirement_join_walk_complete=%t kcp_application_read_deadline_observed=%d kcp_application_read_start_deadline_observed=%d kcp_application_read_start_deadline_valid=%d kcp_application_read_start_deadline_set=%d kcp_application_read_start_deadline_unix_ns=%d kcp_application_read_start_sample_unix_ns=%d kcp_application_read_return_deadline_observed=%d kcp_application_read_return_deadline_valid=%d kcp_application_read_return_deadline_set=%d kcp_application_read_return_deadline_unix_ns=%d kcp_application_read_return_sample_unix_ns=%d",
		observed, original.classes, original.timeout, original.complete, reader.classes, reader.timeout, reader.complete, retirement.classes, retirement.timeout, retirement.complete,
		flag("kcp_application_read_deadline_observed"), flag("kcp_application_read_start_deadline_observed"), flag("kcp_application_read_start_deadline_valid"), flag("kcp_application_read_start_deadline_set"), numbers["kcp_application_read_start_deadline_unix_ns"], numbers["kcp_application_read_start_sample_unix_ns"], flag("kcp_application_read_return_deadline_observed"), flag("kcp_application_read_return_deadline_valid"), flag("kcp_application_read_return_deadline_set"), numbers["kcp_application_read_return_deadline_unix_ns"], numbers["kcp_application_read_return_sample_unix_ns"])
}

func addApplicationReadDeadlineKeys(numbers map[string]int64) {
	for _, key := range []string{
		"kcp_application_read_deadline_observed", "kcp_application_read_start_deadline_observed", "kcp_application_read_start_deadline_valid", "kcp_application_read_start_deadline_set", "kcp_application_read_start_deadline_unix_ns", "kcp_application_read_start_sample_unix_ns", "kcp_application_read_return_deadline_observed", "kcp_application_read_return_deadline_valid", "kcp_application_read_return_deadline_set", "kcp_application_read_return_deadline_unix_ns", "kcp_application_read_return_sample_unix_ns",
	} {
		numbers[key] = 0
	}
}
