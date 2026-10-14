//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"io"
	"net"
	"strings"
)

// Never include err.Error(): provider errors can carry private values.
func closeErrorClasses(err error) string {
	if err == nil {
		return "none"
	}
	known := []struct {
		err  error
		name string
	}{
		{context.Canceled, "context_canceled"}, {context.DeadlineExceeded, "deadline"},
		{carriercommon.ErrDataDrainUnacknowledged, "data_unacknowledged"}, {io.ErrClosedPipe, "closed_pipe"}, {net.ErrClosed, "net_closed"}, {io.EOF, "eof"},
	}
	found := make([]bool, len(known))
	other := false
	pending := []error{err}
	// A malformed cycle or excessively deep join must never look canceled-only.
	budget := 128
	for len(pending) > 0 {
		if budget == 0 {
			other = true
			break
		}
		budget--
		node := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if node == nil {
			continue
		}
		switch wrapped := node.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			actual := false
			for _, child := range children {
				if child != nil {
					pending = append(pending, child)
					actual = true
				}
			}
			if !actual {
				other = true
			}
			continue
		case interface{ Unwrap() error }:
			if child := wrapped.Unwrap(); child != nil {
				pending = append(pending, child)
				continue
			}
		}
		matched := false
		for i, item := range known {
			if errors.Is(node, item.err) {
				found[i] = true
				matched = true
			}
		}
		if !matched {
			other = true
		}
	}
	classes := make([]string, 0, len(known)+1)
	for i, item := range known {
		if found[i] {
			classes = append(classes, item.name)
		}
	}
	if other {
		classes = append(classes, "other")
	}
	return strings.Join(classes, ",")
}

func (c *Carrier) logCloseDiagnostic(component string, err error) {
	if c != nil && c.logf != nil {
		c.logf("WLT close diagnostic component=%s error_classes=%s returned_error=%t", component, closeErrorClasses(err), err != nil)
	}
}

// Only bounded numeric and enum fields are copied. Never log arbitrary values.
func (c *Carrier) logStreamCloseDiagnostic(component string, err error, flowID uint16, conn net.Conn) {
	if c == nil || c.logf == nil {
		return
	}
	c.closeStateMu.Lock()
	abortRequested := channelClosed(c.abortRequested)
	c.closeStateMu.Unlock()
	observed := false
	var pending, calls, elapsed int64
	result := "unknown"
	numbers := map[string]int64{"kcp_lifetime_id": 0, "kcp_lifetime_process_id": 0, "kcp_lifetime_started_unix_ms": 0, "kcp_drain_snapshot_observed": 0, "kcp_drain_snapshot_call": 0, "kcp_drain_snapshot_unix_ms": 0, "kcp_drain_input_ended": 0, "kcp_drain_remote_data_close": 0, "kcp_drain_pending_mux": 0, "kcp_drain_pending_kcp": 0}
	protocol, inputError := "unknown", "unknown"
	if reporter, ok := conn.(interface{ DiagnosticFields() []any }); ok {
		observed = true
		fields := reporter.DiagnosticFields()
		for i := 0; i+1 < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if !ok {
				continue
			}
			if key == "kcp_drain_result" {
				v, _ := fields[i+1].(string)
				switch v {
				case "acked", "timeout", "aborted", "legacy_input_closed", "data_input_closed":
					result = v
				}
				continue
			}
			if key == "kcp_drain_protocol" {
				v, _ := fields[i+1].(string)
				switch v {
				case "legacy", "watermark", "retained_half", "control":
					protocol = v
				}
				continue
			}
			if key == "kcp_drain_input_error" {
				v, _ := fields[i+1].(string)
				switch v {
				case "open", "eof", "net_closed", "closed_pipe", "timeout", "context_canceled", "other":
					inputError = v
				}
				continue
			}
			var value int64
			switch v := fields[i+1].(type) {
			case int:
				value = int64(v)
			case int64:
				value = v
			default:
				continue
			}
			if value < 0 {
				continue
			}
			if _, ok := numbers[key]; ok {
				numbers[key] = value
			}
			switch key {
			case "kcp_wait_snd":
				pending = value
			case "kcp_drain_calls":
				calls = value
			case "kcp_drain_ms":
				elapsed = value
			}
		}
	}
	c.logf("WLT close diagnostic component=%s flow_id=%d carrier_abort_requested=%t error_classes=%s returned_error=%t transport_diagnostics=%t kcp_wait_snd=%d kcp_drain_calls=%d kcp_drain_ms=%d kcp_drain_result=%s kcp_lifetime_id=%d kcp_lifetime_process_id=%d kcp_lifetime_started_unix_ms=%d kcp_drain_snapshot_observed=%d kcp_drain_snapshot_call=%d kcp_drain_snapshot_unix_ms=%d kcp_drain_input_ended=%d kcp_drain_remote_data_close=%d kcp_drain_pending_mux=%d kcp_drain_pending_kcp=%d kcp_drain_protocol=%s kcp_drain_input_error=%s", component, flowID, abortRequested, closeErrorClasses(err), err != nil, observed, pending, calls, elapsed, result, numbers["kcp_lifetime_id"], numbers["kcp_lifetime_process_id"], numbers["kcp_lifetime_started_unix_ms"], numbers["kcp_drain_snapshot_observed"], numbers["kcp_drain_snapshot_call"], numbers["kcp_drain_snapshot_unix_ms"], numbers["kcp_drain_input_ended"], numbers["kcp_drain_remote_data_close"], numbers["kcp_drain_pending_mux"], numbers["kcp_drain_pending_kcp"], protocol, inputError)
}
