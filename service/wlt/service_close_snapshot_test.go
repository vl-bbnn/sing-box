//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/log"
)

func TestFormatWLTServiceCloseSnapshotKeepsCountersUnverified(t *testing.T) {
	stats := wltpkg.CarrierStats{ActiveStreams: 4, DrainingStreams: 2, PeakDrainingStreams: 3, DrainCapacity: 8, OpenedStreams: 53, ClosedStreams: 49,
		FailedStreams: 2, RejectedStreams: 3, RejectedOpen: 1}
	for _, test := range []struct{ phase, mode, result string }{
		{"pre_close", "graceful", "not_called"},
		{"post_close", "abort", "returned_error"},
	} {
		line := formatWLTServiceCloseSnapshot(test.phase, test.mode, test.result, 7, stats)
		for _, required := range []string{
			"schema=2", "phase=" + test.phase, "close_mode=" + test.mode,
			"close_result=" + test.result, "closure_status=unverified",
			"snapshot_consistency=independent_reads", "carrier_generation=7",
			"active=4", "draining=2", "peak_draining=3", "drain_capacity=8",
			"opened=53", "closed=49", "failed=2", "rejected=3", "rejected_open=1",
		} {
			if !strings.Contains(line, required) {
				t.Fatalf("missing %q in %q", required, line)
			}
		}
	}
	if line := formatWLTServiceCloseSnapshot("post_close", "graceful", "returned_nil", 7, wltpkg.CarrierStats{}); !strings.Contains(line, "closure_status=unverified") || strings.Contains(line, "terminal_complete=true") {
		t.Fatalf("zero counters were promoted to closure proof: %q", line)
	}
	t.Log("WLT_CLOSE_SNAPSHOT_SAMPLE=" + formatWLTServiceCloseSnapshot("post_close", "graceful", "returned_nil", 7, wltpkg.CarrierStats{}))
}

func TestServiceCloseSnapshotsBracketAbortAndReturnSameError(t *testing.T) {
	var output bytes.Buffer
	factory := log.NewDefaultFactory(context.Background(), log.Formatter{BaseTime: time.Now()}, &output, "", nil, false)
	defer factory.Close()
	sentinel := errors.New("close sentinel")
	reads := 0
	service := &Service{
		logger: factory.Logger(), carrier: &wltpkg.Carrier{}, carrierGeneration: 7,
		interfaceUnavailable: true, interfaceReady: make(chan struct{}), carrierReady: make(chan struct{}),
		closeSnapshotStatsHook: func(*wltpkg.Carrier) wltpkg.CarrierStats {
			reads++
			if reads == 1 {
				return wltpkg.CarrierStats{ActiveStreams: 4, OpenedStreams: 53, ClosedStreams: 49, FailedStreams: 2}
			}
			return wltpkg.CarrierStats{ActiveStreams: 0, OpenedStreams: 53, ClosedStreams: 53, FailedStreams: 2}
		},
	}
	service.abortCarrierHook = func(*wltpkg.Carrier) error {
		if !strings.Contains(output.String(), "phase=pre_close close_mode=abort close_result=not_called") {
			t.Fatal("pre-close snapshot did not precede abort")
		}
		if strings.Contains(output.String(), "phase=post_close") {
			t.Fatal("post-close snapshot preceded abort return")
		}
		return sentinel
	}
	if got := service.Close(); got != sentinel {
		t.Fatalf("returned error identity changed: %v", got)
	}
	if reads != 2 {
		t.Fatalf("expected independent reads, got %d", reads)
	}
	text := output.String()
	for _, snapshot := range []struct{ phase, counters string }{
		{"phase=pre_close close_mode=abort close_result=not_called", "active=4 peak_active=0 draining=0 peak_draining=0 drain_capacity=0 pending=0 peak_pending=0 opened=53 closed=49"},
		{"phase=post_close close_mode=abort close_result=returned_error", "active=0 peak_active=0 draining=0 peak_draining=0 drain_capacity=0 pending=0 peak_pending=0 opened=53 closed=53"},
	} {
		found := false
		for _, line := range strings.Split(text, "\n") {
			if strings.Contains(line, snapshot.phase) && strings.Contains(line, snapshot.counters) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("close snapshot missing phase %q and counters %q in %q", snapshot.phase, snapshot.counters, text)
		}
	}
	if strings.Count(text, "closure_status=unverified") != 2 {
		t.Fatalf("incorrect close snapshots: %q", text)
	}
	before := output.Len()
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if output.Len() != before || reads != 2 {
		t.Fatal("repeated close fabricated a snapshot")
	}
}

func TestServiceCloseWithoutCarrierHasNoSnapshot(t *testing.T) {
	var output bytes.Buffer
	factory := log.NewDefaultFactory(context.Background(), log.Formatter{BaseTime: time.Now()}, &output, "", nil, false)
	defer factory.Close()
	service := &Service{logger: factory.Logger(), interfaceReady: make(chan struct{}), carrierReady: make(chan struct{})}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "close_snapshot") {
		t.Fatal("fabricated carrier snapshot")
	}
}
