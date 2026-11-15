//go:build with_wlt

package wlt

import (
	"strings"
	"testing"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
)

func TestDescribeWLTIncident(t *testing.T) {
	previous := wltpkg.CarrierStats{}
	current := wltpkg.CarrierStats{
		FailedStreams:   2,
		RejectedStreams: 1,
	}
	current.Runtime.FullReconnects = 1
	current.Runtime.LastReconnectReason = "tinymux session died"
	current.Runtime.Peer.OnlinePeers = 2
	current.Runtime.Peer.OutgoingQueueFull = 3

	incident := describeWLTIncident(current, previous)
	for _, expected := range []string{
		"reconnects_delta=1",
		"failed_delta=2",
		"rejected_delta=1",
		"peer_out_queue_full_delta=3",
		"last_reconnect=tinymux session died",
	} {
		if !strings.Contains(incident, expected) {
			t.Fatalf("incident %q does not contain %q", incident, expected)
		}
	}
}

func TestDescribeWLTIncidentIgnoresStableCounters(t *testing.T) {
	stats := wltpkg.CarrierStats{
		FailedStreams:   2,
		RejectedStreams: 1,
	}
	stats.Runtime.FullReconnects = 1
	stats.Runtime.Peer.OutgoingQueueFull = 3

	if incident := describeWLTIncident(stats, stats); incident != "" {
		t.Fatalf("expected no incident for stable counters, got %q", incident)
	}
}
