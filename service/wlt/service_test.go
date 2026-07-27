//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/option"
)

func TestPersistentDNSCacheFileUsesWritableAuthDirectory(t *testing.T) {
	authPath := filepath.Join(t.TempDir(), "wlt-auth.json")
	got := persistentDNSCacheFile(option.WLTServiceOptions{
		AuthSnapshotOutputFile: authPath,
	})
	want := filepath.Join(filepath.Dir(authPath), "wlt-dns-cache.json")
	if got != want {
		t.Fatalf("persistentDNSCacheFile=%q, want %q", got, want)
	}
}

func TestPersistentDNSCacheFileFallsBackToInputAndRejectsRelativePath(t *testing.T) {
	authPath := filepath.Join(t.TempDir(), "wlt-auth.json")
	if got := persistentDNSCacheFile(option.WLTServiceOptions{AuthSnapshotFile: authPath}); got != filepath.Join(filepath.Dir(authPath), "wlt-dns-cache.json") {
		t.Fatalf("persistentDNSCacheFile fallback=%q", got)
	}
	if got := persistentDNSCacheFile(option.WLTServiceOptions{AuthSnapshotOutputFile: "relative.json"}); got != "" {
		t.Fatalf("persistentDNSCacheFile accepted relative path %q", got)
	}
}

func TestWaitCarrierWaitsForReplacement(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	service := &Service{
		ctx:          serviceContext,
		carrierReady: make(chan struct{}),
	}
	want := &wltpkg.Carrier{}
	result := make(chan *wltpkg.Carrier, 1)
	errorResult := make(chan error, 1)
	go func() {
		carrier, err := service.WaitCarrier(context.Background())
		result <- carrier
		errorResult <- err
	}()

	select {
	case <-result:
		t.Fatal("WaitCarrier returned before replacement was published")
	case <-time.After(20 * time.Millisecond):
	}

	service.access.Lock()
	service.carrier = want
	close(service.carrierReady)
	service.access.Unlock()

	select {
	case got := <-result:
		if got != want {
			t.Fatalf("WaitCarrier returned %p, want %p", got, want)
		}
		if err := <-errorResult; err != nil {
			t.Fatalf("WaitCarrier returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitCarrier did not observe replacement")
	}
}

func TestWaitCarrierHonorsDialContext(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	service := &Service{
		ctx:          serviceContext,
		carrierReady: make(chan struct{}),
	}
	dialContext, cancelDial := context.WithCancel(context.Background())
	cancelDial()
	if _, err := service.WaitCarrier(dialContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitCarrier error = %v, want context cancellation", err)
	}
}

func TestWaitCarrierRejectsStoppedService(t *testing.T) {
	service := &Service{
		ctx:          context.Background(),
		carrierReady: make(chan struct{}),
		stopped:      true,
	}
	if _, err := service.WaitCarrier(context.Background()); err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("WaitCarrier error = %v, want stopped service", err)
	}
}

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
