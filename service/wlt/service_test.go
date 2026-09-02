//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/option"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
)

func TestNetworkInterfaceIdentityDetectsHandoverAndAddressChange(t *testing.T) {
	lte := &adapter.NetworkInterface{}
	lte.Index = 10
	lte.Name = "cellular0"
	lte.Addresses = []netip.Prefix{netip.MustParsePrefix("192.0.2.2/32")}
	wifi := &adapter.NetworkInterface{}
	wifi.Index = 11
	wifi.Name = "wifi0"
	wifi.Addresses = []netip.Prefix{netip.MustParsePrefix("198.51.100.2/24")}

	lteKey := networkInterfaceKey(lte)
	if interfaceIdentityChanged(lteKey, lteKey) {
		t.Fatal("unchanged interface was classified as a handover")
	}
	if !interfaceIdentityChanged(lteKey, networkInterfaceKey(wifi)) {
		t.Fatal("LTE to Wi-Fi handover was not detected")
	}

	lte.Addresses = []netip.Prefix{netip.MustParsePrefix("192.0.2.3/32")}
	if !interfaceIdentityChanged(lteKey, networkInterfaceKey(lte)) {
		t.Fatal("same-interface address change was not detected")
	}
	if interfaceIdentityChanged("", networkInterfaceKey(lte)) {
		t.Fatal("missing initial identity must not force an unsolicited restart")
	}
}

func TestInterfaceRecoveryUsesClientReloadFallbackDelayOnAndroid(t *testing.T) {
	if got := interfaceRecoveryGraceFor("android"); got != wltAndroidInterfaceFallback {
		t.Fatalf("Android interface recovery delay=%s, want %s", got, wltAndroidInterfaceFallback)
	}
	for _, goos := range []string{"darwin", "ios", "linux"} {
		if got := interfaceRecoveryGraceFor(goos); got != wltInterfaceRecoveryGrace {
			t.Fatalf("%s interface recovery grace=%s, want %s", goos, got, wltInterfaceRecoveryGrace)
		}
	}
}

func TestDuplicateInterfaceUpdatesAreIgnoredOnlyOnAndroid(t *testing.T) {
	const identity = "10|cellular0|cellular|[192.0.2.2/32]"
	if !ignoreDuplicateInterfaceUpdate("android", identity, identity) {
		t.Fatal("duplicate Android callback was not ignored")
	}
	if ignoreDuplicateInterfaceUpdate("ios", identity, identity) {
		t.Fatal("duplicate Apple callback lost its recovery-grace behavior")
	}
	if ignoreDuplicateInterfaceUpdate("android", "", identity) {
		t.Fatal("initial Android callback was ignored")
	}
	if ignoreDuplicateInterfaceUpdate("android", identity, identity+"-new") {
		t.Fatal("real Android handover was ignored")
	}
}

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

func TestPersistentAuthSnapshotAvailableRequiresAbsoluteNonEmptyRegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wlt-auth.json")
	options := option.WLTServiceOptions{AuthSnapshotOutputFile: path}
	if persistentAuthSnapshotAvailable(options) {
		t.Fatal("missing snapshot reported as available")
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if persistentAuthSnapshotAvailable(options) {
		t.Fatal("empty snapshot reported as available")
	}
	if err := os.WriteFile(path, []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !persistentAuthSnapshotAvailable(options) {
		t.Fatal("non-empty snapshot was not detected")
	}
	if persistentAuthSnapshotAvailable(option.WLTServiceOptions{AuthSnapshotFile: "relative.json"}) {
		t.Fatal("relative snapshot path reported as available")
	}
}

func TestCarrierRestartRetryDelayBacksOffAndRespectsProviderCooldown(t *testing.T) {
	if got := carrierRestartRetryDelay(1, errors.New("network unavailable")); got != wltCarrierRestartRetryDelay {
		t.Fatalf("first retry delay=%s", got)
	}
	if got := carrierRestartRetryDelay(20, errors.New("network unavailable")); got != wltCarrierRestartRetryMax {
		t.Fatalf("capped retry delay=%s", got)
	}
	if got := carrierRestartRetryDelay(1, carriercommon.ErrHumanChallengeErrorLimit); got != wltCarrierRateLimitRetry {
		t.Fatalf("provider-limited retry delay=%s", got)
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

func TestWaitCarrierBlocksDuringInterfaceRecovery(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	service := &Service{
		ctx:            serviceContext,
		carrier:        &wltpkg.Carrier{},
		carrierReady:   closedSignal(),
		interfaceReady: make(chan struct{}),
	}
	result := make(chan *wltpkg.Carrier, 1)
	go func() {
		carrier, _ := service.WaitCarrier(context.Background())
		result <- carrier
	}()
	select {
	case <-result:
		t.Fatal("WaitCarrier returned while interface recovery gate was closed")
	case <-time.After(20 * time.Millisecond):
	}
	service.access.Lock()
	closeSignal(service.interfaceReady)
	service.access.Unlock()
	select {
	case got := <-result:
		if got != service.carrier {
			t.Fatal("WaitCarrier returned the wrong carrier after recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitCarrier did not resume after interface recovery")
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

func TestInterfaceRecoveryGenerationSupersedesOlderNotification(t *testing.T) {
	service := &Service{interfaceReady: closedSignal()}
	first := service.beginInterfaceRecovery()
	second := service.beginInterfaceRecovery()
	if first == second {
		t.Fatal("interface recovery generation did not advance")
	}
	if service.interfaceRecoveryCurrent(first) {
		t.Fatal("older interface notification remained current")
	}
	if !service.interfaceRecoveryCurrent(second) {
		t.Fatal("latest interface notification was not current")
	}
	service.finishInterfaceRecovery(first)
	select {
	case <-service.interfaceReady:
		t.Fatal("older interface notification opened the current gate")
	default:
	}
	service.finishInterfaceRecovery(second)
	select {
	case <-service.interfaceReady:
	default:
		t.Fatal("latest interface notification did not open the gate")
	}
}

func TestFormatWLTStatsHeartbeatAcceptsVectorValues(t *testing.T) {
	message := formatWLTStatsHeartbeat(
		"peer_in_bytes_by_index=", []uint64{1, 2},
		" peer_send_queue_by_index=", []int{3, 4},
	)
	if message != "peer_in_bytes_by_index=[1 2] peer_send_queue_by_index=[3 4]" {
		t.Fatalf("unexpected heartbeat: %q", message)
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
