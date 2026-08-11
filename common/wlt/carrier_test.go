//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

func TestCarrierConnectAttemptBudgetCoversMobileUnderlay(t *testing.T) {
	if carrierConnectAttemptMax < 20*time.Second {
		t.Fatalf("carrier attempt timeout=%s, want at least 20s", carrierConnectAttemptMax)
	}
}

func TestCarrierRouteMapUsesRouteClasses(t *testing.T) {
	cfg := &carrierconfig.ClientConfig{Routes: []carrierconfig.ClientRoute{
		{RouteID: "vless-reality-main", Socket: "tcp", Transport: "srtp"},
		{RouteID: "vless-reality-eu", Socket: "tcp", Transport: "srtp"},
		{RouteID: "vless-reality-ru", Socket: "tcp", Transport: "srtp"},
	}}

	routeByClass := carrierRouteMap(cfg)
	want := map[string]string{
		"vless-reality-main": "vless-reality-main",
		"vless-reality-eu":   "vless-reality-eu",
		"vless-reality-ru":   "vless-reality-ru",
		"direct":             "vless-reality-main",
		"eu":                 "vless-reality-eu",
		"ru":                 "vless-reality-ru",
	}
	if !reflect.DeepEqual(routeByClass, want) {
		t.Fatalf("route map=%v, want %v", routeByClass, want)
	}
}

func TestFatalCarrierConnectErrorIncludesManualCaptchaUnavailable(t *testing.T) {
	err := errors.Join(errors.New("connect failed"), carriercommon.ErrManualCaptchaUnavailable)
	if !isFatalCarrierConnectError(err) {
		t.Fatal("manual captcha unavailable error should be fatal for startup connect")
	}
}

func TestLoadCarrierAuthSnapshotImportsSnapshot(t *testing.T) {
	expiresAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	snapshot := fmt.Sprintf(`{
		"version":1,
		"platform_id":"vk.com",
		"call_id":"snapshot-test",
		"username":"tester",
		"expires_at":%q,
		"vk":{
			"messages_access_token":"messages",
			"anonym_token":"anonymous",
			"session_key":"session",
			"device_id":"device",
			"endpoint":"wss://example.invalid/ws",
			"turn_user":"turn-user",
			"turn_pass":"turn-pass",
			"turn_addr":"turn:one.example.invalid",
			"turn_addrs":["turn:one.example.invalid"]
		}
	}`, expiresAt)
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("snapshot-test"), CarrierOptions{AuthSnapshot: snapshot}, nil); err != nil {
		t.Fatal(err)
	}
	exported, err := carrierengine.ExportAuthSnapshotJSON(carrierconfig.ClientConfig{
		PlatformID: "vk.com",
		CallID:     "snapshot-test",
		Username:   "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exported), `"anonym_token":"anonymous"`) {
		t.Fatalf("exported snapshot does not contain imported token: %s", exported)
	}
}

func TestLoadCarrierAuthSnapshotDoesNotBlockInlineSnapshotOnRemoteRefresh(t *testing.T) {
	var logs []string
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshot:             testCarrierAuthSnapshot("inline-token"),
		AuthSnapshotURL:          "https://127.0.0.1:1/unreachable",
		AuthSnapshotFetchTimeout: time.Second,
	}, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(logs, "\n")
	if strings.Contains(joined, "remote refresh") {
		t.Fatalf("remote refresh ran before inline snapshot:\n%s", joined)
	}
	if !strings.Contains(joined, "source=inline") {
		t.Fatalf("inline snapshot was not loaded:\n%s", joined)
	}
}

func TestLoadCarrierAuthSnapshotIgnoresMissingFile(t *testing.T) {
	missingPath := t.TempDir() + "/missing-auth-snapshot.json"
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{AuthSnapshotFile: missingPath}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestLoadCarrierAuthSnapshotIgnoresCorruptFile(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{AuthSnapshotFile: path}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestLoadCarrierAuthSnapshotRejectsCorruptInlineSnapshot(t *testing.T) {
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{AuthSnapshot: "{not-json"}, nil); err == nil {
		t.Fatal("expected corrupt inline snapshot to fail")
	}
}

func TestLoadCarrierAuthSnapshotPrefersPersistedFileOnRestart(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshot("persisted-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshot:           testCarrierAuthSnapshot("inline-token"),
		AuthSnapshotFile:       path,
		AuthSnapshotURL:        "https://127.0.0.1:1/unreachable",
		AuthSnapshotPreferFile: true,
		AuthSnapshotSkipRemote: true,
	}, nil); err != nil {
		t.Fatal(err)
	}
	exported, err := carrierengine.ExportAuthSnapshotJSON(carrierconfig.ClientConfig{
		PlatformID: "vk.com",
		CallID:     "restart-snapshot-test",
		Username:   "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exported), `"anonym_token":"persisted-token"`) || strings.Contains(string(exported), "inline-token") {
		t.Fatalf("persisted snapshot was not preferred: %s", exported)
	}
}

func TestLoadCarrierAuthSnapshotFallsBackFromCorruptPreferredFile(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshot:           testCarrierAuthSnapshot("inline-fallback-token"),
		AuthSnapshotFile:       path,
		AuthSnapshotPreferFile: true,
		AuthSnapshotSkipRemote: true,
	}, nil); err != nil {
		t.Fatal(err)
	}
	exported, err := carrierengine.ExportAuthSnapshotJSON(carrierconfig.ClientConfig{
		PlatformID: "vk.com",
		CallID:     "restart-snapshot-test",
		Username:   "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exported), `"anonym_token":"inline-fallback-token"`) {
		t.Fatalf("inline fallback snapshot was not imported: %s", exported)
	}
}

func testCarrierAuthSnapshot(token string) string {
	return testCarrierAuthSnapshotAt(token, time.Now().Add(time.Hour))
}

func testCarrierAuthSnapshotAt(token string, expiresAt time.Time) string {
	return fmt.Sprintf(`{
		"version":1,
		"platform_id":"vk.com",
		"call_id":"restart-snapshot-test",
		"username":"tester",
		"expires_at":%q,
		"vk":{
			"messages_access_token":"messages",
			"anonym_token":%q,
			"session_key":"session",
			"device_id":"device",
			"endpoint":"wss://example.invalid/ws",
			"turn_user":"turn-user",
			"turn_pass":"turn-pass",
			"turn_addr":"turn:one.example.invalid",
			"turn_addrs":["turn:one.example.invalid"]
		}
	}`, expiresAt.UTC().Format(time.RFC3339Nano), token)
}

func testCarrierClientConfig(callID string) *carrierconfig.ClientConfig {
	return &carrierconfig.ClientConfig{PlatformID: "vk.com", CallID: callID, Username: "tester"}
}

func TestLoadCarrierAuthSnapshotRefreshesExpiredPersistedIdentity(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshotAt("expired-token", time.Now().Add(-time.Hour))), 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshCarrierAuthSnapshot
	refreshCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		return []byte(testCarrierAuthSnapshotAt("refreshed-token", time.Now().Add(30*time.Minute))), nil
	}
	t.Cleanup(func() { refreshCarrierAuthSnapshot = previousRefresh })
	var logs []string
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		AuthSnapshotPreferFile: true,
		AuthSnapshotSkipRemote: true,
	}, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	}); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "refreshed-token") || !strings.Contains(strings.Join(logs, "\n"), "phase=auth_snapshot_refreshed") {
		t.Fatalf("refreshed snapshot was not persisted; logs=%v", logs)
	}
}

func TestLoadCarrierAuthSnapshotReusesSavedIdentityWhenRefreshFails(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshotAt("expired-token", time.Now().Add(-time.Hour))), 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshCarrierAuthSnapshot
	refreshCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		return nil, errors.New("provider unavailable")
	}
	t.Cleanup(func() { refreshCarrierAuthSnapshot = previousRefresh })
	var logs []string
	err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotPreferFile: true,
		AuthSnapshotSkipRemote: true,
	}, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	})
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	joinedLogs := strings.Join(logs, "\n")
	if !strings.Contains(string(content), "expired-token") || !strings.Contains(joinedLogs, "phase=auth_snapshot_refresh_unavailable") || !strings.Contains(joinedLogs, "fallback=saved_snapshot") {
		t.Fatalf("saved snapshot fallback was not preserved; logs=%v", logs)
	}
}

func TestFetchCarrierAuthSnapshotUsesHTTPSAndBoundsPayload(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Fatalf("method=%s, want GET", request.Method)
		}
		_, _ = writer.Write([]byte(`{"version":1}`))
	}))
	defer server.Close()

	data, err := fetchCarrierAuthSnapshotWithClient(context.Background(), server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"version":1}` {
		t.Fatalf("snapshot=%q", data)
	}
	if _, err := fetchCarrierAuthSnapshot(context.Background(), "http://example.com/snapshot", time.Second); err == nil {
		t.Fatal("expected non-HTTPS URL rejection")
	}
}

func TestCarrierDefaultsAreIPhoneBounded(t *testing.T) {
	options := withCarrierDefaults(CarrierOptions{})
	if options.MaxActiveStreams != defaultCarrierMaxActive {
		t.Fatalf("max active=%d, want %d", options.MaxActiveStreams, defaultCarrierMaxActive)
	}
	if options.MaxOpenAttempts != defaultCarrierMaxOpen {
		t.Fatalf("max open=%d, want %d", options.MaxOpenAttempts, defaultCarrierMaxOpen)
	}
	if options.MaxPendingDials != defaultCarrierMaxPending {
		t.Fatalf("max pending=%d, want %d", options.MaxPendingDials, defaultCarrierMaxPending)
	}
	if options.DialQueueTimeout != defaultCarrierQueueTimeout {
		t.Fatalf("queue timeout=%s, want %s", options.DialQueueTimeout, defaultCarrierQueueTimeout)
	}
	if options.IdleTimeout != defaultCarrierIdleTimeout {
		t.Fatalf("idle timeout=%s, want %s", options.IdleTimeout, defaultCarrierIdleTimeout)
	}
	if options.BufferSize != 32*1024 {
		t.Fatalf("buffer size=%d, want 32768", options.BufferSize)
	}
}

func TestCarrierRuntimeOptionsAreBounded(t *testing.T) {
	previousOptions := carrierconfig.Options
	defer func() {
		carrierconfig.Options = previousOptions
	}()

	carrierconfig.Options.Transport.RelayBandwidthBytesPerSecond = 1234
	options := withCarrierDefaults(CarrierOptions{})
	transportOptions := applyCarrierRuntimeOptions(options)
	if !reflect.DeepEqual(transportOptions, carrierconfig.TransportOptions{
		TinyMuxFlowBuffer:            defaultCarrierTinyMuxFlowBuffer,
		TinyMuxFlowSendBuffer:        defaultCarrierTinyMuxFlowSendBuffer,
		TinyMuxControlBuffer:         defaultCarrierTinyMuxControlBuffer,
		TinyMuxPingTimeoutMillis:     int(defaultCarrierTinyMuxPingTimeout / time.Millisecond),
		PeerIncomingBuffer:           defaultCarrierPeerIncomingBuffer,
		PeerWriteBuffer:              defaultCarrierPeerWriteBuffer,
		SRTPPacketBuffer:             defaultCarrierSRTPPacketBuffer,
		KCPWindowSize:                defaultCarrierKCPWindowSize,
		KCPReadWriteBuffer:           defaultCarrierKCPReadWriteBuffer,
		RelayBandwidthBytesPerSecond: 1234,
	}) {
		t.Fatalf("transport options=%+v", transportOptions)
	}

	transportOptions = applyCarrierRuntimeOptions(CarrierOptions{
		BufferSize:                   64 * 1024,
		TinyMuxRateBurstBytes:        512 * 1024,
		TinyMuxPingTimeout:           25 * time.Second,
		RelayBandwidthBytesPerSecond: 5 * 1024 * 1024,
	})
	if transportOptions.TinyMuxFlowBuffer != 256 ||
		transportOptions.TinyMuxFlowSendBuffer != 64 ||
		transportOptions.TinyMuxRateBurstBytes != 512*1024 ||
		transportOptions.TinyMuxPingTimeoutMillis != 25000 ||
		transportOptions.PeerIncomingBuffer != 256 ||
		transportOptions.SRTPPacketBuffer != 512 ||
		transportOptions.KCPWindowSize != 768 ||
		transportOptions.KCPReadWriteBuffer != 1024*1024 ||
		transportOptions.RelayBandwidthBytesPerSecond != 5*1024*1024 {
		t.Fatalf("scaled transport options=%+v", transportOptions)
	}
}

func TestCarrierDialStreamUsesRouteClassAndStats(t *testing.T) {
	carrier := newTestCarrier(2, 2, 1, time.Second, time.Second)
	var (
		mu     sync.Mutex
		dialed []string
	)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		mu.Lock()
		dialed = append(dialed, routeID)
		mu.Unlock()
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	conn, err := carrier.DialStream(context.Background(), "eu", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	stats := carrier.Stats()
	if stats.OpenAttempts != 1 || stats.OpenedStreams != 1 || stats.ActiveStreams != 1 || stats.PeakActiveStreams != 1 || stats.PendingDials != 0 {
		t.Fatalf("stats after dial=%+v", stats)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	stats = carrier.Stats()
	if stats.ActiveStreams != 0 || stats.PeakActiveStreams != 1 || stats.ClosedStreams != 1 {
		t.Fatalf("stats after close=%+v", stats)
	}
	if want := []string{"route-eu"}; !reflect.DeepEqual(dialed, want) {
		t.Fatalf("dialed routes=%v, want %v", dialed, want)
	}
}

func TestCarrierQueuesOverActiveLimit(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1 && stats.PeakPendingDials == 1 && stats.QueuedDials == 1
	})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	defer second.conn.Close()
	stats := carrier.Stats()
	if stats.RejectedStreams != 0 || stats.ActiveStreams != 1 || stats.PeakActiveStreams != 1 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want queued dial without rejection", stats)
	}
}

func TestCarrierRejectsActiveQueueTimeout(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, 20*time.Millisecond)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := carrier.DialStream(context.Background(), "eu", "two.example:443"); err == nil {
		t.Fatal("expected active queue timeout error")
	}
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedActive != 1 || stats.ActiveStreams != 1 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want one active timeout rejection", stats)
	}
}

func TestCarrierReclaimsPressureIdleStream(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, 20*time.Millisecond)
	carrier.pressureIdleTimeout = 10 * time.Millisecond
	var rights []net.Conn
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		_ = routeID
		left, right := net.Pipe()
		rights = append(rights, right)
		return left, nil
	}
	defer func() {
		for _, right := range rights {
			_ = right.Close()
		}
	}()

	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	second, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
	if err != nil {
		t.Fatalf("pressure reclaim should admit second stream: %v", err)
	}
	defer second.Close()
	_ = first.Close()
	stats := carrier.Stats()
	if stats.PressureIdleReclaims != 1 || stats.RejectedStreams != 0 || stats.ActiveStreams != 1 {
		t.Fatalf("stats=%+v, want one pressure reclaim and no rejection", stats)
	}
}

func TestCarrierDoesNotReclaimRecentStream(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, 20*time.Millisecond)
	carrier.pressureIdleTimeout = time.Second
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		_ = routeID
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}
	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := carrier.DialStream(context.Background(), "eu", "two.example:443"); err == nil {
		t.Fatal("expected recent stream to remain protected")
	}
	stats := carrier.Stats()
	if stats.PressureIdleReclaims != 0 || stats.RejectedActive != 1 {
		t.Fatalf("stats=%+v, want no reclaim and one active rejection", stats)
	}
}

func TestCarrierRejectsOverPendingLimit(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1
	})
	if _, err := carrier.DialStream(context.Background(), "eu", "three.example:443"); err == nil {
		t.Fatal("expected pending limit error")
	}
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedQueue != 1 || stats.PendingDials != 1 {
		t.Fatalf("stats=%+v, want one pending rejection", stats)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	_ = second.conn.Close()
}

func TestCarrierQueuesOverOpenAttemptLimit(t *testing.T) {
	carrier := newTestCarrier(2, 1, 1, time.Second, time.Second)
	started := make(chan struct{})
	unblock := make(chan struct{})
	var (
		mu    sync.Mutex
		calls int
	)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()
		if call == 1 {
			close(started)
			<-unblock
		}
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	firstDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
		firstDone <- ioResult{conn: conn, err: err}
	}()
	<-started
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1 && stats.PeakPendingDials == 1 && stats.QueuedDials == 1
	})
	close(unblock)
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	defer first.conn.Close()
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	defer second.conn.Close()
	stats := carrier.Stats()
	if stats.RejectedStreams != 0 || stats.RejectedOpen != 0 || stats.OpenAttempts != 2 || stats.OpenedStreams != 2 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want queued open attempts without rejection", stats)
	}
}

func TestCarrierRejectsOverOpenAttemptLimit(t *testing.T) {
	carrier := newTestCarrier(2, 1, 1, time.Second, 20*time.Millisecond)
	started := make(chan struct{})
	unblock := make(chan struct{})
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		select {
		case <-started:
		default:
			close(started)
		}
		<-unblock
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	firstDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
		firstDone <- ioResult{conn: conn, err: err}
	}()
	<-started
	if _, err := carrier.DialStream(context.Background(), "eu", "two.example:443"); err == nil {
		t.Fatal("expected open attempt limit error")
	}
	close(unblock)
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	_ = first.conn.Close()
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedOpen != 1 || stats.OpenAttempts != 1 || stats.QueuedDials != 1 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want one queued open-attempt timeout and one attempted open", stats)
	}
}

func TestCarrierRejectsOverPendingLimitWhileOpenFull(t *testing.T) {
	carrier := newTestCarrier(3, 1, 1, time.Second, time.Second)
	started := make(chan struct{})
	unblock := make(chan struct{})
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		select {
		case <-started:
		default:
			close(started)
		}
		<-unblock
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	firstDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
		firstDone <- ioResult{conn: conn, err: err}
	}()
	<-started
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1 && stats.PeakPendingDials == 1
	})
	if _, err := carrier.DialStream(context.Background(), "eu", "three.example:443"); err == nil {
		t.Fatal("expected pending limit error while open attempt is full")
	}
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedQueue != 1 || stats.PendingDials != 1 {
		t.Fatalf("stats=%+v, want one pending rejection while open attempt is full", stats)
	}
	close(unblock)
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	_ = first.conn.Close()
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	_ = second.conn.Close()
}

func TestCarrierDNSReserveGetsNextOpenSlotUnderNormalPressure(t *testing.T) {
	carrier := newTestCarrier(4, 2, 4, time.Second, time.Second)
	carrier.openGate = newPrioritySlotGate(2, 1)

	releaseFirst, err := carrier.acquireOpenSlot(context.Background(), "eu", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond, err := carrier.acquireOpenSlot(context.Background(), "eu", "two.example:443")
	if err != nil {
		t.Fatal(err)
	}

	normalDone := make(chan error, 1)
	var releaseNormal func()
	go func() {
		var acquireErr error
		releaseNormal, acquireErr = carrier.acquireOpenSlot(context.Background(), "eu", "three.example:443")
		normalDone <- acquireErr
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1
	})

	dnsDone := make(chan error, 1)
	var releaseDNS func()
	go func() {
		var acquireErr error
		releaseDNS, acquireErr = carrier.acquireOpenSlot(context.Background(), "eu", "10.0.0.53:53")
		dnsDone <- acquireErr
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 2 && stats.DNSOpenQueued == 1
	})

	releaseFirst()
	select {
	case acquireErr := <-dnsDone:
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("DNS waiter did not receive the reserved slot")
	}
	select {
	case <-normalDone:
		t.Fatal("normal waiter acquired before DNS reclaimed its reserve")
	default:
	}

	releaseDNS()
	select {
	case acquireErr := <-normalDone:
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("normal waiter starved after DNS released its slot")
	}

	releaseNormal()
	releaseSecond()
	stats := carrier.Stats()
	if stats.DNSOpenRequests != 1 || stats.DNSOpenQueued != 1 || stats.DNSOpenRejected != 0 {
		t.Fatalf("DNS open stats=%+v, want one queued request without rejection", stats)
	}
}

func TestCarrierDNSReserveTracksQueueTimeout(t *testing.T) {
	carrier := newTestCarrier(3, 2, 2, time.Second, 20*time.Millisecond)
	carrier.openGate = newPrioritySlotGate(2, 1)

	releaseFirst, err := carrier.acquireOpenSlot(context.Background(), "eu", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond, err := carrier.acquireOpenSlot(context.Background(), "eu", "two.example:443")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = carrier.acquireOpenSlot(context.Background(), "eu", "10.0.0.53:853"); err == nil {
		t.Fatal("expected DNS open queue timeout")
	}
	releaseFirst()
	releaseSecond()

	stats := carrier.Stats()
	if stats.DNSOpenRequests != 1 || stats.DNSOpenQueued != 1 || stats.DNSOpenRejected != 1 || stats.RejectedOpen != 1 {
		t.Fatalf("DNS timeout stats=%+v, want one rejected queued request", stats)
	}
}

func TestCarrierDialStreamCancelsOpen(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, 30*time.Millisecond, time.Second)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = routeID
		<-ctx.Done()
		return nil, ctx.Err()
	}

	if _, err := carrier.DialStream(context.Background(), "eu", "slow.example:443"); err == nil {
		t.Fatal("expected open timeout")
	}
	stats := carrier.Stats()
	if stats.FailedStreams != 1 || stats.ActiveStreams != 0 || stats.OpenAttempts != 1 || stats.LastDialMillis <= 0 {
		t.Fatalf("stats=%+v, want failed canceled open with released active slot", stats)
	}
}

func TestCarrierDialStreamWaitsForReconnect(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	var calls int
	ready := make(chan struct{})
	carrier.waitReady = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ready:
			return nil
		}
	}
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		_ = routeID
		calls++
		if calls == 1 {
			return nil, errors.New("full reconnect is in progress")
		}
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ready)
	}()

	conn, err := carrier.DialStream(context.Background(), "eu", "reconnect.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stats := carrier.Stats()
	if calls != 2 || stats.OpenAttempts != 2 || stats.ReconnectRetries != 1 || stats.ReconnectWaitMillis <= 0 || stats.FailedStreams != 0 {
		t.Fatalf("calls=%d stats=%+v, want one reconnect retry and successful open", calls, stats)
	}
}

func TestCarrierDialStreamBacksOffWhenReconnectWaitReturnsEarly(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, 120*time.Millisecond, time.Second)
	var calls int
	carrier.waitReady = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		_ = routeID
		calls++
		return nil, errors.New("full reconnect is in progress")
	}

	startedAt := time.Now()
	if _, err := carrier.DialStream(context.Background(), "eu", "reconnect.example:443"); err == nil {
		t.Fatal("expected reconnect timeout")
	}
	elapsed := time.Since(startedAt)
	stats := carrier.Stats()
	if calls > 10 || stats.OpenAttempts > 10 {
		t.Fatalf("calls=%d stats=%+v, want bounded reconnect retry loop", calls, stats)
	}
	if stats.ReconnectRetries == 0 || stats.ReconnectWaitMillis == 0 || elapsed < 50*time.Millisecond {
		t.Fatalf("elapsed=%s stats=%+v, want reconnect backoff before timeout", elapsed, stats)
	}
	if stats.FailedStreams != 1 || stats.ActiveStreams != 0 {
		t.Fatalf("stats=%+v, want failed reconnect open with released active slot", stats)
	}
}

func TestCarrierIdleTimeoutClosesStaleTail(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	conn := &carrierConn{
		Conn:          left,
		carrier:       carrier,
		releaseActive: func() {},
		idleTimeout:   30 * time.Millisecond,
	}
	defer conn.Close()

	_, err := conn.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected idle read timeout")
	}
	netErr, ok := err.(net.Error)
	if !ok || !netErr.Timeout() {
		t.Fatalf("read error=%v, want timeout", err)
	}
}

func TestCarrierPressureReclaimSkipsStreamWithActiveRead(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()

	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.pressureIdleTimeout = time.Millisecond
	conn := &carrierConn{
		Conn:          left,
		carrier:       carrier,
		releaseActive: func() {},
		idleTimeout:   time.Minute,
		routeClass:    "eu",
	}
	carrier.registerStream(conn)
	defer conn.Close()

	readStarted := make(chan struct{})
	original := conn.Conn
	conn.Conn = &readStartConn{Conn: original, started: readStarted}
	readDone := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case <-readStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocking read")
	}
	// Make the stream eligible while Read still owns ioMu.RLock.
	conn.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	if carrier.reclaimPressureIdleStream("eu") {
		t.Fatal("pressure reclaim closed a stream with an active read")
	}

	if _, err := right.Write([]byte{'x'}); err != nil {
		t.Fatalf("release blocked read: %v", err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("blocked read failed: %v", err)
	}
	conn.lastActivity.Store(time.Now().Add(-time.Second).UnixNano())
	if !carrier.reclaimPressureIdleStream("eu") {
		t.Fatal("pressure reclaim did not close an idle stream after read completed")
	}
}

type readStartConn struct {
	net.Conn
	started chan<- struct{}
}

func (c *readStartConn) Read(p []byte) (int, error) {
	select {
	case c.started <- struct{}{}:
	default:
	}
	return c.Conn.Read(p)
}

type ioResult struct {
	conn io.ReadWriteCloser
	err  error
}

func newTestCarrier(maxActive int, maxOpen int, maxPending int, connectTimeout time.Duration, queueTimeout time.Duration) *Carrier {
	return &Carrier{
		connectTimeout:   connectTimeout,
		dialQueueTimeout: queueTimeout,
		idleTimeout:      defaultCarrierIdleTimeout,
		streams:          make(map[*carrierConn]struct{}),
		bufferSize:       defaultCarrierBufferSize,
		activeSlots:      make(chan struct{}, maxActive),
		openSlots:        make(chan struct{}, maxOpen),
		pendingSlots:     make(chan struct{}, maxPending),
		routeByClass: map[string]string{
			"direct": "route-direct",
			"eu":     "route-eu",
		},
	}
}

func waitForCarrierStat(t *testing.T, carrier *Carrier, predicate func(CarrierStats) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if predicate(carrier.Stats()) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for carrier stats, last=%+v", carrier.Stats())
}
