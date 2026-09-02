//go:build with_wlt

package wlt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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

func TestCarrierAuthRecoveryIncludesLegacyManualChallengeOutcome(t *testing.T) {
	for _, err := range []error{
		carriercommon.ErrAuthSnapshotReauthorizationRequired,
		carriercommon.ErrManualCaptchaUnavailable,
		fmt.Errorf("wrapped: %w", carriercommon.ErrManualCaptchaUnavailable),
	} {
		if !carrierAuthRecoveryRequired(err) {
			t.Fatalf("auth recovery rejected %v", err)
		}
	}
	if carrierAuthRecoveryRequired(errors.New("network unavailable")) {
		t.Fatal("ordinary network error entered auth recovery")
	}
}

func TestCarrierStartupTelemetryUsesSanitizedOneShotMilestones(t *testing.T) {
	var logs []string
	logf := func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	}
	startup := newCarrierStartupTelemetry(time.Now().Add(-time.Second), logf)
	logger := newLogfSlogLoggerWithObserver(logf, startup.observe)

	startup.markSnapshotChecked()
	startup.markProviderRefreshStarted()
	startup.markProviderRefreshReady("cached")
	logger.Info("relay client session phase=platform_authorize_done", "token", "private-sentinel")
	logger.Info("relay client session phase=signaling_turn_refresh_unavailable", "address", "private-sentinel")
	logger.Info("relay client session connected", "session_uuid", "private-sentinel")
	startup.markCarrierReady()
	startup.markSingBoxReady()
	startup.markFirstPacket("write")
	startup.markFirstPacket("read")
	startup.markTrafficReady()
	startup.markTrafficReady()

	var startupLogs []string
	for _, line := range logs {
		if strings.HasPrefix(line, "WLT startup ") {
			startupLogs = append(startupLogs, line)
		}
	}
	joined := strings.Join(startupLogs, "\n")
	for _, phase := range []string{
		"snapshot_checked",
		"provider_refresh_started",
		"provider_refresh_ready",
		"provider_ready",
		"turn_ready",
		"peer_ready",
		"carrier_ready",
		"sing_box_ready",
		"first_packet",
		"traffic_ready",
	} {
		if strings.Count(joined, "phase="+phase) != 1 {
			t.Fatalf("startup phase %s is missing or duplicated:\n%s", phase, joined)
		}
	}
	if strings.Contains(joined, "private-sentinel") {
		t.Fatalf("startup telemetry copied a private carrier attribute:\n%s", joined)
	}
	if !strings.Contains(joined, "phase=first_packet") || !strings.Contains(joined, "direction=write") {
		t.Fatalf("first packet milestone did not preserve its first direction:\n%s", joined)
	}
}

func TestCarrierConnMarksFirstPacketAfterSuccessfulIO(t *testing.T) {
	var logs []string
	startup := newCarrierStartupTelemetry(time.Now(), func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	})
	carrier := &Carrier{startup: startup}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	stream := &carrierConn{Conn: left, carrier: carrier, releaseActive: func() {}}

	writeDone := make(chan error, 1)
	go func() {
		_, err := right.Write([]byte("private-payload"))
		writeDone <- err
	}()
	buffer := make([]byte, 32)
	if _, err := stream.Read(buffer); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}

	joined := strings.Join(logs, "\n")
	if strings.Count(joined, "phase=first_packet") != 1 || !strings.Contains(joined, "direction=read") {
		t.Fatalf("first successful IO was not recorded once:\n%s", joined)
	}
	if strings.Contains(joined, "private-payload") {
		t.Fatalf("first packet telemetry contains payload data:\n%s", joined)
	}
	if strings.Count(joined, "phase=traffic_ready") != 1 {
		t.Fatalf("first successful read did not mark traffic readiness once:\n%s", joined)
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

func TestFatalCarrierConnectErrorIncludesRejectedAuthSnapshot(t *testing.T) {
	err := errors.Join(errors.New("connect failed"), carriercommon.ErrAuthSnapshotReauthorizationRequired)
	if !isFatalCarrierConnectError(err) {
		t.Fatal("rejected TURN auth snapshot should stop blind connect retries")
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

func TestLoadCarrierAuthSnapshotNeverFetchesURLBeforeTrafficReady(t *testing.T) {
	remoteCalled := false
	previousFetch := fetchCarrierAuthSnapshotForRecovery
	fetchCarrierAuthSnapshotForRecovery = func(context.Context, string, time.Duration) ([]byte, error) {
		remoteCalled = true
		return nil, errors.New("must not run")
	}
	t.Cleanup(func() { fetchCarrierAuthSnapshotForRecovery = previousFetch })
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("no-pre-tunnel-fetch-test"), CarrierOptions{
		AuthSnapshotURL: "https://control.example.com/auth-snapshot",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if remoteCalled {
		t.Fatal("pre-tunnel snapshot load contacted auth_snapshot_url")
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

func TestLoadCarrierAuthSnapshotFallsBackToPreviousWhenCurrentIsCorrupt(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotPreviousSuffix, []byte(testCarrierAuthSnapshot("previous-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs []string
	if err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotPreferFile: true,
		AuthSnapshotSkipRemote: true,
	}, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	}); err != nil {
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
	if !strings.Contains(string(exported), `"anonym_token":"previous-token"`) {
		t.Fatalf("previous snapshot was not imported: %s", exported)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "event=snapshot_loaded source=previous") {
		t.Fatalf("previous source was not reported: %v", logs)
	}
}

func TestWriteCarrierAuthSnapshotRotatesCurrentToProtectedPrevious(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	options := CarrierOptions{AuthSnapshotOutputFile: path}
	current := []byte(testCarrierAuthSnapshot("current-token"))
	next := []byte(testCarrierAuthSnapshot("next-token"))
	if err := writeCarrierAuthSnapshot(options, current, false); err != nil {
		t.Fatal(err)
	}
	if err := writeCarrierAuthSnapshot(options, next, true); err != nil {
		t.Fatal(err)
	}
	currentContent, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previousContent, err := os.ReadFile(path + authSnapshotPreviousSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(currentContent), "next-token") || !strings.Contains(string(previousContent), "current-token") {
		t.Fatal("current/previous snapshot rotation lost the last-known-good state")
	}
	for _, protectedPath := range []string{path, path + authSnapshotPreviousSuffix} {
		info, err := os.Stat(protectedPath)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("mode for %s=%#o, want 0600", protectedPath, got)
		}
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

func TestLoadCarrierAuthSnapshotDefersRefreshedIdentityPersistence(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshotAt("expired-token", time.Now().Add(-time.Hour))), 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshExistingCarrierAuthSnapshot
	refreshExistingCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		return []byte(testCarrierAuthSnapshotAt("refreshed-token", time.Now().Add(30*time.Minute))), nil
	}
	t.Cleanup(func() { refreshExistingCarrierAuthSnapshot = previousRefresh })
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
	joinedLogs := strings.Join(logs, "\n")
	if !strings.Contains(string(content), "expired-token") || strings.Contains(string(content), "refreshed-token") {
		t.Fatalf("unproven refreshed snapshot replaced last-known-good file; logs=%v", logs)
	}
	if !strings.Contains(joinedLogs, "event=refresh_succeeded") || !strings.Contains(joinedLogs, "persistence=deferred") {
		t.Fatalf("deferred refreshed snapshot was not reported; logs=%v", logs)
	}
}

func TestLoadCarrierAuthSnapshotReusesSavedIdentityWhenRefreshFails(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshotAt("expired-token", time.Now().Add(-time.Hour))), 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshExistingCarrierAuthSnapshot
	refreshExistingCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		return nil, errors.New("provider unavailable")
	}
	t.Cleanup(func() { refreshExistingCarrierAuthSnapshot = previousRefresh })
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
	if !strings.Contains(string(content), "expired-token") || !strings.Contains(joinedLogs, "event=refresh_unavailable") || !strings.Contains(joinedLogs, "fallback=saved_snapshot") {
		t.Fatalf("saved snapshot fallback was not preserved; logs=%v", logs)
	}
	if strings.Contains(joinedLogs, "expired-token") || strings.Contains(joinedLogs, "messages") || strings.Contains(joinedLogs, "turn-pass") {
		t.Fatalf("auth telemetry leaked snapshot material: %s", joinedLogs)
	}
}

func TestRefreshCarrierAuthSnapshotAfterRejectionDefersReplacementPersistence(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshot("stale-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshCarrierAuthSnapshot
	refreshCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		return []byte(testCarrierAuthSnapshot("fresh-token")), nil
	}
	t.Cleanup(func() { refreshCarrierAuthSnapshot = previousRefresh })
	var logs []string
	err := refreshCarrierAuthSnapshotAfterRejection(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
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
	if !strings.Contains(string(content), "stale-token") || strings.Contains(string(content), "fresh-token") {
		t.Fatalf("unproven TURN auth replacement replaced last-known-good file; logs=%v", logs)
	}
	if !strings.Contains(joinedLogs, "phase=turn_auth_candidate_ready") || !strings.Contains(joinedLogs, "persistence=deferred") {
		t.Fatalf("deferred TURN auth candidate was not reported; logs=%v", logs)
	}
}

func TestRecoverCarrierAuthPromotesFreshIdentityOnlyAfterSuccessfulConnect(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	current := testCarrierAuthSnapshot("current-token")
	previous := testCarrierAuthSnapshot("previous-token")
	inline := testCarrierAuthSnapshot("inline-token")
	if err := os.WriteFile(path, []byte(current), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotPreviousSuffix, []byte(previous), 0o600); err != nil {
		t.Fatal(err)
	}

	previousRefresh := refreshCarrierAuthSnapshot
	previousPrewarm := prewarmCarrierAuthSnapshot
	previousConnect := connectCarrierClientForStart
	refreshCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		return []byte(testCarrierAuthSnapshot("refreshed-token")), nil
	}
	prewarmCalls := 0
	prewarmCarrierAuthSnapshot = func(_ carrierconfig.ClientConfig) ([]byte, error) {
		prewarmCalls++
		return []byte(testCarrierAuthSnapshot("fresh-token")), nil
	}
	connectCalls := 0
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		connectCalls++
		if connectCalls < 4 {
			return nil, carriercommon.ErrAuthSnapshotReauthorizationRequired
		}
		return &carrierengine.Client{}, nil
	}
	t.Cleanup(func() {
		refreshCarrierAuthSnapshot = previousRefresh
		prewarmCarrierAuthSnapshot = previousPrewarm
		connectCarrierClientForStart = previousConnect
	})

	options := CarrierOptions{
		AuthSnapshot:           inline,
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		ConnectTimeout:         time.Second,
	}
	cfg := testCarrierClientConfig("restart-snapshot-test")
	var logs []string
	client, err := recoverCarrierAuthAfterRejection(context.Background(), cfg, options, carriercommon.ErrAuthSnapshotReauthorizationRequired, nil, true, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	})
	if err != nil {
		t.Fatal(err)
	}
	if client == nil || connectCalls != 4 || prewarmCalls != 1 {
		t.Fatalf("client=%v connect_calls=%d prewarm_calls=%d", client, connectCalls, prewarmCalls)
	}
	beforePromotion, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(beforePromotion) != current {
		t.Fatal("last-known-good current snapshot changed before successful candidate promotion")
	}
	if err := saveCarrierAuthSnapshot(options, cfg, nil); err != nil {
		t.Fatal(err)
	}
	promoted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := os.ReadFile(path + authSnapshotPreviousSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(promoted), "fresh-token") || !strings.Contains(string(rotated), "current-token") {
		t.Fatal("successful fresh snapshot was not promoted with current rotated to previous")
	}
	joinedLogs := strings.Join(logs, "\n")
	for _, phase := range []string{"source=refreshed", "source=previous", "source=inline", "fresh_reauthorization_started", "source=fresh"} {
		if !strings.Contains(joinedLogs, phase) {
			t.Fatalf("missing recovery phase %q in logs: %s", phase, joinedLogs)
		}
	}
	for _, secret := range []string{"current-token", "previous-token", "inline-token", "fresh-token", "turn-pass"} {
		if strings.Contains(joinedLogs, secret) {
			t.Fatalf("auth telemetry leaked snapshot material %q: %s", secret, joinedLogs)
		}
	}
}

func TestRefreshCarrierAuthSnapshotAfterRejectionNeverFallsBackToRemote(t *testing.T) {
	path := t.TempDir() + "/auth-snapshot.json"
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshot("stale-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshCarrierAuthSnapshot
	refreshCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		return nil, carriercommon.ErrAuthSnapshotReauthorizationRequired
	}
	remoteCalled := false
	previousFetch := fetchCarrierAuthSnapshotForRecovery
	fetchCarrierAuthSnapshotForRecovery = func(context.Context, string, time.Duration) ([]byte, error) {
		remoteCalled = true
		return nil, errors.New("must not run")
	}
	t.Cleanup(func() {
		refreshCarrierAuthSnapshot = previousRefresh
		fetchCarrierAuthSnapshotForRecovery = previousFetch
	})
	var logs []string
	err := refreshCarrierAuthSnapshotAfterRejection(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshotFile:         path,
		AuthSnapshotOutputFile:   path,
		AuthSnapshotURL:          "https://control.example.com/auth-snapshot",
		AuthSnapshotFetchTimeout: time.Second,
	}, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	})
	if !errors.Is(err, carriercommon.ErrAuthSnapshotReauthorizationRequired) {
		t.Fatalf("error=%v", err)
	}
	if remoteCalled {
		t.Fatal("pre-tunnel recovery contacted auth_snapshot_url")
	}
	joinedLogs := strings.Join(logs, "\n")
	if strings.Contains(joinedLogs, "control.example.com") {
		t.Fatalf("auth snapshot URL leaked into logs: %s", joinedLogs)
	}
}

func TestRecoveryDoesNotRepeatProviderWorkAfterCaptchaRateLimit(t *testing.T) {
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	startup.markProviderRateLimited()
	options := CarrierOptions{startup: startup}
	refreshCalls := 0
	prewarmCalls := 0
	previousRefresh := refreshCarrierAuthSnapshot
	previousPrewarm := prewarmCarrierAuthSnapshot
	refreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte) ([]byte, error) {
		refreshCalls++
		return nil, errors.New("unexpected refresh")
	}
	prewarmCarrierAuthSnapshot = func(carrierconfig.ClientConfig) ([]byte, error) {
		prewarmCalls++
		return nil, errors.New("unexpected prewarm")
	}
	t.Cleanup(func() {
		refreshCarrierAuthSnapshot = previousRefresh
		prewarmCarrierAuthSnapshot = previousPrewarm
	})
	var logs []string
	client, err := recoverCarrierAuthAfterRejection(context.Background(), testCarrierClientConfig("rate-limit-test"), options, carriercommon.ErrAuthSnapshotReauthorizationRequired, nil, true, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	})
	if client != nil || !errors.Is(err, carriercommon.ErrHumanChallengeErrorLimit) {
		t.Fatalf("client=%v error=%v", client, err)
	}
	if refreshCalls != 0 || prewarmCalls != 0 {
		t.Fatalf("refresh_calls=%d prewarm_calls=%d", refreshCalls, prewarmCalls)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "refresh_skipped") || !strings.Contains(joined, "fresh_reauthorization_skipped") {
		t.Fatalf("missing rate-limit skip evidence: %s", joined)
	}
}

func TestProviderRateLimitPersistsAcrossProcessStartupAndClearsAfterRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	firstStartup := newCarrierStartupTelemetry(time.Now(), nil)
	options := CarrierOptions{AuthSnapshotOutputFile: path, startup: firstStartup}
	recordProviderRateLimit(options, nil)
	if !firstStartup.providerRateLimited() {
		t.Fatal("current startup did not retain provider rate limit")
	}
	data, err := os.ReadFile(path + providerCooldownSuffix)
	if err != nil {
		t.Fatal(err)
	}
	var persisted providerCooldownState
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	remaining := time.Until(time.Unix(persisted.BlockedUntil, 0))
	if persisted.Version != providerCooldownVersion || persisted.Attempts != 1 || remaining < providerCooldownInitial-time.Minute || remaining > providerCooldownInitial+time.Minute {
		t.Fatalf("persisted cooldown=%+v remaining=%s", persisted, remaining)
	}

	restarted := newCarrierStartupTelemetry(time.Now(), nil)
	restartedOptions := CarrierOptions{AuthSnapshotOutputFile: path, startup: restarted}
	loadProviderCooldown(restartedOptions, nil)
	if !restarted.providerRateLimited() {
		t.Fatal("restarted process ignored active provider cooldown")
	}

	clearProviderCooldown(restartedOptions, nil)
	if _, err := os.Stat(path + providerCooldownSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider cooldown survived successful recovery: %v", err)
	}
}

func TestProviderRateLimitBackoffEscalatesAndCaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	options := CarrierOptions{AuthSnapshotOutputFile: path, startup: newCarrierStartupTelemetry(time.Now(), nil)}
	for attempt := 1; attempt <= 6; attempt++ {
		recordProviderRateLimit(options, nil)
	}
	data, err := os.ReadFile(path + providerCooldownSuffix)
	if err != nil {
		t.Fatal(err)
	}
	var persisted providerCooldownState
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	remaining := time.Until(time.Unix(persisted.BlockedUntil, 0))
	if persisted.Attempts != 6 || remaining < providerCooldownMaximum-time.Minute || remaining > providerCooldownMaximum+time.Minute {
		t.Fatalf("persisted cooldown=%+v remaining=%s", persisted, remaining)
	}
}

func TestProviderRateLimitExpiresWithoutProcessRestart(t *testing.T) {
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	startup.markProviderRateLimitedUntil(time.Now().Add(time.Hour))
	if !startup.providerRateLimited() {
		t.Fatal("fresh provider cooldown was not active")
	}
	startup.rateMu.Lock()
	startup.rateLimitUntil = time.Now().Add(-time.Second)
	startup.rateMu.Unlock()
	if startup.providerRateLimited() {
		t.Fatal("expired provider cooldown remained active")
	}
	if remaining := startup.providerRateLimitRemaining(); remaining != 0 {
		t.Fatalf("expired provider cooldown remaining=%s", remaining)
	}
}

func TestLoadCarrierAuthSnapshotRefreshesWithoutConfigTimestamp(t *testing.T) {
	refreshCalls := 0
	previousRefresh := refreshExistingCarrierAuthSnapshot
	refreshExistingCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, _ []byte) ([]byte, error) {
		refreshCalls++
		return []byte(testCarrierAuthSnapshot("refreshed-token")), nil
	}
	t.Cleanup(func() { refreshExistingCarrierAuthSnapshot = previousRefresh })
	var logs []string
	err := loadCarrierAuthSnapshot(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshot: testCarrierAuthSnapshotAt("cached-token", time.Now().Add(-time.Hour)),
	}, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshCalls != 1 {
		t.Fatalf("refresh calls=%d", refreshCalls)
	}
	joinedLogs := strings.Join(logs, "\n")
	if !strings.Contains(joinedLogs, "event=refresh_succeeded") || strings.Contains(joinedLogs, "config_trust") {
		t.Fatalf("saved profile refresh still depends on config timestamp: %s", joinedLogs)
	}
}

func TestFetchCarrierAuthSnapshotUsesHTTPSAndBoundsPayload(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			t.Fatalf("method=%s, want GET", request.Method)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"version":1}`)),
			Request:    request,
		}, nil
	})}

	data, err := fetchCarrierAuthSnapshotWithClient(context.Background(), "https://control.example.com/snapshot", client)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
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

func TestCarrierIdleWatchClosesTransportWithoutDeadlineSupport(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()

	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	var released atomic.Int64
	conn := &carrierConn{
		Conn:    &deadlineUnsupportedConn{Conn: left},
		carrier: carrier,
		releaseActive: func() {
			released.Add(1)
			carrier.activeStreams.Add(-1)
		},
		idleTimeout: 30 * time.Millisecond,
		routeClass:  "eu",
		idleStop:    make(chan struct{}),
	}
	carrier.activeStreams.Store(1)
	carrier.registerStream(conn)

	readDone := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected idle watch to close blocked read")
		}
	case <-time.After(time.Second):
		t.Fatal("idle watch did not close transport without deadline support")
	}

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if released.Load() != 1 || carrier.closedStreams.Load() != 1 || carrier.activeStreams.Load() != 0 {
		t.Fatalf(
			"released=%d closed=%d active=%d, want one release and no active streams",
			released.Load(),
			carrier.closedStreams.Load(),
			carrier.activeStreams.Load(),
		)
	}
}

func TestCarrierIdleWatchExtendsDeadlineOnActivity(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()

	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	conn := &carrierConn{
		Conn:          &deadlineUnsupportedConn{Conn: left},
		carrier:       carrier,
		releaseActive: func() { carrier.activeStreams.Add(-1) },
		idleTimeout:   120 * time.Millisecond,
		routeClass:    "eu",
		idleStop:      make(chan struct{}),
	}
	carrier.activeStreams.Store(1)
	carrier.registerStream(conn)

	readDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		for {
			if _, err := conn.Read(buffer); err != nil {
				readDone <- err
				return
			}
		}
	}()
	for range 6 {
		time.Sleep(30 * time.Millisecond)
		if _, err := right.Write([]byte{1}); err != nil {
			t.Fatalf("write activity: %v", err)
		}
	}
	select {
	case err := <-readDone:
		t.Fatalf("idle watch closed active transport early: %v", err)
	default:
	}

	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected idle watch to close transport after activity stopped")
		}
	case <-time.After(time.Second):
		t.Fatal("idle watch did not close transport after activity stopped")
	}
	if carrier.activeStreams.Load() != 0 {
		t.Fatalf("active=%d, want 0 after idle close", carrier.activeStreams.Load())
	}
}

type deadlineUnsupportedConn struct {
	net.Conn
}

func (c *deadlineUnsupportedConn) SetDeadline(time.Time) error {
	return errors.New("deadlines unsupported")
}

func (c *deadlineUnsupportedConn) SetReadDeadline(time.Time) error {
	return errors.New("read deadlines unsupported")
}

func (c *deadlineUnsupportedConn) SetWriteDeadline(time.Time) error {
	return errors.New("write deadlines unsupported")
}

func TestCarrierPressureReclaimClosesStaleBlockedRead(t *testing.T) {
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
	if !carrier.reclaimPressureIdleStream("eu") {
		t.Fatal("pressure reclaim did not close a stale stream with a blocked read")
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected pressure reclaim to unblock read with an error")
		}
	case <-time.After(time.Second):
		t.Fatal("pressure reclaim did not unblock stale read")
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
