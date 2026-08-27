//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

func TestAuthRingFaultMarkerIsOneShotAndRequiresIndependentReserve(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	reserve := []byte(testCarrierAuthSnapshot("reserve-token"))
	if err := os.WriteFile(path, active, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotReserveSuffix, reserve, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ArmCarrierAuthRingTestRejectActiveOnce(path); err != nil {
		t.Fatal(err)
	}
	statusJSON, err := CarrierAuthRingStatusJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statusJSON, `"active_present":true`) ||
		!strings.Contains(statusJSON, `"reserve_present":true`) ||
		!strings.Contains(statusJSON, `"fault_armed":true`) ||
		strings.Contains(statusJSON, "active-token") {
		t.Fatalf("unexpected sanitized status: %s", statusJSON)
	}
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(carrierconfig.ClientConfig, []byte, []byte) (bool, error) {
		return true, nil
	}
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	rejected, consumed, err := consumeCarrierAuthRingTestRejectActiveOnce(
		testCarrierClientConfig("restart-snapshot-test"),
		CarrierOptions{AuthSnapshotFile: path},
		nil,
	)
	if err != nil || !consumed || !bytes.Equal(rejected, active) {
		t.Fatalf("consumed=%t rejected=%q err=%v", consumed, rejected, err)
	}
	if _, err := os.Stat(path + authSnapshotRejectOnceSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fault marker was not consumed: %v", err)
	}
	if _, consumed, err := consumeCarrierAuthRingTestRejectActiveOnce(
		testCarrierClientConfig("restart-snapshot-test"),
		CarrierOptions{AuthSnapshotFile: path},
		nil,
	); err != nil || consumed {
		t.Fatalf("one-shot marker repeated consumed=%t err=%v", consumed, err)
	}
}

func TestAuthRingBootstrapInstallsIndependentIdentitiesOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	reserve := []byte(testCarrierAuthSnapshot("reserve-token"))
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(_ carrierconfig.ClientConfig, left []byte, right []byte) (bool, error) {
		return !bytes.Contains(left, []byte("active-token")) || !bytes.Contains(right, []byte("active-token")), nil
	}
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	options := CarrierOptions{
		AuthSnapshot:           string(active),
		AuthReserveSnapshot:    string(reserve),
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		AuthSnapshotPreferFile: false,
		AuthSnapshotSkipRemote: true,
	}
	var logs []string
	if _, err := bootstrapCarrierAuthRing(testCarrierClientConfig("restart-snapshot-test"), options, func(format string, arguments ...any) {
		logs = append(logs, fmt.Sprintf(format, arguments...))
	}); err != nil {
		t.Fatal(err)
	}
	for suffix, token := range map[string]string{"": "active-token", authSnapshotReserveSuffix: "reserve-token"} {
		content, err := os.ReadFile(path + suffix)
		if err != nil || !bytes.Contains(content, []byte(token)) {
			t.Fatalf("bootstrap file suffix=%q token=%q err=%v", suffix, token, err)
		}
		if info, err := os.Stat(path + suffix); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("bootstrap file suffix=%q mode=%v err=%v", suffix, info.Mode().Perm(), err)
		}
	}
	if _, err := os.Stat(path + authSnapshotBootstrapSuffix); err != nil {
		t.Fatalf("bootstrap marker unavailable: %v", err)
	}
	statusJSON, err := CarrierAuthRingStatusJSON(path)
	if err != nil || !strings.Contains(statusJSON, `"bootstrap_consumed":true`) {
		t.Fatalf("bootstrap status unavailable status=%q err=%v", statusJSON, err)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "bootstrap_consumed identities=2 independent=true") {
		t.Fatalf("sanitized bootstrap evidence missing: %v", logs)
	}

	// The consumed marker makes the embedded bundle one-shot even if a later
	// profile still contains it.
	options.AuthSnapshot = "invalid-reused-active"
	options.AuthReserveSnapshot = "invalid-reused-reserve"
	if _, err := bootstrapCarrierAuthRing(testCarrierClientConfig("restart-snapshot-test"), options, nil); err != nil {
		t.Fatalf("consumed bootstrap was revalidated or reused: %v", err)
	}
}

func TestAuthRingBootstrapRejectsSameIdentityBeforeWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	snapshot := testCarrierAuthSnapshot("same-token")
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(carrierconfig.ClientConfig, []byte, []byte) (bool, error) { return false, nil }
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	_, err := bootstrapCarrierAuthRing(testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshot:           snapshot,
		AuthReserveSnapshot:    snapshot,
		AuthSnapshotOutputFile: path,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "not independent") {
		t.Fatalf("error=%v, want same-identity rejection", err)
	}
	for _, suffix := range []string{"", authSnapshotReserveSuffix, authSnapshotBootstrapSuffix} {
		if _, statErr := os.Stat(path + suffix); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("partial bootstrap file suffix=%q err=%v", suffix, statErr)
		}
	}
}

func TestAuthRingBootstrapResumesAfterReserveFirstInterruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	reserve := []byte(testCarrierAuthSnapshot("reserve-token"))
	if err := os.WriteFile(path+authSnapshotReserveSuffix, reserve, 0o600); err != nil {
		t.Fatal(err)
	}
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(_ carrierconfig.ClientConfig, left []byte, right []byte) (bool, error) {
		leftActive := bytes.Contains(left, []byte("active-token"))
		rightActive := bytes.Contains(right, []byte("active-token"))
		return leftActive != rightActive, nil
	}
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	if _, err := bootstrapCarrierAuthRing(testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshot:           string(active),
		AuthReserveSnapshot:    string(reserve),
		AuthSnapshotOutputFile: path,
	}, nil); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(content, []byte("active-token")) {
		t.Fatalf("active bootstrap was not resumed: %v", err)
	}
}

func TestAuthRingBootstrapPreservesAnExistingIndependentRing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	existingActive := []byte(testCarrierAuthSnapshot("existing-active"))
	existingReserve := []byte(testCarrierAuthSnapshot("existing-reserve"))
	for suffix, content := range map[string][]byte{"": existingActive, authSnapshotReserveSuffix: existingReserve} {
		if err := os.WriteFile(path+suffix, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(_ carrierconfig.ClientConfig, left []byte, right []byte) (bool, error) {
		return !bytes.Equal(left, right), nil
	}
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	provided, err := bootstrapCarrierAuthRing(testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshot:           testCarrierAuthSnapshot("bootstrap-active"),
		AuthReserveSnapshot:    testCarrierAuthSnapshot("bootstrap-reserve"),
		AuthSnapshotOutputFile: path,
	}, nil)
	if err != nil || !provided {
		t.Fatalf("provided=%t err=%v", provided, err)
	}
	for suffix, want := range map[string][]byte{"": existingActive, authSnapshotReserveSuffix: existingReserve} {
		got, readErr := os.ReadFile(path + suffix)
		if readErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("existing ring changed suffix=%q err=%v", suffix, readErr)
		}
	}
}

func TestInjectedIdentityRejectionSkipsSameIdentitySessions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	reserve := []byte(testCarrierAuthSnapshot("reserve-token"))
	if err := os.WriteFile(path+authSnapshotPreviousSuffix, active, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotReserveSuffix, reserve, 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshCarrierAuthSnapshot
	previousPrewarm := prewarmCarrierAuthSnapshot
	previousConnect := connectCarrierClientForStart
	previousIndependent := carrierAuthSnapshotsIndependent
	refreshCalls := 0
	prewarmCalls := 0
	connectCalls := 0
	refreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte) ([]byte, error) {
		refreshCalls++
		return nil, errors.New("must not refresh")
	}
	prewarmCarrierAuthSnapshot = func(carrierconfig.ClientConfig) ([]byte, error) {
		prewarmCalls++
		return nil, errors.New("must not authorize")
	}
	carrierAuthSnapshotsIndependent = func(_ carrierconfig.ClientConfig, left []byte, right []byte) (bool, error) {
		return !bytes.Equal(left, right), nil
	}
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		connectCalls++
		return &carrierengine.Client{}, nil
	}
	t.Cleanup(func() {
		refreshCarrierAuthSnapshot = previousRefresh
		prewarmCarrierAuthSnapshot = previousPrewarm
		connectCarrierClientForStart = previousConnect
		carrierAuthSnapshotsIndependent = previousIndependent
	})
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	var logs []string
	client, err := recoverCarrierAuthAfterRejection(
		context.Background(),
		testCarrierClientConfig("restart-snapshot-test"),
		CarrierOptions{
			AuthSnapshot:           string(active),
			AuthSnapshotFile:       path,
			AuthSnapshotOutputFile: path,
			ConnectTimeout:         time.Second,
			startup:                startup,
		},
		carriercommon.ErrAuthSnapshotReauthorizationRequired,
		active,
		true,
		func(format string, arguments ...any) { logs = append(logs, fmt.Sprintf(format, arguments...)) },
	)
	if err != nil || client == nil {
		t.Fatalf("client=%v err=%v", client, err)
	}
	if connectCalls != 1 || refreshCalls != 0 || prewarmCalls != 0 {
		t.Fatalf("connect=%d refresh=%d prewarm=%d", connectCalls, refreshCalls, prewarmCalls)
	}
	if source := startup.getAuthSource(); source != "reserve" {
		t.Fatalf("auth source=%q", source)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "candidate_rejected_same_identity source=previous") ||
		!strings.Contains(joined, "fallback_succeeded source=reserve") {
		t.Fatalf("identity-scoped recovery evidence missing: %s", joined)
	}
}

func TestRecoveryUsesReserveBeforeProviderAuthorization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshot("active-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotReserveSuffix, []byte(testCarrierAuthSnapshot("reserve-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	previousRefresh := refreshCarrierAuthSnapshot
	previousPrewarm := prewarmCarrierAuthSnapshot
	previousConnect := connectCarrierClientForStart
	refreshCalls := 0
	prewarmCalls := 0
	refreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte) ([]byte, error) {
		refreshCalls++
		return nil, errors.New("must not refresh")
	}
	prewarmCarrierAuthSnapshot = func(carrierconfig.ClientConfig) ([]byte, error) {
		prewarmCalls++
		return nil, errors.New("must not authorize")
	}
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		return &carrierengine.Client{}, nil
	}
	t.Cleanup(func() {
		refreshCarrierAuthSnapshot = previousRefresh
		prewarmCarrierAuthSnapshot = previousPrewarm
		connectCarrierClientForStart = previousConnect
	})
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	options := CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		ConnectTimeout:         time.Second,
		startup:                startup,
	}
	client, err := recoverCarrierAuthAfterRejection(context.Background(), testCarrierClientConfig("restart-snapshot-test"), options, errors.New("active rejected"), nil, true, nil)
	if err != nil || client == nil {
		t.Fatalf("client=%v err=%v", client, err)
	}
	if refreshCalls != 0 || prewarmCalls != 0 {
		t.Fatalf("refresh_calls=%d prewarm_calls=%d", refreshCalls, prewarmCalls)
	}
	if source := startup.getAuthSource(); source != "reserve" {
		t.Fatalf("auth source=%q", source)
	}
}

func TestTransportRecoveryExhaustsLocalRingWithoutProviderAuthorization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	reserve := []byte(testCarrierAuthSnapshot("reserve-token"))
	if err := os.WriteFile(path, active, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotReserveSuffix, reserve, 0o600); err != nil {
		t.Fatal(err)
	}

	previousRefresh := refreshCarrierAuthSnapshot
	previousPrewarm := prewarmCarrierAuthSnapshot
	previousConnect := connectCarrierClientForStart
	previousIndependent := carrierAuthSnapshotsIndependent
	refreshCalls := 0
	prewarmCalls := 0
	connectCalls := 0
	refreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte) ([]byte, error) {
		refreshCalls++
		return nil, errors.New("must not refresh")
	}
	prewarmCarrierAuthSnapshot = func(carrierconfig.ClientConfig) ([]byte, error) {
		prewarmCalls++
		return nil, errors.New("must not authorize")
	}
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		connectCalls++
		return nil, context.DeadlineExceeded
	}
	carrierAuthSnapshotsIndependent = func(_ carrierconfig.ClientConfig, left []byte, right []byte) (bool, error) {
		return !bytes.Equal(left, right), nil
	}
	t.Cleanup(func() {
		refreshCarrierAuthSnapshot = previousRefresh
		prewarmCarrierAuthSnapshot = previousPrewarm
		connectCarrierClientForStart = previousConnect
		carrierAuthSnapshotsIndependent = previousIndependent
	})

	startup := newCarrierStartupTelemetry(time.Now(), nil)
	var logs []string
	client, err := recoverCarrierAuthAfterRejection(
		context.Background(),
		testCarrierClientConfig("transport-local-only-test"),
		CarrierOptions{
			AuthSnapshotFile:       path,
			AuthSnapshotOutputFile: path,
			ConnectTimeout:         time.Second,
			startup:                startup,
		},
		context.DeadlineExceeded,
		active,
		false,
		func(format string, arguments ...any) { logs = append(logs, fmt.Sprintf(format, arguments...)) },
	)
	if client != nil || err == nil {
		t.Fatalf("client=%v err=%v", client, err)
	}
	if connectCalls != 1 || refreshCalls != 0 || prewarmCalls != 0 {
		t.Fatalf("connect=%d refresh=%d prewarm=%d", connectCalls, refreshCalls, prewarmCalls)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "fallback_started source=reserve") ||
		!strings.Contains(joined, "provider_fallback_skipped reason=local_only") {
		t.Fatalf("local-only transport recovery evidence missing: %s", joined)
	}
}

func TestReservePromotionQuarantinesOldActiveUntilReplacementValidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	replacement := []byte(testCarrierAuthSnapshot("reserve-token"))
	if err := os.WriteFile(path, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotPreviousSuffix, active, 0o600); err != nil {
		t.Fatal(err)
	}
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(carrierconfig.ClientConfig, []byte, []byte) (bool, error) {
		return true, nil
	}
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	options := CarrierOptions{AuthSnapshotFile: path, AuthSnapshotOutputFile: path}
	if err := quarantineRejectedCarrierAuth(testCarrierClientConfig("restart-snapshot-test"), options, "reserve", nil); err != nil {
		t.Fatal(err)
	}
	quarantined, err := os.ReadFile(path + authSnapshotQuarantineSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(quarantined), "active-token") {
		t.Fatal("rejected active identity was not quarantined")
	}
	if _, err := os.Stat(path + authSnapshotPreviousSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected identity remained in normal fallback rotation: %v", err)
	}
}

func TestSameIdentitySessionRotationIsNotQuarantined(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	for suffix, content := range map[string][]byte{
		"":                         active,
		authSnapshotPreviousSuffix: active,
	} {
		if err := os.WriteFile(path+suffix, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(carrierconfig.ClientConfig, []byte, []byte) (bool, error) {
		return false, nil
	}
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	options := CarrierOptions{AuthSnapshotFile: path, AuthSnapshotOutputFile: path}
	if err := quarantineRejectedCarrierAuth(testCarrierClientConfig("restart-snapshot-test"), options, "inline", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + authSnapshotQuarantineSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("same identity was quarantined: %v", err)
	}
	if _, err := os.Stat(path + authSnapshotPreviousSuffix); err != nil {
		t.Fatalf("same-identity previous session was removed: %v", err)
	}
}

func TestValidatedReserveReplenishmentRetiresQuarantine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	fresh := []byte(testCarrierAuthSnapshot("fresh-token"))
	if err := os.WriteFile(path, active, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+authSnapshotQuarantineSuffix, []byte(testCarrierAuthSnapshot("rejected-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	previousFresh := prewarmFreshCarrierAuthSnapshot
	previousIndependent := carrierAuthSnapshotsIndependent
	previousConnect := connectCarrierClientForStart
	previousPromote := promoteCarrierAuthSnapshot
	prewarmFreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig) ([]byte, error) {
		return fresh, nil
	}
	carrierAuthSnapshotsIndependent = func(carrierconfig.ClientConfig, []byte, []byte) (bool, error) {
		return true, nil
	}
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		return &carrierengine.Client{}, nil
	}
	promoteCarrierAuthSnapshot = func(carrierconfig.ClientConfig) error { return nil }
	t.Cleanup(func() {
		prewarmFreshCarrierAuthSnapshot = previousFresh
		carrierAuthSnapshotsIndependent = previousIndependent
		connectCarrierClientForStart = previousConnect
		promoteCarrierAuthSnapshot = previousPromote
	})
	options := CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		ConnectTimeout:         time.Second,
		startup:                newCarrierStartupTelemetry(time.Now(), nil),
	}
	if err := replenishCarrierAuthReserve(context.Background(), testCarrierClientConfig("restart-snapshot-test"), options, nil); err != nil {
		t.Fatal(err)
	}
	reserve, err := os.ReadFile(path + authSnapshotReserveSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(reserve), "fresh-token") {
		t.Fatal("fresh validated identity was not installed as reserve")
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(current), "active-token") {
		t.Fatal("reserve replenishment replaced the active identity")
	}
	if _, err := os.Stat(path + authSnapshotQuarantineSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quarantine survived validated replacement: %v", err)
	}
}

func TestReserveReplenishmentSkipsProviderDuringCooldown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshot("active-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	previousFresh := prewarmFreshCarrierAuthSnapshot
	called := false
	prewarmFreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig) ([]byte, error) {
		called = true
		return nil, errors.New("unexpected provider call")
	}
	t.Cleanup(func() { prewarmFreshCarrierAuthSnapshot = previousFresh })
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	startup.markProviderRateLimited()
	if err := replenishCarrierAuthReserve(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		startup:                startup,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("provider authorization ran during cooldown")
	}
}

func TestReserveReplenishmentPersistsNewProviderCooldown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshot("active-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	previousFresh := prewarmFreshCarrierAuthSnapshot
	prewarmFreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig) ([]byte, error) {
		return nil, errors.Join(errors.New("provider blocked"), carriercommon.ErrHumanChallengeErrorLimit)
	}
	t.Cleanup(func() { prewarmFreshCarrierAuthSnapshot = previousFresh })
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	err := replenishCarrierAuthReserve(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		startup:                startup,
	}, nil)
	if !errors.Is(err, carriercommon.ErrHumanChallengeErrorLimit) {
		t.Fatalf("error=%v", err)
	}
	if !startup.providerRateLimited() {
		t.Fatal("provider cooldown was not retained in memory")
	}
	if _, err := os.Stat(path + providerCooldownSuffix); err != nil {
		t.Fatalf("provider cooldown was not persisted: %v", err)
	}
}

func TestCrashRecoveryRetiresQuarantineOnlyWithIndependentReserve(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active := []byte(testCarrierAuthSnapshot("active-token"))
	reserve := []byte(testCarrierAuthSnapshot("reserve-token"))
	quarantine := []byte(testCarrierAuthSnapshot("rejected-token"))
	for suffix, content := range map[string][]byte{
		"":                           active,
		authSnapshotReserveSuffix:    reserve,
		authSnapshotQuarantineSuffix: quarantine,
	} {
		if err := os.WriteFile(path+suffix, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	previousIndependent := carrierAuthSnapshotsIndependent
	carrierAuthSnapshotsIndependent = func(carrierconfig.ClientConfig, []byte, []byte) (bool, error) {
		return true, nil
	}
	t.Cleanup(func() { carrierAuthSnapshotsIndependent = previousIndependent })
	options := CarrierOptions{AuthSnapshotFile: path, AuthSnapshotOutputFile: path}
	if err := replenishCarrierAuthReserve(context.Background(), testCarrierClientConfig("restart-snapshot-test"), options, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + authSnapshotQuarantineSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validated reserve did not retire quarantine: %v", err)
	}
}

func TestReserveReplenishmentWaitsForTrafficReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	if err := os.WriteFile(path, []byte(testCarrierAuthSnapshot("active-token")), 0o600); err != nil {
		t.Fatal(err)
	}
	previousFresh := prewarmFreshCarrierAuthSnapshot
	called := make(chan struct{}, 1)
	prewarmFreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig) ([]byte, error) {
		called <- struct{}{}
		return nil, errors.New("test stop")
	}
	t.Cleanup(func() { prewarmFreshCarrierAuthSnapshot = previousFresh })
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := CarrierOptions{
		AuthSnapshotFile:       path,
		AuthSnapshotOutputFile: path,
		startup:                startup,
	}
	scheduleCarrierAuthReserve(ctx, testCarrierClientConfig("restart-snapshot-test"), options, nil)
	select {
	case <-called:
		t.Fatal("reserve replenishment ran before traffic_ready")
	case <-time.After(50 * time.Millisecond):
	}
	startup.markTrafficReady()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("reserve replenishment did not run after traffic_ready")
	}
}
