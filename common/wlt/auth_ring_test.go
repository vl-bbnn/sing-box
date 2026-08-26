//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

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
	client, err := recoverCarrierAuthAfterRejection(context.Background(), testCarrierClientConfig("restart-snapshot-test"), options, errors.New("active rejected"), nil)
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
