//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

func TestEndedSessionRecoveryPreservesIdentityAndSavedLanes(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		reserve, reserveWorks    bool
		refreshFails, cooldown   bool
		wantSuccess              bool
		wantConnect, wantRefresh int
		wantSource               string
	}{
		{name: "renew_current_without_reserve", wantSuccess: true, wantConnect: 1, wantRefresh: 1, wantSource: "current_refreshed"},
		{name: "saved_reserve_before_provider", reserve: true, reserveWorks: true, wantSuccess: true, wantConnect: 1, wantSource: "reserve"},
		{name: "closed_reserve_then_current_renewal", reserve: true, wantSuccess: true, wantConnect: 2, wantRefresh: 1, wantSource: "current_refreshed"},
		{name: "provider_unreachable_retains_identity", refreshFails: true, wantRefresh: 1},
		{name: "cooldown_blocks_session_renewal", cooldown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth-snapshot.json")
			active := []byte(testCarrierAuthSnapshot("active-token"))
			if err := os.WriteFile(path, active, 0o600); err != nil {
				t.Fatal(err)
			}
			if tc.reserve {
				if err := os.WriteFile(path+authSnapshotReserveSuffix, []byte(testCarrierAuthSnapshot("reserve-token")), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			oldConnect, oldExisting := connectCarrierClientForStart, refreshExistingCarrierAuthSnapshot
			oldFull, oldPrewarm := refreshCarrierAuthSnapshot, prewarmCarrierAuthSnapshot
			oldIndependent := carrierAuthSnapshotsIndependent
			t.Cleanup(func() {
				connectCarrierClientForStart, refreshExistingCarrierAuthSnapshot = oldConnect, oldExisting
				refreshCarrierAuthSnapshot, prewarmCarrierAuthSnapshot = oldFull, oldPrewarm
				carrierAuthSnapshotsIndependent = oldIndependent
			})
			connects, refreshes := 0, 0
			carrierAuthSnapshotsIndependent = func(_ carrierconfig.ClientConfig, a, b []byte) (bool, error) { return !bytes.Equal(a, b), nil }
			connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
				connects++
				if tc.reserveWorks || refreshes > 0 {
					return &carrierengine.Client{}, nil
				}
				return nil, carriercommon.ErrSignalingSessionEnded
			}
			refreshExistingCarrierAuthSnapshot = func(_ context.Context, _ carrierconfig.ClientConfig, raw []byte, _ func(string, string)) ([]byte, error) {
				refreshes++
				if !bytes.Equal(raw, active) {
					t.Fatal("lost active identity before existing-only renewal")
				}
				if tc.refreshFails {
					return nil, context.DeadlineExceeded
				}
				return raw, nil
			}
			refreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte) ([]byte, error) {
				t.Fatal("session end permitted full authorization refresh")
				return nil, errors.New("unexpected full refresh")
			}
			prewarmCarrierAuthSnapshot = func(carrierconfig.ClientConfig) ([]byte, error) {
				t.Fatal("session end permitted fresh authorization")
				return nil, errors.New("unexpected fresh authorization")
			}
			startup := newCarrierStartupTelemetry(time.Now(), nil)
			options := CarrierOptions{AuthSnapshotFile: path, AuthSnapshotOutputFile: path, ConnectTimeout: time.Second, startup: startup}
			if tc.cooldown {
				recordProviderRateLimit(options, nil)
			}
			client, err := recoverCarrierAuthAfterRejection(context.Background(), testCarrierClientConfig("restart-snapshot-test"), options, carriercommon.ErrSignalingSessionEnded, nil, false, nil)
			if (client != nil && err == nil) != tc.wantSuccess {
				t.Fatalf("client=%v err=%v, want success=%v", client, err, tc.wantSuccess)
			}
			if connects != tc.wantConnect || refreshes != tc.wantRefresh {
				t.Fatalf("connect=%d refresh=%d, want %d/%d", connects, refreshes, tc.wantConnect, tc.wantRefresh)
			}
			if tc.wantSuccess && startup.getAuthSource() != tc.wantSource {
				t.Fatalf("source=%s, want %s", startup.getAuthSource(), tc.wantSource)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, active) {
				t.Fatal("recovery wrote the active snapshot before promotion")
			}
		})
	}
}
