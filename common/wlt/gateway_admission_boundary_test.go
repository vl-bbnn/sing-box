//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGatewayAdmissionStopsRetriesWithoutProviderRecovery(t *testing.T) {
	for _, err := range []error{carriercommon.ErrRelayAdmissionRejected, errors.Join(carriercommon.ErrRelayAdmissionRejected, context.Canceled)} {
		if !isFatalCarrierConnectError(err) || carrierAuthRecoveryRequired(err) || isProviderRateLimitError(err) {
			t.Fatalf("gateway failure must be terminal without provider authority: %v", err)
		}
	}
	if !carrierAuthRecoveryRequired(carriercommon.ErrAuthSnapshotReauthorizationRequired) {
		t.Fatal("true provider authorization recovery was removed")
	}
	for _, err := range []error{context.DeadlineExceeded, errors.New("relay gateway admission rejected")} {
		if isFatalCarrierConnectError(err) {
			t.Fatalf("untyped/transient error gained admission authority: %v", err)
		}
	}
}

func TestGatewayAdmissionRecoveryPreservesStateAndSkipsAllProviderWork(t *testing.T) {
	oldConnect, oldRefresh, oldPrewarm := connectCarrierClientForStart, refreshCarrierAuthSnapshot, prewarmCarrierAuthSnapshot
	connects, refreshes, prewarms := 0, 0, 0
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		connects++
		return nil, carriercommon.ErrRelayAdmissionRejected
	}
	refreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte) ([]byte, error) {
		refreshes++
		return nil, carriercommon.ErrRelayAdmissionRejected
	}
	prewarmCarrierAuthSnapshot = func(carrierconfig.ClientConfig) ([]byte, error) {
		prewarms++
		return nil, carriercommon.ErrRelayAdmissionRejected
	}
	t.Cleanup(func() {
		connectCarrierClientForStart = oldConnect
		refreshCarrierAuthSnapshot = oldRefresh
		prewarmCarrierAuthSnapshot = oldPrewarm
	})
	path := filepath.Join(t.TempDir(), "auth.json")
	snapshots := map[string][]byte{path: []byte(testCarrierAuthSnapshot("active")), path + authSnapshotPreviousSuffix: []byte(testCarrierAuthSnapshot("previous"))}
	for name, data := range snapshots {
		if err := os.WriteFile(name, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := testCarrierClientConfig("gateway-admission-test")
	options := CarrierOptions{AuthSnapshot: string(snapshots[path]), AuthSnapshotFile: path, AuthSnapshotOutputFile: path, ConnectTimeout: time.Second}
	for _, allow := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		original := errors.Join(carriercommon.ErrRelayAdmissionRejected, errors.New("actual gateway rejected route"))
		client, err := recoverCarrierAuthAfterRejection(ctx, cfg, options, original, nil, allow, nil)
		cancel()
		if client != nil || err != original || connects != 0 || refreshes != 0 || prewarms != 0 {
			t.Fatalf("gateway error touched provider: client=%v err=%v calls=%d/%d/%d", client, err, connects, refreshes, prewarms)
		}
		for name, data := range snapshots {
			actual, err := os.ReadFile(name)
			if err != nil || string(actual) != string(data) {
				t.Fatalf("auth state changed: %s", name)
			}
		}
	}
}
