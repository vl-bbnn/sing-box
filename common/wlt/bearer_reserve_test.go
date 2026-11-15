//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

func testBearerReserveSnapshot(t *testing.T, session string) []byte {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(testCarrierAuthSnapshot("shared-account-token")), &doc); err != nil {
		t.Fatal(err)
	}
	vk := doc["vk"].(map[string]any)
	vk["calls_auth_mode"] = "bearer_call_token"
	vk["device_id"] = session + "-device"
	vk["session_key"] = session + "-session"
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestBearerReserveMaintenanceCannotEvictActiveStream(t *testing.T) {
	for _, hasReserve := range []bool{true, false} {
		t.Run(fmt.Sprintf("reserve=%t", hasReserve), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth-snapshot.json")
			active, reserve := testBearerReserveSnapshot(t, "active"), testBearerReserveSnapshot(t, "reserve")
			files := map[string][]byte{"": active, authSnapshotQuarantineSuffix: []byte("retained quarantine")}
			if hasReserve {
				files[authSnapshotReserveSuffix] = reserve
			}
			for suffix, raw := range files {
				if err := os.WriteFile(path+suffix, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			local, remote := net.Pipe()
			defer local.Close()
			defer remote.Close()
			oldConnect, oldExisting, oldFresh := connectCarrierClientForStart, refreshExistingCarrierAuthSnapshot, prewarmFreshCarrierAuthSnapshot
			t.Cleanup(func() {
				connectCarrierClientForStart, refreshExistingCarrierAuthSnapshot, prewarmFreshCarrierAuthSnapshot = oldConnect, oldExisting, oldFresh
			})
			providerCalls := 0
			evict := func() error { providerCalls++; _ = remote.Close(); return errors.New("ANOTHER_DEVICE") }
			connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
				return nil, evict()
			}
			refreshExistingCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte, func(string, string)) ([]byte, error) {
				return nil, evict()
			}
			prewarmFreshCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig) ([]byte, error) { return nil, evict() }
			var logs []string
			ready, err := replenishCarrierAuthReserve(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{AuthSnapshotFile: path, AuthSnapshotOutputFile: path, startup: newCarrierStartupTelemetry(time.Now(), nil)}, func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) })
			if err != nil || ready || providerCalls != 0 {
				t.Fatalf("ready=%v err=%v competing provider calls=%d", ready, err, providerCalls)
			}
			if !strings.Contains(strings.Join(logs, "\n"), "reason=serialized_bearer_identity") {
				t.Fatal("serialized deferral missing")
			}
			payload := []byte("active transport remains available")
			writeDone := make(chan error, 1)
			go func() { _, err := remote.Write(payload); writeDone <- err }()
			_ = local.SetReadDeadline(time.Now().Add(time.Second))
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(local, got); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("active stream interrupted: %v", err)
			}
			if err := <-writeDone; err != nil {
				t.Fatal(err)
			}
			for suffix, want := range files {
				got, err := os.ReadFile(path + suffix)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatal("maintenance modified saved auth state")
				}
			}
		})
	}
}

func TestBearerReserveRemainsAvailableForSerializedRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	active, reserve := testBearerReserveSnapshot(t, "active"), testBearerReserveSnapshot(t, "reserve")
	for suffix, raw := range map[string][]byte{"": active, authSnapshotReserveSuffix: reserve} {
		if err := os.WriteFile(path+suffix, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	oldConnect, oldExisting := connectCarrierClientForStart, refreshExistingCarrierAuthSnapshot
	t.Cleanup(func() { connectCarrierClientForStart, refreshExistingCarrierAuthSnapshot = oldConnect, oldExisting })
	connects := 0
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		connects++
		return &carrierengine.Client{}, nil
	}
	refreshExistingCarrierAuthSnapshot = func(context.Context, carrierconfig.ClientConfig, []byte, func(string, string)) ([]byte, error) {
		t.Fatal("saved bearer reserve should be attempted before renewal")
		return nil, errors.New("unexpected renewal")
	}
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	client, err := recoverCarrierAuthAfterRejection(context.Background(), testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{AuthSnapshotFile: path, AuthSnapshotOutputFile: path, startup: startup}, carriercommon.ErrSignalingSessionEnded, nil, false, nil)
	if client == nil || err != nil || connects != 1 || startup.getAuthSource() != "reserve" {
		t.Fatalf("client=%v err=%v connects=%d source=%s", client, err, connects, startup.getAuthSource())
	}
}
