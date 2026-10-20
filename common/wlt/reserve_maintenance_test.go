//go:build with_wlt

package wlt

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

func TestReserveMaintenanceWaitsForRecheckAndJoinsOnCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth-snapshot.json")
	for suffix, token := range map[string]string{"": "active-token", authSnapshotReserveSuffix: "reserve-token"} {
		if err := os.WriteFile(path+suffix, []byte(testCarrierAuthSnapshot(token)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	oldIndependent, oldConnect, oldPromote := carrierAuthSnapshotsIndependent, connectCarrierClientForStart, promoteCarrierAuthSnapshot
	t.Cleanup(func() {
		carrierAuthSnapshotsIndependent, connectCarrierClientForStart, promoteCarrierAuthSnapshot = oldIndependent, oldConnect, oldPromote
	})
	carrierAuthSnapshotsIndependent = func(carrierconfig.ClientConfig, []byte, []byte) (bool, error) { return true, nil }
	connectCarrierClientForStart = func(context.Context, *carrierconfig.ClientConfig, time.Duration, func(string, ...any)) (*carrierengine.Client, error) {
		return &carrierengine.Client{}, nil
	}
	promoteCarrierAuthSnapshot = func(carrierconfig.ClientConfig) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	startup := newCarrierStartupTelemetry(time.Now(), nil)
	startup.markTrafficReady()
	scheduled := make(chan string, 1)
	done := scheduleCarrierAuthReserve(ctx, testCarrierClientConfig("restart-snapshot-test"), CarrierOptions{AuthSnapshotFile: path, AuthSnapshotOutputFile: path, ConnectTimeout: time.Second, startup: startup}, func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		if strings.Contains(line, "event=replenish_deferred retry_seconds=") {
			select {
			case scheduled <- line:
			default:
			}
		}
	})
	t.Cleanup(func() { cancel(); <-done })
	select {
	case line := <-scheduled:
		if !strings.Contains(line, "validated=true") {
			t.Fatalf("successful validation not followed by recheck: %s", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reserve validation did not schedule its next check")
	}
	select {
	case <-done:
		t.Fatal("maintenance exited after first success")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance wait did not stop on cancellation")
	}
}

func TestCarrierCloseJoinsReserveMaintenanceBeforeReturning(t *testing.T) {
	for _, immediate := range []bool{false, true} {
		t.Run(fmt.Sprintf("abort=%t", immediate), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			workerDone, release := make(chan struct{}), make(chan struct{})
			workerCancelled := make(chan struct{})
			go func() {
				<-ctx.Done()
				close(workerCancelled)
				<-release // old generation is still restoring its auth cache
				close(workerDone)
			}()
			c := &Carrier{cancel: cancel, authReserveDone: workerDone}
			closed := make(chan struct{})
			go func() { _ = c.close(immediate); close(closed) }()
			<-workerCancelled
			select {
			case <-closed:
				close(release)
				t.Fatal("close returned while old auth worker was still running")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("close did not finish after auth worker exited")
			}
		})
	}
}
