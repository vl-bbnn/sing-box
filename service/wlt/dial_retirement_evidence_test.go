//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"io"
	"testing"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
)

func TestPendingDialRetirementEvidenceIsExactAndLocalOnly(t *testing.T) {
	seen := make(map[uint64]bool)
	for _, mode := range []string{"local", "terminal", "caller-first"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newPendingDialTestService(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lease, err := s.acquireDialLease(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer lease.cancel(nil)
			if lease.attemptID == 0 || seen[lease.attemptID] {
				t.Fatal("attempt ID missing or reused across services")
			}
			seen[lease.attemptID] = true
			if mode == "caller-first" {
				cancel()
			}
			s.access.Lock()
			s.retireDialLeasesLocked(mode != "terminal")
			delete(s.pendingDialLeases, lease)
			s.access.Unlock()
			cause := context.Cause(lease.ctx)
			if !errors.Is(cause, context.Canceled) {
				t.Fatal("cancellation compatibility lost")
			}
			meta := carriercommon.LocalGenerationRetirementCause(lease.ctx, errors.Join(lease.ctx.Err(), cause))
			if mode != "local" {
				if meta != nil || lease.retired != nil {
					t.Fatal("non-local event gained recovery evidence")
				}
				return
			}
			if meta == nil || meta.AttemptID != lease.attemptID || meta.CarrierGeneration != lease.generation || meta.InterfaceGeneration != lease.interfaceGeneration || cause != lease.retired {
				t.Fatal("telemetry does not identify the exact retired lease")
			}
		})
	}
}

func TestCallerRetirementMetadataDoesNotAuthorizeServiceRetry(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	calls := 0
	s.dialStreamHook = func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error) {
		calls++
		cause := &wltpkg.LocalGenerationRetirement{AttemptID: 123, CarrierGeneration: 1}
		cancel(cause)
		return nil, cause
	}
	_, err := s.DialStream(ctx, "eu", "example.com:443")
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("diagnostic marker authorized retry: err=%v calls=%d", err, calls)
	}
	assertNoDialLeases(t, s)
}
