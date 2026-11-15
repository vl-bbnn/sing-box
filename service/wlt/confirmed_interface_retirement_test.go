//go:build with_wlt

package wlt

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/log"
)

const (
	retirementLTE  = "2|pdp_ip0|cellular|[192.0.2.2/32]"
	retirementWiFi = "20|en0|wifi|[198.51.100.2/24]"
)

func newConfirmedRetirementService(t *testing.T, old *wltpkg.Carrier) *Service {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Service{ctx: ctx, logger: log.NewNOPFactory().Logger(), carrier: old,
		carrierReady: closedSignal(), interfaceReady: closedSignal(), initialized: true,
		interfaceKey: retirementLTE}
}

func TestConfirmedPhysicalHandoverAbortsOldBeforeSuccessorSettle(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	var aborts atomic.Int32
	var socketClosed atomic.Bool
	s.abortCarrierHook = func(c *wltpkg.Carrier) error {
		if c != old || s.interfaceGeneration != 1 || s.interfaceKey != retirementWiFi {
			t.Fatalf("aborted wrong carrier or generation: %p %d %q", c, s.interfaceGeneration, s.interfaceKey)
		}
		select {
		case <-s.interfaceReady:
			t.Fatal("new generation admitted before abort")
		default:
		}
		aborts.Add(1)
		socketClosed.Store(true)
		return nil
	}
	var starts atomic.Int32
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) {
		starts.Add(1)
		return &wltpkg.Carrier{}, nil
	}
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	var staleWrites atomic.Int32
	if !socketClosed.Load() {
		staleWrites.Add(1)
	}
	if aborts.Load() != 1 || starts.Load() != 0 || s.carrier != old {
		t.Fatalf("handover boundary: aborts=%d starts=%d carrier=%p", aborts.Load(), starts.Load(), s.carrier)
	}
	if staleWrites.Load() != 0 {
		t.Fatalf("stale fake-socket writes after authoritative callback=%d", staleWrites.Load())
	}
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	if aborts.Load() != 1 || s.interfaceGeneration != 1 {
		t.Fatalf("duplicate callback churn: aborts=%d generation=%d", aborts.Load(), s.interfaceGeneration)
	}
}

func TestConfirmedHandoverDoesNotAbortAddressUpdateOrLossReturn(t *testing.T) {
	for _, tc := range []struct {
		name, current string
		loss          bool
	}{
		{"address-update", "2|pdp_ip0|cellular|[192.0.2.3/32]", false},
		{"return-after-loss", retirementWiFi, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := &wltpkg.Carrier{}
			s := newConfirmedRetirementService(t, old)
			if tc.loss {
				s.interfaceUnavailable = true
				s.interfaceReady = make(chan struct{})
			}
			aborts := 0
			s.abortCarrierHook = func(*wltpkg.Carrier) error { aborts++; return nil }
			s.interfaceUpdated(tc.current, "ios", time.Hour)
			if aborts != 0 {
				t.Fatalf("unexpected aborts=%d", aborts)
			}
		})
	}
}

func TestDelayedPhysicalCallbackCannotAbortNewerCarrier(t *testing.T) {
	old, replacement := &wltpkg.Carrier{}, &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var aborted *wltpkg.Carrier
	s.abortCarrierHook = func(c *wltpkg.Carrier) error { aborted = c; close(entered); <-release; return nil }
	go func() { s.interfaceUpdated(retirementWiFi, "ios", time.Hour); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("old abort did not start")
	}
	s.access.Lock()
	s.carrier = replacement
	s.carrierGeneration++
	s.interfaceGeneration++
	s.interfaceKey = retirementLTE
	s.access.Unlock()
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old callback did not settle")
	}
	if aborted != old || s.carrier != replacement {
		t.Fatal("stale callback touched successor")
	}
}

func TestSupersedingAddressCallbackStillAbortsOldCarrier(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	_, _, first, _, ignored := s.beginInterfaceUpdate(retirementWiFi, "ios")
	if ignored {
		t.Fatal("physical callback ignored")
	}
	_, _, second, _, ignored := s.beginInterfaceUpdate("20|en0|wifi|[198.51.100.3/24]", "ios")
	if ignored || second <= first {
		t.Fatal("address callback did not supersede generation")
	}
	aborts := 0
	s.abortCarrierHook = func(c *wltpkg.Carrier) error {
		if c != old {
			t.Fatal("aborted wrong carrier")
		}
		aborts++
		return nil
	}
	s.abortRetiredPhysicalCarrier(old)
	if aborts != 1 {
		t.Fatalf("still-published obsolete carrier aborts=%d", aborts)
	}
	s.access.Lock()
	s.carrier = &wltpkg.Carrier{}
	s.access.Unlock()
	s.abortRetiredPhysicalCarrier(old)
	if aborts != 1 {
		t.Fatal("delayed callback aborted after replacement")
	}
}

func TestConfirmedPhysicalRetirementRecoversUnpublishedCaller(t *testing.T) {
	s, old := newPendingDialTestService(t)
	s.interfaceKey = retirementLTE
	newCarrier := &wltpkg.Carrier{}
	stream := &dialTestStream{}
	entered := make(chan dialTestAttempt, 2)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx: ctx, carrier: c}
		if c == old {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return stream, nil
	}
	aborted := make(chan *wltpkg.Carrier, 2)
	s.abortCarrierHook = func(c *wltpkg.Carrier) error { aborted <- c; return nil }
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return newCarrier, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := dialAsync(s, ctx)
	if attempt := awaitDialAttempt(t, entered); attempt.carrier != old {
		t.Fatal("first attempt did not use old carrier")
	}
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	select {
	case c := <-aborted:
		if c != old {
			t.Fatal("handover aborted wrong carrier")
		}
	default:
		t.Fatal("old underlay was not aborted before callback returned")
	}
	s.restartCarrier(old, "test confirmed handover", s.interfaceGeneration)
	s.finishInterfaceRecovery(s.interfaceGeneration)
	if attempt := awaitDialAttempt(t, entered); attempt.carrier != newCarrier {
		t.Fatal("retired caller did not retry on current carrier")
	}
	got := awaitDialResult(t, result)
	if got.err != nil || got.stream != stream {
		t.Fatalf("caller outcome stream=%v err=%v", got.stream, got.err)
	}
	assertNoDialLeases(t, s)
}
