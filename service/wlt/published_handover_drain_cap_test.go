//go:build with_wlt

package wlt

import (
	"context"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing/common/json/badoption"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishedHandoverAbsoluteCapSurvivesAddressSuccessor(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	s.options.PhysicalHandoverDrainGrace = badoption.Duration(time.Second)
	var aborts atomic.Int32
	s.abortCarrierHook = func(c *wltpkg.Carrier) error {
		if c == old {
			aborts.Add(1)
		}
		return nil
	}
	s.closeSnapshotStatsHook = func(*wltpkg.Carrier) wltpkg.CarrierStats { return wltpkg.CarrierStats{ActiveStreams: 1} }
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return nil, context.Canceled }
	begin := time.Now()
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	time.Sleep(80 * time.Millisecond)
	address := strings.Replace(retirementWiFi, "192.0.2.2", "192.0.2.99", 1)
	if address == retirementWiFi {
		address = retirementWiFi + "address-change"
	}
	s.interfaceUpdated(address, "ios", time.Hour)
	deadline := begin.Add(1600 * time.Millisecond)
	for aborts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if aborts.Load() == 0 {
		t.Fatal("address successor extended original carrier retention beyond absolute grace")
	}
}

func TestPublishedHandoverLossInvalidatesOriginalDeadline(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	s.options.PhysicalHandoverDrainGrace = badoption.Duration(time.Second)
	var aborts atomic.Int32
	s.abortCarrierHook = func(*wltpkg.Carrier) error { aborts.Add(1); return nil }
	s.closeSnapshotStatsHook = func(*wltpkg.Carrier) wltpkg.CarrierStats { return wltpkg.CarrierStats{ActiveStreams: 1} }
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	s.NetworkUnavailable()
	if aborts.Load() != 1 {
		t.Fatal("real loss did not abort immediately")
	}
	time.Sleep(1250 * time.Millisecond)
	if aborts.Load() != 1 {
		t.Fatal("old positive handover timer survived real loss")
	}
}

func TestPublishedHandoverCapNeverAbortsReplacement(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	s.options.PhysicalHandoverDrainGrace = badoption.Duration(time.Second)
	var aborts atomic.Int32
	s.abortCarrierHook = func(*wltpkg.Carrier) error { aborts.Add(1); return nil }
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	s.access.Lock()
	s.carrier = &wltpkg.Carrier{}
	s.interfaceGeneration++
	s.access.Unlock()
	time.Sleep(1250 * time.Millisecond)
	if aborts.Load() != 0 {
		t.Fatal("stale original carrier deadline touched successor")
	}
}

func TestPublishedHandoverUnknownSuccessorImmediatelyAbortsRetainedCarrier(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	s.options.PhysicalHandoverDrainGrace = badoption.Duration(5 * time.Second)
	var aborts atomic.Int32
	s.abortCarrierHook = func(c *wltpkg.Carrier) error {
		if c == old {
			aborts.Add(1)
		}
		return nil
	}
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	if aborts.Load() != 0 {
		t.Fatal("positive grace was not active")
	}
	s.interfaceUpdated("unknown-positive-interface", "ios", time.Hour)
	if aborts.Load() != 1 {
		t.Fatal("incompatible callback discarded cap without closing retained carrier")
	}
}
