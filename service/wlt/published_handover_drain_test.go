//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestOptInPositiveHandoverKeepsPublishedReadAndClosesDialGate(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	s.options.PhysicalHandoverDrainGrace = badoption.Duration(5 * time.Second)
	var aborts atomic.Int32
	s.abortCarrierHook = func(*wltpkg.Carrier) error { aborts.Add(1); return nil }
	lease, err := s.acquireDialLease(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.interfaceUpdated(retirementWiFi, "ios", time.Hour)
	if aborts.Load() != 0 {
		t.Fatal("published stream aborted at positive Wi-Fi callback")
	}
	if lease.ctx.Err() == nil {
		t.Fatal("old unpublished OPEN was not retired")
	}
	select {
	case <-s.interfaceReady:
		t.Fatal("new OPEN admitted on retained carrier")
	default:
	}
}

func TestOptInHandoverCompletesExactOriginalBodyWithoutReplay(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	sender, receiver := net.Pipe()
	defer sender.Close()
	defer receiver.Close()
	payload := bytes.Repeat([]byte("WLT"), 8192)
	var active atomic.Int64
	active.Store(1)
	s.closeSnapshotStatsHook = func(*wltpkg.Carrier) wltpkg.CarrierStats { return wltpkg.CarrierStats{ActiveStreams: active.Load()} }
	sent, received := make(chan error, 1), make(chan []byte, 1)
	go func() {
		_, err := sender.Write(payload[:128])
		if err == nil {
			time.Sleep(80 * time.Millisecond)
			_, err = sender.Write(payload[128:])
		}
		sent <- err
		_ = sender.Close()
	}()
	go func() {
		body, err := io.ReadAll(receiver)
		if err != nil {
			received <- nil
			return
		}
		received <- body
		active.Store(0)
	}()
	before := time.Now()
	if !s.waitPublishedHandoverDrain(old, 0, before.Add(time.Second)) {
		t.Fatal("current stream wait rejected")
	}
	if time.Since(before) < 60*time.Millisecond {
		t.Fatal("body owner was not retained")
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if body := <-received; !bytes.Equal(body, payload) {
		t.Fatal("original body truncated or replayed")
	}
}

func TestOptInDrainRealLossStillAbortsExactCarrierWithoutWaiting(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	s.options.PhysicalHandoverDrainGrace = badoption.Duration(5 * time.Second)
	s.closeSnapshotStatsHook = func(*wltpkg.Carrier) wltpkg.CarrierStats { return wltpkg.CarrierStats{ActiveStreams: 1} }
	aborted := make(chan *wltpkg.Carrier, 1)
	s.abortCarrierHook = func(c *wltpkg.Carrier) error { aborted <- c; return nil }
	done := make(chan bool, 1)
	go func() { done <- s.waitPublishedHandoverDrain(old, 0, time.Now().Add(time.Second)) }()
	s.NetworkUnavailable()
	select {
	case c := <-aborted:
		if c != old {
			t.Fatal("wrong generation aborted")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("loss waited for drain")
	}
	select {
	case v := <-done:
		if v {
			t.Fatal("lost generation authorized replacement")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("lost drain worker retained")
	}
}

func TestOptInDrainDeadlineBoundsStalledPublishedBody(t *testing.T) {
	old := &wltpkg.Carrier{}
	s := newConfirmedRetirementService(t, old)
	s.closeSnapshotStatsHook = func(*wltpkg.Carrier) wltpkg.CarrierStats {
		return wltpkg.CarrierStats{ActiveStreams: 1, DrainingStreams: 1}
	}
	begin := time.Now()
	if !s.waitPublishedHandoverDrain(old, 0, begin.Add(60*time.Millisecond)) {
		t.Fatal("deadline did not admit bounded retirement")
	}
	if d := time.Since(begin); d < 50*time.Millisecond || d > 300*time.Millisecond {
		t.Fatalf("unbounded wait: %v", d)
	}
}

func TestOptInDrainStaleReplacementAndStopNeverTouchSuccessor(t *testing.T) {
	for _, kind := range []string{"carrier", "generation", "stop", "context"} {
		t.Run(kind, func(t *testing.T) {
			old := &wltpkg.Carrier{}
			s := newConfirmedRetirementService(t, old)
			switch kind {
			case "carrier":
				s.carrier = &wltpkg.Carrier{}
			case "generation":
				s.interfaceGeneration = 1
			case "stop":
				s.stopped = true
			case "context":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				s.ctx = ctx
			}
			if s.waitPublishedHandoverDrain(old, 0, time.Now().Add(time.Second)) {
				t.Fatal("stale worker authorized restart")
			}
		})
	}
}

func TestOptInDrainAdmissionRejectsReverseAddressUnknownOtherPlatform(t *testing.T) {
	if !allowPublishedHandoverDrain("ios", retirementLTE, retirementWiFi, false, time.Second) {
		t.Fatal("positive opt-in missing")
	}
	for _, c := range []struct {
		os, old, next string
		lost          bool
		grace         time.Duration
	}{
		{"ios", retirementWiFi, retirementLTE, false, time.Second}, {"ios", retirementLTE, retirementWiFi, true, time.Second},
		{"android", retirementLTE, retirementWiFi, false, time.Second}, {"ios", retirementLTE, retirementWiFi, false, 0},
		{"ios", retirementLTE, retirementWiFi, false, 6 * time.Second}, {"ios", "unknown", retirementWiFi, false, time.Second},
		{"ios", retirementLTE, "2|pdp_ip0|cellular|[192.0.2.3/32]", false, time.Second},
	} {
		if allowPublishedHandoverDrain(c.os, c.old, c.next, c.lost, c.grace) {
			t.Fatalf("untrusted drain %+v", c)
		}
	}
}

func TestOptInDrainOptionBoundsAlsoRejectDirectStructConfiguration(t *testing.T) {
	for _, grace := range []time.Duration{-time.Second, 500 * time.Millisecond, 6 * time.Second} {
		_, err := NewService(context.Background(), nil, "wlt", option.WLTServiceOptions{PhysicalHandoverDrainGrace: badoption.Duration(grace)})
		if err == nil {
			t.Fatal("invalid grace admitted")
		}
	}
}
