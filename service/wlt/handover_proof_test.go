//go:build with_wlt

package wlt

import (
	"context"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"testing"
	"time"
)

func retainedProofFixture() *Service {
	a := &wltpkg.Carrier{}
	return &Service{ctx: context.Background(), carrier: a, carrierGeneration: 1, retainedCarrier: a, retainedCarrierGeneration: 1, interfaceGeneration: 3, interfaceKey: "5|wlan0|wifi|[192.0.2.2/32]", interfaceReady: closedSignal()}
}
func TestRetainedHandoverProofRejectsStartupWithoutRecovery(t *testing.T) {
	s := retainedProofFixture()
	s.retainedCarrier = nil
	if proof, err := s.BeginWltHandoverProof(context.Background(), "wlan0", 5); err == nil || proof != nil {
		t.Fatal("cached startup carrier authorized retention")
	}
}
func TestRetainedHandoverProofRejectsEveryOwnershipChange(t *testing.T) {
	tests := map[string]func(*Service){
		"carrier": func(s *Service) { s.carrier = &wltpkg.Carrier{} }, "carrier-generation": func(s *Service) { s.carrierGeneration++ }, "interface-generation-ABA": func(s *Service) { s.interfaceGeneration++ }, "interface-key": func(s *Service) { s.interfaceKey = "6|rmnet0|cellular|[]" }, "unavailable": func(s *Service) { s.interfaceUnavailable = true }, "hold-expired": func(s *Service) { s.retentionRejectedGeneration = 1 }, "stopped": func(s *Service) { s.stopped = true }, "quiescing": func(s *Service) { s.quiescing = true }, "gate-closed": func(s *Service) { s.interfaceReady = make(chan struct{}) }, "retention-lost": func(s *Service) { s.retainedCarrier = nil },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			s := retainedProofFixture()
			proof, err := s.BeginWltHandoverProof(context.Background(), "wlan0", 5)
			if err != nil || !proof.Current() {
				t.Fatal("current proof not acquired", err)
			}
			s.access.Lock()
			change(s)
			s.access.Unlock()
			if proof.Current() {
				t.Fatal("obsolete ownership accepted")
			}
		})
	}
}
func TestRetainedHandoverProofScopeAndInterfaceAreMandatory(t *testing.T) {
	s := retainedProofFixture()
	for _, args := range []struct {
		name  string
		index int
	}{{"", 5}, {"wlan0", 0}, {"rmnet0", 5}, {"wlan0", 6}} {
		if proof, err := s.BeginWltHandoverProof(context.Background(), args.name, args.index); err == nil || proof != nil {
			t.Fatal("wrong interface accepted", args)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	proof, err := s.BeginWltHandoverProof(ctx, "wlan0", 5)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if proof.Current() {
		t.Fatal("canceled proof remains usable")
	}
	if proof, err = s.BeginWltHandoverProof(ctx, "wlan0", 5); err == nil || proof != nil {
		t.Fatal("canceled check acquired")
	}
}
func TestRetainedHandoverProofWaitsForExactResume(t *testing.T) {
	s, a, h := newHoldFixture(t, time.Second)
	s.NetworkUnavailable()
	s.interfaceUpdated("5|wlan0|wifi|[192.0.2.2/32]", "android", time.Second)
	resume := awaitResume(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type result struct {
		proof wltpkg.HandoverProof
		err   error
	}
	done := make(chan result, 1)
	go func() { p, e := s.BeginWltHandoverProof(ctx, "wlan0", 5); done <- result{p, e} }()
	select {
	case got := <-done:
		t.Fatal("proof returned before actual same-session completion", got.err)
	case <-time.After(time.Millisecond):
	}
	resume.reply <- nil
	got := <-done
	if got.err != nil || got.proof == nil || !got.proof.Current() || s.carrier != a {
		t.Fatal("exact original completion not accepted", got.err)
	}
}
