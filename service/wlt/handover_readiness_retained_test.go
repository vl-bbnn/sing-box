//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestReadinessPendingInterfaceDoesNotConsultRetainedCarrier(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	_, _, _, ignored := s.beginInterfaceUpdate("wifi", "android")
	if ignored {
		t.Fatal("handover ignored")
	}
	// The retained placeholder returns os.ErrInvalid if touched. Readiness must
	// instead wait for the same interface gate used by actual dial admission.
	err := s.WaitWltTrafficReady(context.Background(), 20)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("readiness consulted retained obsolete carrier: %v", err)
	}
}
func TestReadinessSettledInterfacePreservesActualCarrierError(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	_, _, generation, ignored := s.beginInterfaceUpdate("wifi", "android")
	if ignored {
		t.Fatal("handover ignored")
	}
	s.finishInterfaceRecovery(generation)
	if err := s.WaitWltTrafficReady(context.Background(), 20); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("settled carrier error hidden: %v", err)
	}
}
func TestReadinessPendingInterfaceHonorsCallerCancellation(t *testing.T) {
	s, _ := newPendingDialTestService(t)
	s.beginInterfaceUpdate("wifi", "android")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.WaitWltTrafficReady(ctx, 20); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancel lost: %v", err)
	}
}
