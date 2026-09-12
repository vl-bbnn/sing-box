//go:build with_wlt

package wlt

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
)

func TestDarwinLossRecoveryStartsWithinPathSettleWindow(t *testing.T) {
	s := newLifecycleTestService(t)
	old, replacement := &wltpkg.Carrier{}, &wltpkg.Carrier{}
	calls := 0
	ready := make(chan struct{})
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) {
		calls++
		if calls == 1 {
			return old, nil
		}
		return replacement, nil
	}
	if err := s.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal(err)
	}
	s.abortCarrierHook = func(*wltpkg.Carrier) error { return nil }
	s.markCarrierReadyHook = func(*wltpkg.Carrier) { close(ready) }
	s.NetworkUnavailable()
	s.interfaceUpdated("12|new", "darwin", interfaceRecoveryGraceFor("darwin"))
	select {
	case <-ready:
		t.Fatal("replacement skipped physical-path settle")
	case <-time.After(100 * time.Millisecond):
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := s.WaitCarrier(ctx)
	if err != nil || got != replacement {
		t.Fatalf("recovery got=%p err=%v", got, err)
	}
	<-ready
}
