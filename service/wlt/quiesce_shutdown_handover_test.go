//go:build with_wlt

package wlt

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
)

type shutdownTestNetwork struct {
	adapter.NetworkManager
	current *adapter.NetworkInterface
}

func (n *shutdownTestNetwork) DefaultNetworkInterface() *adapter.NetworkInterface { return n.current }

func TestHandoverShutdownRetiresBeforeParentCancellation(t *testing.T) {
	for _, notified := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_delayed_callback", true: "after_interface_callback"}[notified], func(t *testing.T) {
			s := newLifecycleTestService(t)
			network := &shutdownTestNetwork{current: &adapter.NetworkInterface{}}
			network.current.Index = 10
			network.current.Name = "cellular0"
			s.network = network
			carrier := &wltpkg.Carrier{}
			s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return carrier, nil }
			var aborted atomic.Bool
			s.abortCarrierHook = func(c *wltpkg.Carrier) error {
				if c != carrier {
					t.Error("wrong carrier retired")
				}
				aborted.Store(true)
				return nil
			}
			if err := s.Start(adapter.StartStateInitialize); err != nil {
				t.Fatal(err)
			}
			s.carrierStartCancel = func() {
				if !aborted.Load() {
					t.Error("carrier context canceled before obsolete carrier abort")
				}
			}
			network.current = &adapter.NetworkInterface{}
			network.current.Index = 11
			network.current.Name = "wifi0"
			if notified {
				s.beginInterfaceUpdate(s.currentInterfaceKey(), "android")
			}
			if err := s.QuiescePendingDials(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !aborted.Load() {
				t.Fatal("obsolete carrier was not retired before parent cancellation")
			}
			if !s.quiescing {
				t.Fatal("shutdown did not close admission")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOrdinaryShutdownKeepsGracefulCarrierClose(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "duplicate_callback"}[duplicate], func(t *testing.T) {
			s := newLifecycleTestService(t)
			n := &shutdownTestNetwork{current: &adapter.NetworkInterface{}}
			n.current.Index = 10
			n.current.Name = "cellular0"
			s.network = n
			s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { return &wltpkg.Carrier{}, nil }
			aborted := false
			s.abortCarrierHook = func(*wltpkg.Carrier) error { aborted = true; return nil }
			if err := s.Start(adapter.StartStateInitialize); err != nil {
				t.Fatal(err)
			}
			if duplicate {
				s.beginInterfaceUpdate(s.currentInterfaceKey(), "android")
			}
			if err := s.QuiescePendingDials(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if aborted {
				t.Fatal("ordinary stop was changed to abort")
			}
		})
	}
}

func TestQuiesceRejectsLateCarrierPublication(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(map[bool]string{false: "restart", true: "initial_start"}[initial], func(t *testing.T) {
			s := newLifecycleTestService(t)
			s.initialized = !initial
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			c := &wltpkg.Carrier{}
			s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) { close(entered); <-release; return c, nil }
			var aborted atomic.Bool
			s.abortCarrierHook = func(got *wltpkg.Carrier) error {
				if got != c {
					t.Error("wrong late carrier")
				}
				aborted.Store(true)
				return nil
			}
			go func() {
				if initial {
					_ = s.Start(adapter.StartStateInitialize)
				} else {
					s.restartCarrierCurrent(nil, "test shutdown")
				}
				close(done)
			}()
			awaitLifecycleEvent(t, entered, "carrier start")
			if err := s.QuiescePendingDials(context.Background()); err != nil {
				t.Fatal(err)
			}
			close(release)
			awaitLifecycleEvent(t, done, "late carrier result")
			if s.Carrier() != nil || !aborted.Load() {
				t.Fatal("quiesced service published a late carrier")
			}
		})
	}
}
