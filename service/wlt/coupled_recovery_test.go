//go:build with_wlt && wlt_recovery_fixture

package wlt

import (
	"context"
	"errors"
	"fmt"
	"github.com/sagernet/sing-box/adapter"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/log"
	boxvless "github.com/sagernet/sing-box/protocol/vless"
	singmux "github.com/sagernet/sing-mux"
	vmessvless "github.com/sagernet/sing-vmess/vless"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/vl-bbnn/wlt-carrier/pkg/engine"
	"github.com/vl-bbnn/wlt-carrier/pkg/recoveryfixture"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNestedVLESSActualCarrierOriginalBusyHTTPBody(t *testing.T) {
	rootT := t
	recoveryfixture.Run(t, func(t *testing.T, handler recoveryfixture.Handler) recoveryfixture.Owner {
		s := newLifecycleTestService(rootT)
		client := engine.AdoptRecoveryFixture(handler)
		a := wltpkg.AdoptRecoveryFixture(client)
		s.carrier, s.carrierGeneration, s.initialized, s.interfaceKey = a, 1, true, "lte"
		closeSignal(s.carrierReady)
		// No scripted suspend/dial/abort/start hook is installed.
		upstream := &nestedFixtureUpstream{service: s, t: t}
		nested, err := boxvless.AdoptNestedRecoveryFixture(upstream)
		if err != nil {
			t.Fatal(err)
		}
		rootT.Cleanup(func() { _ = nested.Close() })
		var originalNestedSessions []any
		var cycle int
		return recoveryfixture.Owner{
			Open: func(ctx context.Context, _ byte) (net.Conn, error) {
				conn, err := nested.DialContext(ctx, "tcp", M.ParseSocksaddr("example.com:443"))
				if err == nil {
					originalNestedSessions = nested.RecoveryFixtureSessions()
					if len(originalNestedSessions) != 1 {
						t.Fatal("nested mux did not establish exactly one session")
					}
				}
				return conn, err
			},
			Terminal: upstream.abort,
			Server:   func(ctx context.Context, raw net.Conn) (net.Conn, error) { return unwrapNestedFixture(t, ctx, raw) },
			Suspend: func() wltpkg.NetworkRecovery {
				s.NetworkUnavailable()
				nested.InterfaceUpdated()
				s.access.RLock()
				defer s.access.RUnlock()
				h := s.networkHold
				if h == nil || h.carrier != a || !s.interfaceUnavailable {
					t.Fatal("actual core did not retain original carrier")
				}
				if interfaceGateOpenUnlocked(s) {
					t.Fatal("unavailable core admission left open")
				}
				return h.recovery
			},
			Resume: func(ctx context.Context) error {
				cycle++
				s.interfaceUpdated(fmt.Sprintf("fixture-network-%d", cycle), "android", time.Second)
				for !interfaceGateOpen(s) {
					s.access.RLock()
					failed := s.retentionRejectedGeneration == 1 || s.carrier != a
					s.access.RUnlock()
					if failed {
						return fmt.Errorf("core original hold terminal rejection cycle=%d", cycle)
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Millisecond):
					}
				}
				nested.InterfaceUpdated()
				// Android may emit a second settled callback for the same network.
				s.interfaceUpdated(fmt.Sprintf("fixture-network-%d", cycle), "android", time.Millisecond)
				nested.InterfaceUpdated()
				return nil
			},
			Verify: func(t *testing.T) {
				currentNested := nested.RecoveryFixtureSessions()
				if len(currentNested) != len(originalNestedSessions) || currentNested[0] != originalNestedSessions[0] {
					t.Fatal("nested VLESS mux session replaced or reset")
				}
				s.access.RLock()
				defer s.access.RUnlock()
				if s.carrier != a || s.carrierGeneration != 1 || s.retainedCarrier != a || s.interfaceUnavailable || s.networkHold != nil {
					t.Fatal("core lost exact original carrier/session ownership")
				}
			},
		}
	})
}
func interfaceGateOpenUnlocked(s *Service) bool {
	select {
	case <-s.interfaceReady:
		return !s.interfaceUnavailable
	default:
		return false
	}
}

type nestedFixtureUpstream struct {
	adapter.Outbound
	service *Service
	t       *testing.T
	access  sync.Mutex
	streams []net.Conn
}

func (u *nestedFixtureUpstream) WLTNetworkResetRetained() bool {
	return u.service.WLTNetworkResetRetained()
}
func (u *nestedFixtureUpstream) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	stream, err := u.service.DialStream(ctx, "eu", "192.0.2.1:443")
	if err != nil {
		return nil, err
	}
	u.access.Lock()
	u.streams = append(u.streams, stream.(net.Conn))
	u.access.Unlock()
	u.t.Cleanup(u.abort)
	return stream.(net.Conn), nil
}
func (u *nestedFixtureUpstream) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("fixture UDP unsupported")
}
func unwrapNestedFixture(t *testing.T, ctx context.Context, raw net.Conn) (net.Conn, error) {
	t.Helper()
	logger := log.NewNOPFactory().Logger()
	accepted := make(chan net.Conn, 2)
	errorsCh := make(chan error, 2)
	appHandler := adapter.NewUpstreamContextHandlerEx(
		func(_ context.Context, c net.Conn, m adapter.InboundContext, _ N.CloseHandlerFunc) {
			if m.Destination.String() != "example.com:443" {
				errorsCh <- errors.New("nested original destination mismatch")
				return
			}
			accepted <- c
		}, func(context.Context, N.PacketConn, adapter.InboundContext, N.CloseHandlerFunc) {
			errorsCh <- errors.New("unexpected nested UDP")
		})
	muxService, err := singmux.NewService(singmux.ServiceOptions{Logger: logger, HandlerEx: appHandler, NewStreamContext: func(ctx context.Context, _ net.Conn) context.Context { return ctx }})
	if err != nil {
		return nil, err
	}
	vlessHandler := adapter.NewUpstreamContextHandlerEx(
		func(ctx context.Context, c net.Conn, m adapter.InboundContext, onClose N.CloseHandlerFunc) {
			if m.Destination != singmux.Destination {
				errorsCh <- errors.New("VLESS did not carry actual nested mux destination")
				return
			}
			muxService.NewConnectionEx(ctx, c, m.Source, m.Destination, onClose)
		}, func(context.Context, N.PacketConn, adapter.InboundContext, N.CloseHandlerFunc) {
			errorsCh <- errors.New("unexpected VLESS UDP")
		})
	service := vmessvless.NewService[int](logger, vlessHandler)
	service.UpdateUsers([]int{1}, []string{"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"}, []string{""})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := service.NewConnection(ctx, raw, M.Socksaddr{}, nil); err != nil {
			select {
			case errorsCh <- err:
			default:
			}
		}
	}()
	t.Cleanup(func() {
		_ = raw.(interface{ Abort() error }).Abort()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("nested VLESS/mux server worker not joined")
		}
	})
	select {
	case c := <-accepted:
		return c, nil
	case err := <-errorsCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (u *nestedFixtureUpstream) abort() {
	u.access.Lock()
	streams := append([]net.Conn(nil), u.streams...)
	u.access.Unlock()
	for _, stream := range streams {
		_ = stream.(interface{ Abort() error }).Abort()
	}
}
