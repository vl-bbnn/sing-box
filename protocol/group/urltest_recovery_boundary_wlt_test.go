//go:build with_wlt

package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func recoveryBoundaryGroup(tag string, members ...adapter.Outbound) *URLTest {
	history := urltest.NewHistoryStorage()
	for i, member := range members {
		history.StoreURLTestHistory(RealTag(member), &adapter.URLTestHistory{Time: time.Now(), Delay: uint16(10 + i)})
	}
	return &URLTest{
		Adapter: outbound.NewAdapter("urltest", tag, []string{N.NetworkTCP, N.NetworkUDP}, nil),
		logger:  log.NewNOPFactory().Logger(),
		group: &URLTestGroup{
			outbounds: members, history: history, preferFirstAvailable: true,
			selectedOutboundTCP: members[0], selectedOutboundUDP: members[0],
			interruptGroup: interrupt.NewGroup(),
		},
	}
}

func recoveryBoundaryDial(t *testing.T, o *URLTest, network string) error {
	t.Helper()
	// The outer request deliberately remains live, as during runtime reload.
	// An inner cancellation/recovery result must be respected on its own.
	if network == "packet" {
		conn, err := o.ListenPacket(context.Background(), M.ParseSocksaddr("example.com:443"))
		if conn != nil {
			conn.Close()
		}
		return err
	}
	conn, err := o.DialContext(context.Background(), network, M.ParseSocksaddr("example.com:443"))
	if conn != nil {
		conn.Close()
	}
	return err
}

func TestURLTestLocalRecoveryDoesNotEscapeThroughNestedGroup(t *testing.T) {
	for name, cause := range map[string]error{
		"service-stop":            errors.Join(errors.New("wlt service is stopped"), context.Canceled),
		"recovery-cancel":         &wltpkg.CarrierRecoveryError{Err: context.Canceled},
		"recovery-deadline":       &wltpkg.CarrierRecoveryError{Err: context.DeadlineExceeded},
		"recovery-remote-failure": &wltpkg.CarrierRecoveryError{Err: errors.New("replacement carrier failed")},
	} {
		for _, network := range []string{N.NetworkTCP, N.NetworkUDP, "packet"} {
			t.Run(name+"/"+network, func(t *testing.T) {
				primary := &startupFallbackOutbound{tag: "saved-wlt", err: fmt.Errorf("nested transport: %w", cause)}
				innerFallback := &startupFallbackOutbound{tag: "ordinary-inner"}
				inner := recoveryBoundaryGroup("inner", primary, innerFallback)
				outerFallback := &startupFallbackOutbound{tag: "ordinary-outer"}
				outer := recoveryBoundaryGroup("outer", inner, outerFallback)
				err := recoveryBoundaryDial(t, outer, network)
				if !errors.Is(err, cause) || primary.calls != 1 || innerFallback.calls != 0 || outerFallback.calls != 0 {
					t.Fatalf("terminal dial escaped: err=%v calls=%d/%d/%d", err, primary.calls, innerFallback.calls, outerFallback.calls)
				}
				for _, group := range []*URLTest{inner, outer} {
					if group.group.history.LoadURLTestHistory(primary.Tag()) == nil || group.group.selectedOutboundTCP != group.group.outbounds[0] || group.group.selectedOutboundUDP != group.group.outbounds[0] {
						t.Fatal("local lifecycle failure invalidated health or route selection")
					}
				}
			})
		}
	}
}

func TestURLTestFallbackRecoveryKeepsItsHealthAndStopsOuterRetry(t *testing.T) {
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP, "packet"} {
		t.Run(network, func(t *testing.T) {
			primary := &startupFallbackOutbound{tag: "ordinary", err: errors.New("gateway unreachable")}
			cause := &wltpkg.CarrierRecoveryError{Err: context.DeadlineExceeded}
			fallback := &startupFallbackOutbound{tag: "saved-wlt", err: cause}
			inner := recoveryBoundaryGroup("inner", primary, fallback)
			outerFallback := &startupFallbackOutbound{tag: "outer-upstream"}
			outer := recoveryBoundaryGroup("outer", inner, outerFallback)
			err := recoveryBoundaryDial(t, outer, network)
			if !errors.Is(err, cause) || primary.calls != 1 || fallback.calls != 1 || outerFallback.calls != 0 {
				t.Fatalf("fallback recovery escaped: err=%v calls=%d/%d/%d", err, primary.calls, fallback.calls, outerFallback.calls)
			}
			if inner.group.history.LoadURLTestHistory(primary.Tag()) != nil || inner.group.history.LoadURLTestHistory(fallback.Tag()) == nil {
				t.Fatal("real route failure and local recovery were not distinguished")
			}
		})
	}
}

func TestURLTestOrdinaryFailureStillUsesSavedAlternative(t *testing.T) {
	for name, cause := range map[string]error{
		"connect-timeout":           context.DeadlineExceeded,
		"remote-eof":                io.EOF,
		"recovery-text-is-not-type": errors.New("wlt pending dial carrier generation retired locally"),
	} {
		for _, network := range []string{N.NetworkTCP, N.NetworkUDP, "packet"} {
			t.Run(name+"/"+network, func(t *testing.T) {
				primary := &startupFallbackOutbound{tag: "ordinary", err: cause}
				fallback := &startupFallbackOutbound{tag: "saved-wlt"}
				group := recoveryBoundaryGroup("merged", primary, fallback)
				if err := recoveryBoundaryDial(t, group, network); err != nil || primary.calls != 1 || fallback.calls != 1 {
					t.Fatalf("ordinary fallback suppressed: err=%v calls=%d/%d", err, primary.calls, fallback.calls)
				}
				if group.group.history.LoadURLTestHistory(primary.Tag()) != nil {
					t.Fatal("actual route failure did not invalidate health")
				}
			})
		}
	}
}
