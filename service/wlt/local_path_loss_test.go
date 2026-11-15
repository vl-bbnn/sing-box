//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"io"
	"testing"
)

// NetworkUnavailable occurs before the real control-reader result returns,
// exactly the order which rejected the observed dial74 despite local loss.
func TestPendingOpenProvedLocalPathLossRecoversAfterAlreadyRetired(t *testing.T) {
	s, a := newPendingDialTestService(t)
	b := &wltpkg.Carrier{}
	fresh := &dialTestStream{}
	entered := make(chan dialTestAttempt, 2)
	release := make(chan struct{})
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		entered <- dialTestAttempt{ctx, c}
		if c == a {
			<-release
			return nil, carriercommon.BindPendingOpenPathLoss(ctx, fmt.Errorf("control: %w", &carriercommon.PeerPathLossError{Peers: 2}))
		}
		return fresh, nil
	}
	result := dialAsync(s, context.Background())
	awaitDialAttempt(t, entered)
	s.NetworkUnavailable()
	publishDialReplacement(t, s, b)
	close(release)
	if second := awaitDialAttempt(t, entered); second.carrier != b {
		t.Fatal("old carrier retried")
	}
	if got := awaitDialResult(t, result); got.err != nil || got.stream != fresh {
		t.Fatalf("recovery: %+v", got)
	}
	assertNoDialLeases(t, s)
}

func TestPendingOpenLocalPathLossRejectsWrongBindingOrMixedCause(t *testing.T) {
	for _, kind := range []string{"attempt", "logical", "carrier", "interface", "mixed", "unproved"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newPendingDialTestService(t)
			entered := make(chan dialTestAttempt, 2)
			release := make(chan struct{})
			var original error
			s.dialStreamHook = func(ctx context.Context, _ *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
				entered <- dialTestAttempt{ctx, nil}
				<-release
				d := carriercommon.OpenDiagnosticFromContext(ctx)
				leaf := error(&carriercommon.PeerPathLossError{Peers: 2})
				switch kind {
				case "attempt":
					d.DialAttemptID++
				case "logical":
					d.LogicalDialID++
				case "carrier":
					d.CarrierGeneration++
				case "interface":
					d.InterfaceGeneration++
				case "mixed":
					leaf = errors.Join(leaf, errors.New("provider failure"))
				case "unproved":
					leaf = io.EOF
				}
				original = &carriercommon.PendingOpenPathLoss{Diagnostic: d, Err: leaf}
				return nil, original
			}
			result := dialAsync(s, context.Background())
			awaitDialAttempt(t, entered)
			s.NetworkUnavailable()
			close(release)
			got := awaitDialResult(t, result)
			if got.stream != nil || got.err != original {
				t.Fatalf("wrong proof accepted: %+v", got)
			}
			select {
			case <-entered:
				t.Fatal("unproved retry")
			default:
			}
			assertNoDialLeases(t, s)
		})
	}
}
