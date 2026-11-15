//go:build with_wlt

package wlt

import (
	"context"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"io"
	"testing"
	"time"
)

// A closed underlay can precede the system's network callback. The unpublished
// OPEN must retain the first error but may recover after a confirmed local
// generation retirement; no application bytes have been sent or replayed.
func TestEarlyOpenPrecallbackRecovery(t *testing.T) {
	s, a := newPendingDialTestService(t)
	b, stream := &wltpkg.Carrier{}, &dialTestStream{}
	failed := make(chan struct{})
	attempts := make(chan dialTestAttempt, 2)
	s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
		attempts <- dialTestAttempt{ctx, c}
		if c == a {
			close(failed)
			return nil, carriercommon.BindPendingOpenPathLoss(ctx, &carriercommon.PeerPathLossError{Peers: 2})
		}
		return stream, nil
	}
	deadline := time.Now().Add(900 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	resultCh := dialAsync(s, ctx)
	first := awaitDialAttempt(t, attempts)
	<-failed
	select {
	case result := <-resultCh:
		t.Fatalf("unpublished OPEN returned before physical callback: %v", result.err)
	case <-time.After(20 * time.Millisecond):
	}
	s.NetworkUnavailable()
	publishDialReplacement(t, s, b)
	second := awaitDialAttempt(t, attempts)
	result := awaitDialResult(t, resultCh)
	if result.err != nil || result.stream != stream || first.carrier != a || second.carrier != b {
		t.Fatalf("recovery failed: %+v", result)
	}
	for _, attempt := range []dialTestAttempt{first, second} {
		got, ok := attempt.ctx.Deadline()
		if !ok || !got.Equal(deadline) {
			t.Fatal("caller deadline changed")
		}
	}
	assertNoDialLeases(t, s)
	if stream.reads.Load() != 0 || stream.writes.Load() != 0 || stream.aborted.Load() != 0 {
		t.Fatal("application stream replayed")
	}
}
