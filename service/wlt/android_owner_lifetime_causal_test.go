//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// One exact local retirement; only owner lifetime differs. No published bytes
// are replayed, no transport error or shutdown predicate is relaxed.
func TestAndroidHandoverOwnerLifetimeCausalComparison(t *testing.T) {
	for _, teardown := range []bool{false, true} {
		name := "preserve_service"
		if teardown {
			name = "quiesce_old_service"
		}
		t.Run(name, func(t *testing.T) {
			s, a := newPendingDialTestService(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			stream := &dialTestStream{}
			s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
				calls.Add(1)
				if c == a {
					close(entered)
					<-ctx.Done()
					<-release
					return nil, ctx.Err()
				}
				return stream, nil
			}
			caller, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := dialAsync(s, caller)
			<-entered
			s.NetworkUnavailable()
			var drain chan error
			if teardown {
				drain = make(chan error, 1)
				go func() { drain <- s.QuiescePendingDials(context.Background()) }()
				awaitQuiescing(t, s)
			} else {
				publishDialReplacement(t, s, &wltpkg.Carrier{})
			}
			close(release)
			got := awaitDialResult(t, result)
			if teardown {
				if got.stream != nil || !errors.Is(got.err, context.Canceled) || calls.Load() != 1 {
					t.Fatalf("terminal owner gained retry: %+v calls=%d", got, calls.Load())
				}
				select {
				case err := <-drain:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("quiesce leaked")
				}
				t.Log("local retirement + old owner quiesce: pending OPEN failed=context_canceled; replacement owner cannot inherit it")
			} else {
				if got.err != nil || got.stream != stream || calls.Load() != 2 {
					t.Fatalf("in-Service recovery failed: %+v calls=%d", got, calls.Load())
				}
				t.Log("same local retirement + preserved owner: pending OPEN recovered on replacement carrier")
			}
			assertNoDialLeases(t, s)
		})
	}
}
