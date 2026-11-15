//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnprovedEarlyFailureCannotGainAuthorityFromLaterCallback(t *testing.T) {
	for _, original := range []error{io.EOF, io.ErrUnexpectedEOF, io.ErrClosedPipe, net.ErrClosed, errors.Join(io.EOF, errors.New("provider rejected"))} {
		t.Run(original.Error(), func(t *testing.T) {
			s, a := newPendingDialTestService(t)
			failed := make(chan struct{})
			callback := make(chan struct{})
			var calls atomic.Int32
			s.dialStreamHook = func(ctx context.Context, c *wltpkg.Carrier, _, _ string) (io.ReadWriteCloser, error) {
				calls.Add(1)
				if c == a {
					close(failed)
					return nil, original
				}
				return &dialTestStream{}, nil
			}
			go func() {
				<-failed
				time.Sleep(20 * time.Millisecond)
				s.NetworkUnavailable()
				publishDialReplacement(t, s, &wltpkg.Carrier{})
				close(callback)
			}()
			got, err := s.DialStream(context.Background(), "eu", "example.com:443")
			<-callback
			if got != nil || err != original || calls.Load() != 1 {
				t.Fatalf("unproved early failure retried: stream=%v error=%v calls=%d", got, err, calls.Load())
			}
			assertNoDialLeases(t, s)
		})
	}
}
