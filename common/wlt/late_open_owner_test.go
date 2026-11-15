//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestLateOpenCarrierAbortJoinsRetirementOwner(t *testing.T) {
	for _, kind := range []string{"live", "blocked", "injected"} {
		t.Run(kind, func(t *testing.T) {
			c := newTestCarrier(2, 2, 2, time.Second, time.Second)
			c.done = make(chan struct{})
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.cancel = cancel
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			expected := errors.New("late notification failure")
			conn := &parentRetirementConn{Conn: a, parent: parent, blocked: kind == "blocked"}
			if kind == "injected" {
				conn.injected = expected
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			c.dialRoute = func(context.Context, string) (net.Conn, error) { close(entered); <-release; return conn, nil }
			dial := make(chan error, 1)
			go func() {
				stream, err := c.DialStream(context.Background(), "eu", "example.com:443")
				if stream != nil {
					stream.Close()
				}
				dial <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("dial not entered")
			}
			aborted := make(chan error, 1)
			go func() { aborted <- c.Abort() }()
			select {
			case <-c.done:
			case <-time.After(time.Second):
				t.Fatal("publication not sealed")
			}
			close(release)
			var dialErr, abortErr error
			select {
			case dialErr = <-dial:
			case <-time.After(time.Second):
				t.Fatal("late OPEN not joined")
			}
			select {
			case abortErr = <-aborted:
			case <-time.After(time.Second):
				t.Fatal("Abort not joined")
			}
			if !errors.Is(dialErr, net.ErrClosed) {
				t.Fatalf("late stream published: %v", dialErr)
			}
			switch kind {
			case "live":
				if abortErr != nil || errors.Is(dialErr, context.Canceled) {
					t.Fatalf("parent canceled before late retirement: dial=%v abort=%v", dialErr, abortErr)
				}
			case "blocked":
				if !errors.Is(abortErr, context.Canceled) || !errors.Is(dialErr, context.Canceled) {
					t.Fatalf("blocked owner errors lost: dial=%v abort=%v", dialErr, abortErr)
				}
			case "injected":
				if !errors.Is(abortErr, expected) || !errors.Is(dialErr, expected) {
					t.Fatalf("notification errors lost: dial=%v abort=%v", dialErr, abortErr)
				}
			}
		})
	}
}
