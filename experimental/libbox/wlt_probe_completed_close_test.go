//go:build with_wlt

package libbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

// Real TLS/HTTP IO reaches the selected socket. Close and Abort deliberately
// differ so successful response disposal cannot be inferred from HTTP status.
type completedProbeCloseConn struct {
	net.Conn
	closeCount  atomic.Int32
	abortCount  atomic.Int32
	closeErr    error
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func (c *completedProbeCloseConn) Close() error {
	c.closeCount.Add(1)
	if c.entered != nil {
		select {
		case c.entered <- struct{}{}:
		default:
		}
	}
	if c.release != nil {
		<-c.release
	}
	return errors.Join(c.Conn.Close(), c.closeErr)
}
func (c *completedProbeCloseConn) Abort() error {
	c.abortCount.Add(1)
	if c.release != nil {
		c.releaseOnce.Do(func() { close(c.release) })
	}
	return c.Conn.Close()
}

func TestCompletedProbeTLSHTTPDisposal(t *testing.T) {
	for _, kind := range []string{"204", "fixed-body", "chunked-complete", "chunked-read-limit", "wrong-status", "close-error"} {
		t.Run(kind, func(t *testing.T) {
			request := wltHTTPSRequest()
			if strings.Contains(kind, "body") || strings.HasPrefix(kind, "chunked") {
				request.ExpectedStatus = 200
				request.ExpectedBytes = wltProbeBulkBytes
				request.MaxReadBytes = wltProbeBulkBytes + 1
			}
			if kind == "chunked-read-limit" {
				request.MaxReadBytes = wltProbeBulkBytes
			}
			ctx, address, stopServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if kind == "wrong-status" {
					w.WriteHeader(503)
					return
				}
				if request.ExpectedBytes == 0 {
					w.WriteHeader(204)
					return
				}
				if kind == "fixed-body" {
					w.Header().Set("Content-Length", fmt.Sprint(wltProbeBulkBytes))
				}
				w.WriteHeader(200)
				if strings.HasPrefix(kind, "chunked") {
					w.(http.Flusher).Flush()
				}
				amount := wltProbeBulkBytes
				if kind == "chunked-read-limit" {
					amount += 4096
				}
				_, _ = w.Write(make([]byte, amount))
			}))
			defer stopServer()
			var selected *completedProbeCloseConn
			resolver, group, leaf, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
				raw, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				selected = &completedProbeCloseConn{Conn: raw}
				if kind == "close-error" {
					selected.closeErr = errors.New("private real close error must not leak")
				}
				return selected, nil
			})
			result := executeWLTProbeForTest(t, ctx, new(int), resolver, request)
			if selected == nil || leaf.dialCount.Load() != 1 || group.dialCount.Load() != 0 {
				t.Fatal("wrong socket ownership")
			}
			abort := kind == "chunked-read-limit" || kind == "wrong-status"
			if abort {
				if selected.abortCount.Load() != 1 || selected.closeCount.Load() != 0 {
					t.Fatalf("unread/error response disposal: close=%d abort=%d", selected.closeCount.Load(), selected.abortCount.Load())
				}
			} else if selected.closeCount.Load() != 1 || selected.abortCount.Load() != 0 {
				t.Fatalf("completed response aborted: close=%d abort=%d", selected.closeCount.Load(), selected.abortCount.Load())
			}
			if kind == "close-error" {
				if result.Status != "failed" || result.ErrorCode != "https_close_failed" {
					t.Fatalf("real Close failure hidden: %+v", result)
				}
			} else if kind == "wrong-status" {
				if result.Status != "failed" || result.ErrorCode != "https_status_mismatch" {
					t.Fatalf("wrong status forgiven: %+v", result)
				}
			} else if result.Status != "success" || result.BytesRead == nil || *result.BytesRead != request.ExpectedBytes {
				t.Fatalf("wrong strict body result: %+v", result)
			}
			if strings.Contains(fmt.Sprint(result), "private real close error") {
				t.Fatal("raw close error leaked")
			}
		})
	}
}

func TestCompletedProbeTLSHTTPOriginalDeadlineInterruptsClose(t *testing.T) {
	ctx, address, stopServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer stopServer()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var selected *completedProbeCloseConn
	resolver, _, _, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		raw, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		selected = &completedProbeCloseConn{Conn: raw, entered: entered, release: release}
		return selected, nil
	})
	request := wltHTTPSRequest()
	request.TimeoutMS = 150
	key := new(int)
	done := make(chan wltProbeResult, 1)
	go func() { done <- executeWLTProbeForTest(t, ctx, key, resolver, request) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("graceful close was never entered")
	}
	select {
	case result := <-done:
		if result.Status != "failed" || !strings.HasPrefix(result.ErrorCode, "timeout") {
			t.Fatalf("original deadline became late success: %+v", result)
		}
	case <-time.After(time.Second):
		selected.releaseOnce.Do(func() { close(release) })
		t.Fatal("original context failed to retire close")
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, busy := wltProbeBusy.Load(key)
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned worker did not finish cleanup")
		}
		time.Sleep(time.Millisecond)
	}
	if selected.closeCount.Load() != 1 || selected.abortCount.Load() != 1 {
		t.Fatalf("original close/interrupt ownership lost: close=%d abort=%d", selected.closeCount.Load(), selected.abortCount.Load())
	}
}
