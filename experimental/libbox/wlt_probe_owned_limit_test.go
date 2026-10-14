//go:build with_wlt

package libbox

import (
	"context"
	"errors"
	M "github.com/sagernet/sing/common/metadata"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

// Only external HTTP scheduling is gated. Actual TLS/body reads, selected
// socket abort and original read limit/results are unchanged.
type probeOwnedLimitConn struct {
	net.Conn
	aborted        atomic.Bool
	postAbortReads atomic.Int64
	aborts         atomic.Int64
	tail           chan struct{}
	once           sync.Once
	abortErr       error
}

func (c *probeOwnedLimitConn) Read(p []byte) (int, error) {
	if c.aborted.Load() {
		c.postAbortReads.Add(1)
	}
	return c.Conn.Read(p)
}
func (c *probeOwnedLimitConn) Abort() error {
	c.aborts.Add(1)
	c.aborted.Store(true)
	actual := c.Conn.Close()
	c.once.Do(func() { close(c.tail) })
	return errors.Join(actual, c.abortErr)
}
func TestWLTProbeOriginalReadLimitDoesNotDrainDisposedSocket(t *testing.T) {
	for _, kind := range []string{"bounded-success", "actual-abort-error"} {
		t.Run(kind, func(t *testing.T) {
			tail := make(chan struct{})
			release := make(chan struct{})
			var released sync.Once
			request := wltHTTPSRequest()
			request.ExpectedStatus = 200
			request.ExpectedBytes = wltProbeBulkBytes
			request.MaxReadBytes = wltProbeBulkBytes
			ctx, address, stopServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				_, _ = w.Write(make([]byte, wltProbeBulkBytes))
				w.(http.Flusher).Flush()
				select {
				case <-tail:
				case <-release:
				case <-r.Context().Done():
				}
				_, _ = w.Write(make([]byte, 4096))
			}))
			defer stopServer()
			defer func() { released.Do(func() { close(release) }) }()
			var selected *probeOwnedLimitConn
			sentinel := errors.New("private original abort close sentinel")
			resolver, group, leaf, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
				raw, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				selected = &probeOwnedLimitConn{Conn: raw, tail: tail}
				if kind == "actual-abort-error" {
					selected.abortErr = sentinel
				}
				return selected, nil
			})
			result := executeWLTProbeForTest(t, ctx, new(int), resolver, request)
			if selected == nil || leaf.dialCount.Load() != 1 || group.dialCount.Load() != 0 {
				t.Fatal("original selected socket/request changed")
			}
			if selected.aborts.Load() != 1 {
				t.Fatal("original single owned abort missing", selected.aborts.Load())
			}
			if selected.postAbortReads.Load() != 0 {
				t.Fatalf("actual original disposed socket read after bounded body: count=%d result=%s/%s", selected.postAbortReads.Load(), result.Status, result.ErrorCode)
			}
			if result.BytesRead == nil || *result.BytesRead != request.ExpectedBytes || result.RequestSHA256 == "" {
				t.Fatal("original body/read-limit/request identity lost", result)
			}
			if kind == "actual-abort-error" {
				if result.Status != "failed" || result.ErrorCode != "https_close_failed" {
					t.Fatal("actual original abort error hidden", result)
				}
			} else if result.Status != "success" {
				t.Fatal("bounded original body converted to failure", result)
			}
		})
	}
}
