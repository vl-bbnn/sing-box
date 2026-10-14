//go:build with_wlt && wlt_probe_worker_observer

package libbox

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type wltObservedOriginalCloseGate struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	closed  atomic.Bool
	result  error
}

func (c *wltObservedOriginalCloseGate) Close() error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	c.once.Do(func() { c.result = c.Conn.Close(); c.closed.Store(true) })
	return c.result
}

func TestWLTProbeWorkerObserverSeparatesRealBodyFromUnfinishedClose(t *testing.T) {
	ctx, address, closeServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer closeServer()
	events := make(chan wltProbeWorkerEvent, 24)
	observer := &wltProbeWorkerObserver{hash: strings.Repeat("a", 64), started: time.Now(), sink: func(e wltProbeWorkerEvent) { events <- e }}
	ctx = context.WithValue(ctx, wltProbeWorkerObserverKey{}, observer)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var conn *wltObservedOriginalCloseGate
	resolver, group, leaf, wlt := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		conn = &wltObservedOriginalCloseGate{Conn: raw, entered: entered, release: release}
		return conn, nil
	})
	request := wltHTTPSRequest() // Keep the original fixture's request and deadline.
	key := new(int)
	returned := make(chan string, 1)
	go func() {
		encoded, err := executeWLTProbe(key, ctx, resolver, func() bool { return true }, request, observer.hash)
		if err != nil {
			returned <- "unexpected error"
		} else {
			returned <- encoded
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("original HTTPS body/Close did not reach its actual owner")
	}
	var result wltProbeResult
	select {
	case encoded := <-returned:
		if err := json.Unmarshal([]byte(encoded), &result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original bounded outer result did not return")
	}
	if result.Status != "failed" || result.ErrorCode != "timeout_cleanup_pending" || conn.closed.Load() {
		t.Fatalf("pending original Close was forgiven: %+v closed=%t", result, conn.closed.Load())
	}
	if _, busy := wltProbeBusy.Load(key); !busy {
		t.Fatal("original worker lost its busy ownership before actual Close completion")
	}
	if group.dialCount.Load() != 0 || wlt.dialCount.Load() != 0 || leaf.dialCount.Load() != 1 {
		t.Fatal("observer changed the original concrete dial path")
	}
	releaseOnce.Do(func() { close(release) })
	deadline := time.Now().Add(time.Second)
	for {
		_, busy := wltProbeBusy.Load(key)
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actual old Close/worker completion not joined")
		}
		time.Sleep(time.Millisecond)
	}
	var rows []wltProbeWorkerEvent
	for len(events) != 0 {
		rows = append(rows, <-events)
	}
	var before, pending, after bool
	for _, row := range rows {
		if row.RequestHash != observer.hash {
			t.Fatal("request owner hash changed")
		}
		switch row.Event {
		case "socket_close_start":
			before = row.Status == "success" && row.HTTPStatus == 204 && row.BytesRead == 0 && row.ElapsedMS < int64(request.TimeoutMS)
		case "outer_timeout_pending":
			pending = row.ErrorCode == "timeout_cleanup_pending"
		case "worker_complete":
			after = row.Status == "failed" && row.ErrorCode == "timeout" && row.HTTPStatus == 204
		}
	}
	if !before || !pending || !after || !conn.closed.Load() {
		t.Fatalf("original body, outer timeout and actual worker result not distinguished: %+v", rows)
	}
}

func TestWLTProbeWorkerObserverRetainsErrorAndRedactsOpaqueInput(t *testing.T) {
	events := make(chan wltProbeWorkerEvent, 24)
	ctx := context.WithValue(context.Background(), wltProbeWorkerObserverKey{}, &wltProbeWorkerObserver{
		hash: "private-request", started: time.Now(), sink: func(e wltProbeWorkerEvent) { events <- e },
	})
	sentinel := errors.New("private-close-error")
	result := wltProbeResult{Status: "failed", ErrorCode: "https_close_failed"}
	observeWLTProbeWorker(ctx, "socket_close_return", result, sentinel)
	row := <-events
	if row.RequestHash != "invalid" || row.ErrorClass != "other" || row.ErrorCode != result.ErrorCode || result.Status != "failed" {
		t.Fatalf("observer changed/printed opaque original error or result: %+v", row)
	}
}
