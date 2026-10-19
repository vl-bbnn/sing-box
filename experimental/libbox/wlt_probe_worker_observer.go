//go:build with_wlt && wlt_probe_worker_observer

package libbox

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	L "github.com/sagernet/sing-box/log"
)

type wltProbeWorkerObserverKey struct{}

type wltProbeWorkerEvent struct {
	Event       string
	RequestHash string
	ElapsedMS   int64
	Status      string
	ErrorCode   string
	HTTPStatus  int
	BytesRead   int64
	ErrorClass  string
}

type wltProbeWorkerObserver struct {
	logger  L.ContextLogger
	hash    string
	started time.Time
	count   atomic.Int32
	sink    func(wltProbeWorkerEvent)
}

func withWLTProbeWorkerObserver(ctx context.Context, factory L.Factory, hash string) context.Context {
	if factory == nil {
		return ctx
	}
	return context.WithValue(ctx, wltProbeWorkerObserverKey{}, &wltProbeWorkerObserver{
		logger: factory.NewLogger("wlt-probe-worker"), hash: hash, started: time.Now(),
	})
}

// Observe original operation results only. Never certify delivery, change the
// original result, renew a deadline, or print request/profile/auth/error text.
func observeWLTProbeWorker(ctx context.Context, event string, result wltProbeResult, err error) {
	observer, _ := ctx.Value(wltProbeWorkerObserverKey{}).(*wltProbeWorkerObserver)
	if observer == nil || observer.count.Add(1) > 24 {
		return
	}
	digest := observer.hash
	decoded, invalid := hex.DecodeString(digest)
	if invalid != nil || len(decoded) != 32 || len(digest) != 64 {
		digest = "invalid"
	}
	errorClass := "none"
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			errorClass = "deadline"
		case errors.Is(err, context.Canceled):
			errorClass = "canceled"
		case errors.Is(err, io.EOF):
			errorClass = "eof"
		case errors.Is(err, io.ErrClosedPipe), errors.Is(err, net.ErrClosed):
			errorClass = "closed"
		default:
			errorClass = "other"
		}
	}
	row := wltProbeWorkerEvent{
		Event: event, RequestHash: digest, ElapsedMS: time.Since(observer.started).Milliseconds(),
		Status: result.Status, ErrorCode: result.ErrorCode, HTTPStatus: -1, BytesRead: -1, ErrorClass: errorClass,
	}
	if result.HTTPStatus != nil {
		row.HTTPStatus = *result.HTTPStatus
	}
	if result.BytesRead != nil {
		row.BytesRead = *result.BytesRead
	}
	if observer.sink != nil {
		observer.sink(row)
		return
	}
	observer.logger.InfoContext(ctx, "WLT original probe worker event=", row.Event,
		" request_sha256=", row.RequestHash, " elapsed_ms=", row.ElapsedMS,
		" status=", row.Status, " error_code=", row.ErrorCode,
		" http_status=", row.HTTPStatus, " bytes_read=", row.BytesRead,
		" error_class=", row.ErrorClass)
}
