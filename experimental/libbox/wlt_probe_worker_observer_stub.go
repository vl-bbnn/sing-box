//go:build with_wlt && !wlt_probe_worker_observer

package libbox

import (
	"context"

	L "github.com/sagernet/sing-box/log"
)

func withWLTProbeWorkerObserver(ctx context.Context, _ L.Factory, _ string) context.Context {
	return ctx
}

func observeWLTProbeWorker(context.Context, string, wltProbeResult, error) {}
