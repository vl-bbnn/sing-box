//go:build with_wlt

package daemon

import (
	"context"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/service"
)

// One budget covers every service. Never extend shutdown by retrying a failed
// OPEN or by giving each service its own fresh timeout.
const wltShutdownDrainTimeout = 5 * time.Second

func quiesceWLTServices(instanceCtx context.Context) error {
	manager := service.FromContext[adapter.ServiceManager](instanceCtx)
	if manager == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(instanceCtx, wltShutdownDrainTimeout)
	defer cancel()
	var result error
	for _, candidate := range manager.Services() {
		if candidate.Type() != C.TypeWLT {
			continue
		}
		if quiescer, ok := candidate.(interface{ QuiescePendingDials(context.Context) error }); ok {
			result = E.Append(result, quiescer.QuiescePendingDials(ctx), func(err error) error {
				return E.Cause(err, "quiesce WLT pending dials")
			})
		}
	}
	return result
}
