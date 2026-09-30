//go:build with_wlt

package vless

import (
	"context"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
)

// Diagnostic builds trace only smux application streams whose actual detour
// is a WLT outbound. The mux library applies the destination and stream cap.
func wltFirstPayloadTrace(ctx context.Context, detour string, log logger.Logger) *mux.FirstPayloadTraceOptions {
	if detour == "" {
		return nil
	}
	return &mux.FirstPayloadTraceOptions{
		Enabled: func() bool {
			manager := service.FromContext[adapter.OutboundManager](ctx)
			if manager == nil {
				return false
			}
			outbound, loaded := manager.Outbound(detour)
			return loaded && outbound.Type() == C.TypeWLT
		},
		Log: func(e mux.FirstPayloadTraceEvent) {
			log.Info("wlt payload phase trace_id=", e.ID, " phase=", e.Phase,
				" utc_ms=", e.UTCUnixMS, " elapsed_ms=", e.ElapsedMS, " bytes=", e.Bytes, " error_class=", e.ErrorClass)
		},
	}
}
