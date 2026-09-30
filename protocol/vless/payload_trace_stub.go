//go:build !with_wlt

package vless

import (
	"context"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing/common/logger"
)

func wltFirstPayloadTrace(context.Context, string, logger.Logger) *mux.FirstPayloadTraceOptions {
	return nil
}
