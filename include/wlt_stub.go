//go:build !with_wlt

package include

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	boxService "github.com/sagernet/sing-box/adapter/service"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func registerWLTOutbound(registry *outbound.Registry) {
	outbound.Register[option.WLTOutboundOptions](registry, C.TypeWLT, func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.WLTOutboundOptions) (adapter.Outbound, error) {
		return nil, E.New("WLT support is disabled; rebuild with the with_wlt tag")
	})
}

func registerWLTService(registry *boxService.Registry) {
	boxService.Register[option.WLTServiceOptions](registry, C.TypeWLT, func(ctx context.Context, logger log.ContextLogger, tag string, options option.WLTServiceOptions) (adapter.Service, error) {
		return nil, E.New("WLT support is disabled; rebuild with the with_wlt tag")
	})
}
