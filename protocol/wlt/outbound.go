package wlt

import (
	"context"
	"net"

	wltpkg "2b2n.local/whitelist-transport/pkg/wlt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type carrierService interface {
	Carrier() *wltpkg.TurnableCarrier
}

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.WLTOutboundOptions](registry, C.TypeWLT, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	logger  logger.ContextLogger
	carrier carrierService
	route   string
}

func NewOutbound(ctx context.Context, _ adapter.Router, logger log.ContextLogger, tag string, options option.WLTOutboundOptions) (adapter.Outbound, error) {
	if options.Service == "" {
		return nil, E.New("wlt outbound requires service")
	}
	if options.Route == "" {
		return nil, E.New("wlt outbound requires route")
	}
	for _, network := range options.BuildNetwork() {
		if network != N.NetworkTCP {
			return nil, E.New("wlt outbound supports tcp network only")
		}
	}
	serviceManager := service.FromContext[adapter.ServiceManager](ctx)
	if serviceManager == nil {
		return nil, E.New("missing service manager")
	}
	rawService, loaded := serviceManager.Get(options.Service)
	if !loaded {
		return nil, E.New("wlt service not found: ", options.Service)
	}
	carrier, ok := rawService.(carrierService)
	if !ok {
		return nil, E.New("service ", options.Service, " is not a wlt carrier service")
	}
	return &Outbound{
		Adapter: outbound.NewAdapter(C.TypeWLT, tag, []string{N.NetworkTCP}, nil),
		logger:  logger,
		carrier: carrier,
		route:   options.Route,
	}, nil
}

func (h *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	if N.NetworkName(network) != N.NetworkTCP {
		return nil, E.New("wlt outbound supports tcp connections only")
	}
	carrier := h.carrier.Carrier()
	if carrier == nil {
		return nil, E.New("wlt carrier is not started")
	}
	h.logger.InfoContext(ctx, "outbound WLT connection route=", h.route, " to ", destination)
	stream, err := carrier.DialStream(ctx, h.route, destination.String())
	if err != nil {
		return nil, err
	}
	conn, ok := stream.(net.Conn)
	if !ok {
		_ = stream.Close()
		return nil, E.New("wlt carrier returned non-network stream")
	}
	return conn, nil
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	h.logger.InfoContext(ctx, "rejected WLT packet connection to ", destination)
	return nil, E.New("wlt outbound does not support packet connections; use VLESS/XUDP over TCP")
}
