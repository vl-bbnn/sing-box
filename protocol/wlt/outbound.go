//go:build with_wlt

package wlt

import (
	"context"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
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
	Carrier() *wltpkg.Carrier
	WaitCarrier(ctx context.Context) (*wltpkg.Carrier, error)
	InterfaceUpdated()
}

var _ adapter.InterfaceUpdateListener = (*Outbound)(nil)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.WLTOutboundOptions](registry, C.TypeWLT, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	logger         logger.ContextLogger
	serviceManager adapter.ServiceManager
	serviceTag     string
	carrierAccess  sync.RWMutex
	carrier        carrierService
	route          string
	directFallback bool
	fallbackDialer N.Dialer
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
	fallbackDialer, err := dialer.NewWithOptions(dialer.Options{
		Context:        ctx,
		RemoteIsDomain: true,
		DirectOutbound: true,
	})
	if err != nil {
		return nil, E.Cause(err, "create WLT direct fallback dialer")
	}
	return &Outbound{
		Adapter:        outbound.NewAdapter(C.TypeWLT, tag, []string{N.NetworkTCP}, nil),
		logger:         logger,
		serviceManager: serviceManager,
		serviceTag:     options.Service,
		route:          options.Route,
		directFallback: options.DirectFallbackEnabled(),
		fallbackDialer: fallbackDialer,
	}, nil
}

func (h *Outbound) resolveCarrier() (carrierService, error) {
	h.carrierAccess.RLock()
	if h.carrier != nil {
		carrier := h.carrier
		h.carrierAccess.RUnlock()
		return carrier, nil
	}
	h.carrierAccess.RUnlock()
	h.carrierAccess.Lock()
	defer h.carrierAccess.Unlock()
	if h.carrier != nil {
		return h.carrier, nil
	}
	rawService, loaded := h.serviceManager.Get(h.serviceTag)
	if !loaded {
		return nil, E.New("wlt service not found: ", h.serviceTag)
	}
	carrier, ok := rawService.(carrierService)
	if !ok {
		return nil, E.New("service ", h.serviceTag, " is not a wlt carrier service")
	}
	h.carrier = carrier
	return carrier, nil
}

func (h *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	admissionTarget := wltAdmissionTarget(ctx, destination)
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	if N.NetworkName(network) != N.NetworkTCP {
		return nil, E.New("wlt outbound supports tcp connections only")
	}
	carrierService, err := h.resolveCarrier()
	if err != nil {
		return nil, err
	}
	carrier := carrierService.Carrier()
	if carrier == nil && h.directFallback {
		h.logger.WarnContext(ctx, "WLT carrier unavailable; using encrypted upstream direct fallback")
		return h.fallbackDialer.DialContext(ctx, network, destination)
	}
	if carrier == nil {
		carrier, err = carrierService.WaitCarrier(ctx)
	}
	if err != nil {
		return nil, E.Cause(err, "wait for wlt carrier")
	}
	h.logger.DebugContext(ctx, "outbound WLT connection route=", h.route, " to ", destination)
	stream, err := carrier.DialStream(ctx, h.route, admissionTarget.String())
	if err != nil {
		if h.directFallback {
			h.logger.WarnContext(ctx, "WLT carrier stream unavailable; using encrypted upstream direct fallback")
			return h.fallbackDialer.DialContext(ctx, network, destination)
		}
		return nil, err
	}
	conn, ok := stream.(net.Conn)
	if !ok {
		_ = stream.Close()
		return nil, E.New("wlt carrier returned non-network stream")
	}
	return conn, nil
}

// wltAdmissionTarget preserves the destination that caused a nested outbound
// (normally VLESS) to dial its WLT detour. The immediate destination passed to
// this outbound is the proxy endpoint, which hides DNS :53/:853 traffic from
// class-aware carrier admission and makes every open look like ordinary HTTPS.
func wltAdmissionTarget(ctx context.Context, immediate M.Socksaddr) M.Socksaddr {
	metadata := adapter.ContextFrom(ctx)
	if metadata != nil && metadata.Destination.IsValid() {
		return metadata.Destination
	}
	return immediate
}

func (h *Outbound) InterfaceUpdated() {
	carrierService, err := h.resolveCarrier()
	if err != nil {
		h.logger.Warn("notify WLT carrier about interface update: ", err)
		return
	}
	carrierService.InterfaceUpdated()
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	ctx, metadata := adapter.ExtendContext(ctx)
	metadata.Outbound = h.Tag()
	metadata.Destination = destination
	h.logger.DebugContext(ctx, "rejected WLT packet connection to ", destination)
	return nil, E.New("wlt outbound does not support packet connections; use VLESS/XUDP over TCP")
}
