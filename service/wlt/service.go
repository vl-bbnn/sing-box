package wlt

import (
	"context"
	"fmt"
	"strings"
	"time"

	wltpkg "2b2n.local/whitelist-transport/pkg/wlt"
	"github.com/sagernet/sing-box/adapter"
	boxService "github.com/sagernet/sing-box/adapter/service"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func RegisterService(registry *boxService.Registry) {
	boxService.Register[option.WLTServiceOptions](registry, C.TypeWLT, NewService)
}

type Service struct {
	boxService.Adapter
	ctx         context.Context
	logger      log.ContextLogger
	options     option.WLTServiceOptions
	carrier     *wltpkg.TurnableCarrier
	statsCancel context.CancelFunc
}

func NewService(ctx context.Context, logger log.ContextLogger, tag string, options option.WLTServiceOptions) (adapter.Service, error) {
	transport := strings.ToLower(strings.TrimSpace(options.Transport))
	if transport == "" {
		transport = "turnable"
		options.Transport = transport
	}
	if transport != "turnable" {
		return nil, E.New("unsupported wlt service transport: ", options.Transport)
	}
	return &Service{
		Adapter: boxService.NewAdapter(C.TypeWLT, tag),
		ctx:     ctx,
		logger:  logger,
		options: options,
	}, nil
}

func (s *Service) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateInitialize {
		return nil
	}
	if s.carrier != nil {
		return nil
	}
	startedAt := time.Now()
	carrier, err := wltpkg.StartTurnableCarrier(s.ctx, wltpkg.TurnableCarrierOptions{
		Config:           s.options.TurnableConfig,
		ConfigFile:       s.options.TurnableConfigFile,
		ConnectTimeout:   time.Duration(s.options.ConnectTimeout),
		MaxActiveStreams: s.options.MaxActiveStreams,
		MaxOpenAttempts:  s.options.MaxOpenAttempts,
		IdleTimeout:      time.Duration(s.options.IdleTimeout),
		BufferSize:       s.options.BufferSize,
		Logger: func(format string, args ...any) {
			s.logger.InfoContext(s.ctx, fmt.Sprintf(format, args...))
		},
	})
	if err != nil {
		return err
	}
	s.carrier = carrier
	s.startStatsHeartbeat(carrier)
	s.logger.Info("wlt service started elapsed=", time.Since(startedAt).String())
	return nil
}

func (s *Service) Close() error {
	if s.carrier == nil {
		return nil
	}
	if s.statsCancel != nil {
		s.statsCancel()
		s.statsCancel = nil
	}
	stats := s.carrier.Stats()
	err := s.carrier.Close()
	s.carrier = nil
	s.logger.Info("wlt service stopped active=", stats.ActiveStreams, " peak_active=", stats.PeakActiveStreams, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " rejected=", stats.RejectedStreams, " failed=", stats.FailedStreams)
	return err
}

func (s *Service) Carrier() *wltpkg.TurnableCarrier {
	return s.carrier
}

func (s *Service) startStatsHeartbeat(carrier *wltpkg.TurnableCarrier) {
	if s.statsCancel != nil {
		s.statsCancel()
	}
	statsCtx, cancel := context.WithCancel(s.ctx)
	s.statsCancel = cancel
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-statsCtx.Done():
				return
			case <-ticker.C:
				stats := carrier.Stats()
				s.logger.Info("wlt service stats active=", stats.ActiveStreams, " peak_active=", stats.PeakActiveStreams, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " rejected=", stats.RejectedStreams, " failed=", stats.FailedStreams, " open_attempts=", stats.OpenAttempts)
			}
		}
	}()
}
