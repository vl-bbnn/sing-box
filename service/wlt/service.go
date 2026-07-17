//go:build with_wlt

package wlt

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxService "github.com/sagernet/sing-box/adapter/service"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

const (
	wltStatsHeartbeatInterval    = 30 * time.Second
	wltIncidentPollInterval      = 5 * time.Second
	wltReconnectRecoveryAfter    = 90 * time.Second
	wltCarrierRestartRetryDelay  = 15 * time.Second
	wltCarrierRestartRetryMaxLog = 4
)

func RegisterService(registry *boxService.Registry) {
	boxService.Register[option.WLTServiceOptions](registry, C.TypeWLT, NewService)
}

type Service struct {
	boxService.Adapter
	ctx         context.Context
	logger      log.ContextLogger
	options     option.WLTServiceOptions
	access      sync.RWMutex
	restart     sync.Mutex
	carrier     *wltpkg.Carrier
	statsCancel context.CancelFunc
}

func NewService(ctx context.Context, logger log.ContextLogger, tag string, options option.WLTServiceOptions) (adapter.Service, error) {
	transport := strings.ToLower(strings.TrimSpace(options.Transport))
	if transport == "" {
		transport = "wlt"
		options.Transport = transport
	}
	if transport != "wlt" && transport != "carrier" && transport != "turnable" {
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
	s.restart.Lock()
	defer s.restart.Unlock()
	s.access.RLock()
	if s.carrier != nil {
		s.access.RUnlock()
		return nil
	}
	s.access.RUnlock()

	startedAt := time.Now()
	s.logger.Info("wlt service starting transport=", s.options.Transport)
	carrier, err := s.startCarrier()
	if err != nil {
		s.logger.Error("wlt service start failed elapsed=", time.Since(startedAt).String(), " error=", err)
		return err
	}
	s.access.Lock()
	s.carrier = carrier
	s.access.Unlock()
	s.startStatsHeartbeat(carrier)
	s.logger.Info("wlt service started elapsed=", time.Since(startedAt).String())
	return nil
}

func (s *Service) startCarrier() (*wltpkg.Carrier, error) {
	return wltpkg.StartCarrier(s.ctx, wltpkg.CarrierOptions{
		Config:                       s.options.CarrierConfig,
		ConfigFile:                   s.options.CarrierConfigFile,
		AuthSnapshot:                 s.options.AuthSnapshot,
		AuthSnapshotFile:             s.options.AuthSnapshotFile,
		AuthSnapshotURL:              s.options.AuthSnapshotURL,
		AuthSnapshotFetchTimeout:     time.Duration(s.options.AuthSnapshotFetchTimeout),
		AuthSnapshotOutputFile:       s.options.AuthSnapshotOutputFile,
		ConnectTimeout:               time.Duration(s.options.ConnectTimeout),
		MaxActiveStreams:             s.options.MaxActiveStreams,
		MaxOpenAttempts:              s.options.MaxOpenAttempts,
		MaxPendingDials:              s.options.MaxPendingDials,
		DialQueueTimeout:             time.Duration(s.options.DialQueueTimeout),
		IdleTimeout:                  time.Duration(s.options.IdleTimeout),
		BufferSize:                   s.options.BufferSize,
		TinyMuxFlowBuffer:            s.options.TinyMuxFlowBuffer,
		TinyMuxFlowSendBuffer:        s.options.TinyMuxFlowSendBuffer,
		TinyMuxControlBuffer:         s.options.TinyMuxControlBuffer,
		TinyMuxRateBurstBytes:        s.options.TinyMuxRateBurstBytes,
		TinyMuxPingTimeout:           time.Duration(s.options.TinyMuxPingTimeout),
		PeerIncomingBuffer:           s.options.PeerIncomingBuffer,
		PeerWriteBuffer:              s.options.PeerWriteBuffer,
		RedundantPeerData:            s.options.RedundantPeerData,
		AdaptivePeerData:             s.options.AdaptivePeerData,
		AdaptivePeerThresholdBytes:   s.options.AdaptivePeerThresholdBytes,
		AdaptivePeerIdleTimeout:      time.Duration(s.options.AdaptivePeerIdleTimeout),
		SRTPPacketBuffer:             s.options.SRTPPacketBuffer,
		KCPWindowSize:                s.options.KCPWindowSize,
		KCPReadWriteBuffer:           s.options.KCPReadWriteBuffer,
		RelayBandwidthBytesPerSecond: s.options.RelayBandwidthBytesPerSecond,
		Logger: func(format string, args ...any) {
			s.logger.InfoContext(s.ctx, fmt.Sprintf(format, args...))
		},
	})
}

func (s *Service) Close() error {
	if s.statsCancel != nil {
		s.statsCancel()
		s.statsCancel = nil
	}
	s.access.Lock()
	carrier := s.carrier
	s.carrier = nil
	s.access.Unlock()
	if carrier == nil {
		return nil
	}
	stats := carrier.Stats()
	err := carrier.Close()
	s.logger.Info("wlt service stopped active=", stats.ActiveStreams, " peak_active=", stats.PeakActiveStreams, " pending=", stats.PendingDials, " peak_pending=", stats.PeakPendingDials, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " queued=", stats.QueuedDials, " rejected=", stats.RejectedStreams, " rejected_queue=", stats.RejectedQueue, " rejected_active=", stats.RejectedActive, " rejected_open=", stats.RejectedOpen, " failed=", stats.FailedStreams, " max_dial_ms=", stats.MaxDialMillis, " reconnect_retries=", stats.ReconnectRetries, " reconnect_wait_ms=", stats.ReconnectWaitMillis, " reconnects=", stats.Runtime.FullReconnects, " last_reconnect=", stats.Runtime.LastReconnectReason)
	return err
}

func (s *Service) Carrier() *wltpkg.Carrier {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.carrier
}

func (s *Service) startStatsHeartbeat(carrier *wltpkg.Carrier) {
	if s.statsCancel != nil {
		s.statsCancel()
	}
	statsCtx, cancel := context.WithCancel(s.ctx)
	s.statsCancel = cancel
	go func() {
		heartbeatTicker := time.NewTicker(wltStatsHeartbeatInterval)
		defer heartbeatTicker.Stop()
		incidentTicker := time.NewTicker(wltIncidentPollInterval)
		defer incidentTicker.Stop()
		var reconnectingSince time.Time
		initialStats := carrier.Stats()
		lastStats := initialStats
		lastIncidentStats := initialStats
		lastStatsAt := time.Now()
		var lastIncidentAt time.Time
		lastIncident := "none"
		for {
			select {
			case <-statsCtx.Done():
				return
			case <-incidentTicker.C:
				now := time.Now()
				stats := carrier.Stats()
				if incident := describeWLTIncident(stats, lastIncidentStats); incident != "" {
					lastIncidentAt = now
					lastIncident = incident
					s.logger.Warn("wlt service incident ", incident)
				}
				lastIncidentStats = stats
				rt := stats.Runtime
				if rt.Reconnecting && rt.Peer.OnlinePeers == 0 {
					if reconnectingSince.IsZero() {
						reconnectingSince = now
						s.logger.Warn("wlt service reconnect outage started reason=", rt.LastReconnectReason)
					}
					if now.Sub(reconnectingSince) >= wltReconnectRecoveryAfter {
						s.logger.Warn("wlt service reconnect outage exceeded threshold elapsed=", now.Sub(reconnectingSince).String(), " threshold=", wltReconnectRecoveryAfter.String(), " reason=", rt.LastReconnectReason)
						go s.restartCarrier(carrier, "reconnect outage: "+rt.LastReconnectReason)
						return
					}
				} else if !reconnectingSince.IsZero() {
					s.logger.Info("wlt service reconnect outage recovered elapsed=", now.Sub(reconnectingSince).String())
					reconnectingSince = time.Time{}
				}
			case <-heartbeatTicker.C:
				now := time.Now()
				stats := carrier.Stats()
				rt := stats.Runtime
				mux := rt.Mux
				peer := rt.Peer
				elapsed := now.Sub(lastStatsAt)
				lastMux := lastStats.Runtime.Mux
				lastPeer := lastStats.Runtime.Peer
				s.logger.Info("wlt service stats active=", stats.ActiveStreams, " peak_active=", stats.PeakActiveStreams, " pending=", stats.PendingDials, " peak_pending=", stats.PeakPendingDials, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " queued=", stats.QueuedDials, " rejected=", stats.RejectedStreams, " rejected_queue=", stats.RejectedQueue, " rejected_active=", stats.RejectedActive, " rejected_open=", stats.RejectedOpen, " failed=", stats.FailedStreams, " interval_ms=", elapsed.Milliseconds(), " opened_delta=", counterDelta(stats.OpenedStreams, lastStats.OpenedStreams), " closed_delta=", counterDelta(stats.ClosedStreams, lastStats.ClosedStreams), " queued_delta=", counterDelta(stats.QueuedDials, lastStats.QueuedDials), " rejected_delta=", counterDelta(stats.RejectedStreams, lastStats.RejectedStreams), " failed_delta=", counterDelta(stats.FailedStreams, lastStats.FailedStreams), " last_incident=", lastIncident, " last_incident_age_ms=", incidentAgeMillis(now, lastIncidentAt), " open_attempts=", stats.OpenAttempts, " last_active_wait_ms=", stats.LastActiveWaitMillis, " max_active_wait_ms=", stats.MaxActiveWaitMillis, " last_open_wait_ms=", stats.LastOpenWaitMillis, " max_open_wait_ms=", stats.MaxOpenWaitMillis, " last_dial_ms=", stats.LastDialMillis, " max_dial_ms=", stats.MaxDialMillis, " reconnect_retries=", stats.ReconnectRetries, " reconnect_wait_ms=", stats.ReconnectWaitMillis, " reconnecting=", rt.Reconnecting, " reconnects=", rt.FullReconnects, " last_reconnect=", rt.LastReconnectReason, " mux_open_pending=", mux.OpenPending, " mux_peak_open_pending=", mux.PeakOpenPending, " mux_open_requests=", mux.OpenRequests, " mux_open_replies=", mux.OpenReplies, " mux_open_canceled=", mux.OpenCanceled, " mux_open_errors=", mux.OpenErrors, " mux_open_delta=", counterDelta(mux.OpenRequests, lastMux.OpenRequests), " mux_open_errors_delta=", counterDelta(mux.OpenErrors, lastMux.OpenErrors), " mux_last_open_ms=", mux.LastOpenLatencyMillis, " mux_max_open_ms=", mux.MaxOpenLatencyMillis, " mux_ping_sent=", mux.PingSent, " mux_pong=", mux.PongReceived, " mux_ping_timeouts=", mux.PingTimeouts, " mux_ping_timeouts_delta=", counterDelta(mux.PingTimeouts, lastMux.PingTimeouts), " mux_last_ping_rtt_ms=", mux.LastPingRTTMillis, " mux_max_ping_rtt_ms=", mux.MaxPingRTTMillis, " mux_disconnects=", mux.Disconnects, " mux_control_errors=", mux.ControlReadErrors, " mux_flow_drops=", mux.FlowDrops, " mux_flow_drops_delta=", counterDelta(mux.FlowDrops, lastMux.FlowDrops), " mux_flow_in_kib_s=", kibPerSecond(counterDelta(mux.FlowBytesIn, lastMux.FlowBytesIn), elapsed), " mux_flow_out_kib_s=", kibPerSecond(counterDelta(mux.FlowBytesOut, lastMux.FlowBytesOut), elapsed), " mux_rate_waits=", mux.RateWaits, " mux_rate_waits_delta=", counterDelta(mux.RateWaits, lastMux.RateWaits), " mux_rate_wait_ms=", mux.RateWaitNanos/int64(time.Millisecond), " mux_burst=", mux.RateBurstBytes, " peer_online=", peer.OnlinePeers, " peer_slots=", peer.TotalPeerSlots, " peer_active_data=", peer.ActiveDataPeers, " peer_adaptive_activations=", peer.AdaptiveActivations, " peer_adaptive_delta=", counterDelta(peer.AdaptiveActivations, lastPeer.AdaptiveActivations), " peer_adaptive_fallbacks=", peer.AdaptiveFallbacks, " peer_in_queue_full=", peer.IncomingQueueFull, " peer_in_queue_full_delta=", counterDelta(peer.IncomingQueueFull, lastPeer.IncomingQueueFull), " peer_out_queue_full=", peer.OutgoingQueueFull, " peer_out_queue_full_delta=", counterDelta(peer.OutgoingQueueFull, lastPeer.OutgoingQueueFull), " peer_reconnect_attempts=", peer.ReconnectAttempts, " peer_reconnect_failures=", peer.ReconnectFailures, " peer_in_kib_s=", kibPerSecond(counterDelta(peer.IncomingBytes, lastPeer.IncomingBytes), elapsed), " peer_out_kib_s=", kibPerSecond(counterDelta(peer.OutgoingBytes, lastPeer.OutgoingBytes), elapsed), " peer_bytes_in=", peer.IncomingBytes, " peer_bytes_out=", peer.OutgoingBytes)
				lastStats = stats
				lastStatsAt = now
			}
		}
	}()
}

func (s *Service) restartCarrier(expected *wltpkg.Carrier, reason string) {
	s.restart.Lock()
	defer s.restart.Unlock()

	s.access.RLock()
	current := s.carrier
	s.access.RUnlock()
	if current != expected {
		return
	}

	if s.statsCancel != nil {
		s.statsCancel()
		s.statsCancel = nil
	}

	s.logger.Warn("wlt service restarting carrier reason=", reason)
	s.access.Lock()
	s.carrier = nil
	s.access.Unlock()

	if expected != nil {
		stats := expected.Stats()
		if err := expected.Close(); err != nil {
			s.logger.Warn("wlt service old carrier close error: ", err)
		}
		s.logger.Info("wlt service old carrier closed active=", stats.ActiveStreams, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " failed=", stats.FailedStreams, " reconnect_retries=", stats.ReconnectRetries, " reconnect_wait_ms=", stats.ReconnectWaitMillis, " reconnects=", stats.Runtime.FullReconnects, " last_reconnect=", stats.Runtime.LastReconnectReason)
	}

	for attempt := 1; ; attempt++ {
		if s.ctx.Err() != nil {
			return
		}
		startedAt := time.Now()
		s.logger.Info("wlt service carrier restart attempt=", attempt, " reason=", reason)
		carrier, err := s.startCarrier()
		if err == nil {
			s.access.Lock()
			s.carrier = carrier
			s.access.Unlock()
			s.startStatsHeartbeat(carrier)
			s.logger.Info("wlt service carrier restarted elapsed=", time.Since(startedAt).String(), " attempts=", attempt)
			return
		}
		if attempt <= wltCarrierRestartRetryMaxLog || attempt%10 == 0 {
			s.logger.Error("wlt service carrier restart failed attempt=", attempt, " error=", err)
		}
		timer := time.NewTimer(wltCarrierRestartRetryDelay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func counterDelta(current int64, previous int64) int64 {
	if current < previous {
		return current
	}
	return current - previous
}

func kibPerSecond(bytes int64, elapsed time.Duration) int64 {
	if bytes <= 0 || elapsed <= 0 {
		return 0
	}
	return int64(float64(bytes) / 1024 / elapsed.Seconds())
}

func describeWLTIncident(current wltpkg.CarrierStats, previous wltpkg.CarrierStats) string {
	currentMux := current.Runtime.Mux
	previousMux := previous.Runtime.Mux
	currentPeer := current.Runtime.Peer
	previousPeer := previous.Runtime.Peer
	parts := make([]string, 0, 12)
	appendDelta := func(name string, value int64, oldValue int64) {
		if delta := counterDelta(value, oldValue); delta > 0 {
			parts = append(parts, fmt.Sprintf("%s_delta=%d", name, delta))
		}
	}
	appendDelta("reconnects", current.Runtime.FullReconnects, previous.Runtime.FullReconnects)
	appendDelta("reconnect_retries", current.ReconnectRetries, previous.ReconnectRetries)
	appendDelta("failed", current.FailedStreams, previous.FailedStreams)
	appendDelta("rejected", current.RejectedStreams, previous.RejectedStreams)
	appendDelta("rejected_queue", current.RejectedQueue, previous.RejectedQueue)
	appendDelta("rejected_active", current.RejectedActive, previous.RejectedActive)
	appendDelta("rejected_open", current.RejectedOpen, previous.RejectedOpen)
	appendDelta("mux_open_errors", currentMux.OpenErrors, previousMux.OpenErrors)
	appendDelta("mux_ping_timeouts", currentMux.PingTimeouts, previousMux.PingTimeouts)
	appendDelta("mux_disconnects", currentMux.Disconnects, previousMux.Disconnects)
	appendDelta("mux_control_errors", currentMux.ControlReadErrors, previousMux.ControlReadErrors)
	appendDelta("mux_flow_drops", currentMux.FlowDrops, previousMux.FlowDrops)
	appendDelta("peer_in_queue_full", currentPeer.IncomingQueueFull, previousPeer.IncomingQueueFull)
	appendDelta("peer_out_queue_full", currentPeer.OutgoingQueueFull, previousPeer.OutgoingQueueFull)
	appendDelta("peer_reconnect_failures", currentPeer.ReconnectFailures, previousPeer.ReconnectFailures)
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ") +
		fmt.Sprintf(" reconnecting=%t peer_online=%d active=%d pending=%d last_reconnect=%s", current.Runtime.Reconnecting, currentPeer.OnlinePeers, current.ActiveStreams, current.PendingDials, current.Runtime.LastReconnectReason)
}

func incidentAgeMillis(now time.Time, incidentAt time.Time) int64 {
	if incidentAt.IsZero() {
		return -1
	}
	return now.Sub(incidentAt).Milliseconds()
}
