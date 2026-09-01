//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/sagernet/sing/service"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
)

const (
	wltStatsHeartbeatInterval    = 30 * time.Second
	wltIncidentPollInterval      = 5 * time.Second
	wltReconnectRecoveryAfter    = 90 * time.Second
	wltInterfaceRecoveryGrace    = 15 * time.Second
	wltCarrierRestartRetryDelay  = 15 * time.Second
	wltCarrierRestartRetryMax    = 15 * time.Minute
	wltCarrierRateLimitRetry     = 30 * time.Minute
	wltCarrierRestartRetryMaxLog = 4
)

func RegisterService(registry *boxService.Registry) {
	boxService.Register[option.WLTServiceOptions](registry, C.TypeWLT, NewService)
}

type Service struct {
	boxService.Adapter
	ctx          context.Context
	logger       log.ContextLogger
	options      option.WLTServiceOptions
	network      adapter.NetworkManager
	access       sync.RWMutex
	restart      sync.Mutex
	carrier      *wltpkg.Carrier
	carrierReady chan struct{}
	stopped      bool
	statsCancel  context.CancelFunc
	interfaceKey string
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
		Adapter:      boxService.NewAdapter(C.TypeWLT, tag),
		ctx:          ctx,
		logger:       logger,
		options:      options,
		network:      service.FromContext[adapter.NetworkManager](ctx),
		carrierReady: make(chan struct{}),
	}, nil
}

func (s *Service) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateInitialize {
		return nil
	}
	s.restart.Lock()
	defer s.restart.Unlock()
	s.access.Lock()
	if s.carrier != nil {
		s.access.Unlock()
		return nil
	}
	if s.stopped {
		s.stopped = false
		s.carrierReady = make(chan struct{})
	}
	s.access.Unlock()

	startedAt := time.Now()
	s.logger.Info("wlt service starting transport=", s.options.Transport)
	// A persisted snapshot is the durable provider identity for unattended
	// restarts. Prefer it even on the first service start; otherwise an expired
	// inline profile snapshot sends the client into anonymous authorization
	// before the core can perform the identity-only refresh.
	carrier, err := s.startCarrier(persistentAuthSnapshotAvailable(s.options))
	if err != nil {
		// Keep the sing-box instance available while the WLT provider identity or
		// underlay recovers. WLT outbounds wait on carrierReady with their own dial
		// context, while unrelated/direct outbounds remain usable. Recovery is
		// unattended and serialized by the same restart lock used for handovers.
		s.access.Lock()
		s.stopped = false
		s.interfaceKey = s.currentInterfaceKey()
		s.access.Unlock()
		s.logger.Error("wlt service start degraded elapsed=", time.Since(startedAt).String(), " error=", err)
		go s.restartCarrier(nil, "initial startup degraded")
		return nil
	}
	s.access.Lock()
	s.carrier = carrier
	s.stopped = false
	s.interfaceKey = s.currentInterfaceKey()
	close(s.carrierReady)
	s.access.Unlock()
	carrier.MarkSingBoxReady()
	s.startStatsHeartbeat(carrier)
	s.logger.Info("wlt service started elapsed=", time.Since(startedAt).String())
	return nil
}

func (s *Service) startCarrier(preferPersistedAuth bool) (*wltpkg.Carrier, error) {
	if cacheFile := persistentDNSCacheFile(s.options); cacheFile != "" {
		if err := carriercommon.SetDNSCacheFile(cacheFile); err != nil {
			s.logger.Warn("wlt carrier persistent DNS cache unavailable path=", cacheFile, " error=", err)
		} else {
			s.logger.Info("wlt carrier persistent DNS cache configured path=", cacheFile)
		}
	}
	var socketControl carriercommon.SocketControlFunc
	if s.network != nil {
		if protectFunc := s.network.ProtectFunc(); protectFunc != nil {
			socketControl = carriercommon.SocketControlFunc(protectFunc)
		}
	}
	// The carrier must establish traffic without the profile control plane.
	// auth_snapshot_url remains a compatibility field, but pre-tunnel recovery
	// is limited to the profile bootstrap and client-persisted auth state.
	return wltpkg.StartCarrier(s.ctx, wltpkg.CarrierOptions{
		Config:                       s.options.CarrierConfig,
		ConfigFile:                   s.options.CarrierConfigFile,
		AuthSnapshot:                 s.options.AuthSnapshot,
		AuthReserveSnapshot:          s.options.AuthReserveSnapshot,
		AuthSnapshotFile:             s.options.AuthSnapshotFile,
		AuthSnapshotURL:              s.options.AuthSnapshotURL,
		AuthSnapshotFetchTimeout:     time.Duration(s.options.AuthSnapshotFetchTimeout),
		AuthSnapshotOutputFile:       s.options.AuthSnapshotOutputFile,
		ConfigTrustedAt:              s.options.ConfigTrustedAt,
		AuthSnapshotPreferFile:       preferPersistedAuth,
		ConnectTimeout:               time.Duration(s.options.ConnectTimeout),
		MaxActiveStreams:             s.options.MaxActiveStreams,
		MaxOpenAttempts:              s.options.MaxOpenAttempts,
		DNSOpenReserve:               s.options.DNSOpenReserve,
		MaxPendingDials:              s.options.MaxPendingDials,
		DialQueueTimeout:             time.Duration(s.options.DialQueueTimeout),
		IdleTimeout:                  time.Duration(s.options.IdleTimeout),
		PressureIdleTimeout:          time.Duration(s.options.PressureIdleTimeout),
		BufferSize:                   s.options.BufferSize,
		TinyMuxFlowBuffer:            s.options.TinyMuxFlowBuffer,
		TinyMuxFlowSendBuffer:        s.options.TinyMuxFlowSendBuffer,
		TinyMuxControlBuffer:         s.options.TinyMuxControlBuffer,
		TinyMuxRateBurstBytes:        s.options.TinyMuxRateBurstBytes,
		TinyMuxPingTimeout:           time.Duration(s.options.TinyMuxPingTimeout),
		PeerIncomingBuffer:           s.options.PeerIncomingBuffer,
		PeerWriteBuffer:              s.options.PeerWriteBuffer,
		AdaptivePeerData:             s.options.AdaptivePeerData,
		AdaptivePeerThresholdBytes:   s.options.AdaptivePeerThresholdBytes,
		AdaptivePeerIdleTimeout:      time.Duration(s.options.AdaptivePeerIdleTimeout),
		SRTPPacketBuffer:             s.options.SRTPPacketBuffer,
		KCPWindowSize:                s.options.KCPWindowSize,
		KCPReadWriteBuffer:           s.options.KCPReadWriteBuffer,
		RelayBandwidthBytesPerSecond: s.options.RelayBandwidthBytesPerSecond,
		SocketControl:                socketControl,
		Logger: func(format string, args ...any) {
			s.logger.InfoContext(s.ctx, fmt.Sprintf(format, args...))
		},
	})
}

func persistentDNSCacheFile(options option.WLTServiceOptions) string {
	path := strings.TrimSpace(options.AuthSnapshotOutputFile)
	if path == "" {
		path = strings.TrimSpace(options.AuthSnapshotFile)
	}
	if path == "" || !filepath.IsAbs(path) {
		return ""
	}
	return filepath.Join(filepath.Dir(path), "wlt-dns-cache.json")
}

func persistentAuthSnapshotAvailable(options option.WLTServiceOptions) bool {
	path := strings.TrimSpace(options.AuthSnapshotOutputFile)
	if path == "" {
		path = strings.TrimSpace(options.AuthSnapshotFile)
	}
	if path == "" || !filepath.IsAbs(path) {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() > 0
}

func (s *Service) Close() error {
	if s.statsCancel != nil {
		s.statsCancel()
		s.statsCancel = nil
	}
	s.access.Lock()
	carrier := s.carrier
	s.carrier = nil
	s.stopped = true
	select {
	case <-s.carrierReady:
	default:
		close(s.carrierReady)
	}
	s.access.Unlock()
	if carrier == nil {
		return nil
	}
	stats := carrier.Stats()
	err := carrier.Close()
	s.logger.Info("wlt service stopped active=", stats.ActiveStreams, " peak_active=", stats.PeakActiveStreams, " pending=", stats.PendingDials, " peak_pending=", stats.PeakPendingDials, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " queued=", stats.QueuedDials, " dns_open_requests=", stats.DNSOpenRequests, " dns_open_queued=", stats.DNSOpenQueued, " dns_open_rejected=", stats.DNSOpenRejected, " rejected=", stats.RejectedStreams, " rejected_queue=", stats.RejectedQueue, " rejected_active=", stats.RejectedActive, " rejected_open=", stats.RejectedOpen, " pressure_reclaims=", stats.PressureIdleReclaims, " failed=", stats.FailedStreams, " max_dial_ms=", stats.MaxDialMillis, " reconnect_retries=", stats.ReconnectRetries, " reconnect_wait_ms=", stats.ReconnectWaitMillis, " reconnects=", stats.Runtime.FullReconnects, " last_reconnect=", stats.Runtime.LastReconnectReason)
	return err
}

func (s *Service) Carrier() *wltpkg.Carrier {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.carrier
}

func (s *Service) WaitCarrier(ctx context.Context) (*wltpkg.Carrier, error) {
	for {
		s.access.RLock()
		carrier := s.carrier
		ready := s.carrierReady
		stopped := s.stopped
		s.access.RUnlock()
		if carrier != nil {
			return carrier, nil
		}
		if stopped {
			return nil, E.New("wlt service is stopped")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		case <-ready:
		}
	}
}

func (s *Service) InterfaceUpdated() {
	carrier := s.Carrier()
	if carrier == nil {
		return
	}
	currentInterfaceKey := s.currentInterfaceKey()
	s.access.Lock()
	previousInterfaceKey := s.interfaceKey
	if currentInterfaceKey != "" {
		s.interfaceKey = currentInterfaceKey
	}
	s.access.Unlock()
	if interfaceIdentityChanged(previousInterfaceKey, currentInterfaceKey) {
		// A real default-interface or address change invalidates the UDP/TURN
		// sockets even while their peer goroutines still look online.  Waiting
		// for peer_online to reach zero leaves TinyMux opens hanging for the
		// full connect timeout after LTE <-> Wi-Fi handover.  Abort the stale
		// underlay immediately; restartCarrier reuses the persisted auth
		// snapshot and serializes duplicate notifications.
		s.logger.Warn("wlt service default interface identity changed; replacing carrier immediately")
		s.scheduleCarrierRestart(carrier, "default interface identity changed")
		return
	}
	// An iOS default-interface notification does not prove that the existing
	// TURN underlay is dead.  It can arrive while LTE remains usable, and the
	// old break-before-make path aborted every TinyMux flow immediately.  Give
	// per-peer reconnect and the carrier's own full reconnect a bounded grace
	// period, then replace only a carrier which has actually lost every peer.
	s.logger.Info("wlt service default interface changed; preserving active carrier during recovery grace")
	go func(expected *wltpkg.Carrier) {
		timer := time.NewTimer(wltInterfaceRecoveryGrace)
		defer timer.Stop()
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}
		if s.Carrier() != expected {
			return
		}
		stats := expected.Stats()
		if !interfaceUpdateNeedsCarrierRestart(stats) {
			s.logger.Info("wlt service interface recovery retained carrier peer_online=", stats.Runtime.Peer.OnlinePeers, " reconnecting=", stats.Runtime.Reconnecting)
			return
		}
		s.logger.Warn("wlt service interface recovery lost all peers; replacing carrier after grace=", wltInterfaceRecoveryGrace.String())
		s.scheduleCarrierRestart(expected, "default interface changed with no peers after grace")
	}(carrier)
}

func (s *Service) currentInterfaceKey() string {
	if s.network == nil {
		return ""
	}
	return networkInterfaceKey(s.network.DefaultNetworkInterface())
}

func networkInterfaceKey(networkInterface *adapter.NetworkInterface) string {
	if networkInterface == nil {
		return ""
	}
	return fmt.Sprintf(
		"%d|%s|%s|%v",
		networkInterface.Index,
		networkInterface.Name,
		networkInterface.Type,
		networkInterface.Addresses,
	)
}

func interfaceIdentityChanged(previous string, current string) bool {
	return previous != "" && current != "" && previous != current
}

func interfaceUpdateNeedsCarrierRestart(stats wltpkg.CarrierStats) bool {
	return stats.Runtime.Peer.OnlinePeers == 0
}

func formatWLTStatsHeartbeat(messages ...any) string {
	return fmt.Sprint(messages...)
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
						s.scheduleCarrierRestart(carrier, "reconnect outage: "+rt.LastReconnectReason)
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
				s.logger.Info(formatWLTStatsHeartbeat("wlt service stats active=", stats.ActiveStreams, " peak_active=", stats.PeakActiveStreams, " pending=", stats.PendingDials, " peak_pending=", stats.PeakPendingDials, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " queued=", stats.QueuedDials, " dns_open_requests=", stats.DNSOpenRequests, " dns_open_queued=", stats.DNSOpenQueued, " dns_open_rejected=", stats.DNSOpenRejected, " rejected=", stats.RejectedStreams, " rejected_queue=", stats.RejectedQueue, " rejected_active=", stats.RejectedActive, " rejected_open=", stats.RejectedOpen, " pressure_reclaims=", stats.PressureIdleReclaims, " failed=", stats.FailedStreams, " interval_ms=", elapsed.Milliseconds(), " opened_delta=", counterDelta(stats.OpenedStreams, lastStats.OpenedStreams), " closed_delta=", counterDelta(stats.ClosedStreams, lastStats.ClosedStreams), " queued_delta=", counterDelta(stats.QueuedDials, lastStats.QueuedDials), " rejected_delta=", counterDelta(stats.RejectedStreams, lastStats.RejectedStreams), " failed_delta=", counterDelta(stats.FailedStreams, lastStats.FailedStreams), " last_incident=", lastIncident, " last_incident_age_ms=", incidentAgeMillis(now, lastIncidentAt), " open_attempts=", stats.OpenAttempts, " last_active_wait_ms=", stats.LastActiveWaitMillis, " max_active_wait_ms=", stats.MaxActiveWaitMillis, " last_open_wait_ms=", stats.LastOpenWaitMillis, " max_open_wait_ms=", stats.MaxOpenWaitMillis, " last_dial_ms=", stats.LastDialMillis, " max_dial_ms=", stats.MaxDialMillis, " reconnect_retries=", stats.ReconnectRetries, " reconnect_wait_ms=", stats.ReconnectWaitMillis, " reconnecting=", rt.Reconnecting, " reconnects=", rt.FullReconnects, " last_reconnect=", rt.LastReconnectReason, " mux_open_pending=", mux.OpenPending, " mux_peak_open_pending=", mux.PeakOpenPending, " mux_open_requests=", mux.OpenRequests, " mux_open_replies=", mux.OpenReplies, " mux_open_canceled=", mux.OpenCanceled, " mux_open_errors=", mux.OpenErrors, " mux_open_delta=", counterDelta(mux.OpenRequests, lastMux.OpenRequests), " mux_open_errors_delta=", counterDelta(mux.OpenErrors, lastMux.OpenErrors), " mux_last_open_ms=", mux.LastOpenLatencyMillis, " mux_max_open_ms=", mux.MaxOpenLatencyMillis, " mux_ping_sent=", mux.PingSent, " mux_pong=", mux.PongReceived, " mux_ping_timeouts=", mux.PingTimeouts, " mux_ping_timeouts_delta=", counterDelta(mux.PingTimeouts, lastMux.PingTimeouts), " mux_last_ping_rtt_ms=", mux.LastPingRTTMillis, " mux_max_ping_rtt_ms=", mux.MaxPingRTTMillis, " mux_disconnects=", mux.Disconnects, " mux_control_errors=", mux.ControlReadErrors, " mux_flow_drops=", mux.FlowDrops, " mux_flow_drops_delta=", counterDelta(mux.FlowDrops, lastMux.FlowDrops), " mux_flow_in_kib_s=", kibPerSecond(counterDelta(mux.FlowBytesIn, lastMux.FlowBytesIn), elapsed), " mux_flow_out_kib_s=", kibPerSecond(counterDelta(mux.FlowBytesOut, lastMux.FlowBytesOut), elapsed), " mux_rate_waits=", mux.RateWaits, " mux_rate_waits_delta=", counterDelta(mux.RateWaits, lastMux.RateWaits), " mux_rate_wait_ms=", mux.RateWaitNanos/int64(time.Millisecond), " mux_burst=", mux.RateBurstBytes, " peer_online=", peer.OnlinePeers, " peer_slots=", peer.TotalPeerSlots, " peer_active_data=", peer.ActiveDataPeers, " peer_adaptive_activations=", peer.AdaptiveActivations, " peer_adaptive_delta=", counterDelta(peer.AdaptiveActivations, lastPeer.AdaptiveActivations), " peer_adaptive_fallbacks=", peer.AdaptiveFallbacks, " peer_in_queue_full=", peer.IncomingQueueFull, " peer_in_queue_full_delta=", counterDelta(peer.IncomingQueueFull, lastPeer.IncomingQueueFull), " peer_out_queue_full=", peer.OutgoingQueueFull, " peer_out_queue_full_delta=", counterDelta(peer.OutgoingQueueFull, lastPeer.OutgoingQueueFull), " peer_reconnect_attempts=", peer.ReconnectAttempts, " peer_reconnect_failures=", peer.ReconnectFailures, " peer_in_kib_s=", kibPerSecond(counterDelta(peer.IncomingBytes, lastPeer.IncomingBytes), elapsed), " peer_out_kib_s=", kibPerSecond(counterDelta(peer.OutgoingBytes, lastPeer.OutgoingBytes), elapsed), " peer_bytes_in=", peer.IncomingBytes, " peer_bytes_out=", peer.OutgoingBytes, " peer_in_bytes_by_index=", peer.PerPeerIncomingBytes, " peer_out_bytes_by_index=", peer.PerPeerOutgoingBytes, " peer_write_ns_by_index=", peer.PerPeerWriteNanos, " peer_max_write_ns_by_index=", peer.PerPeerMaxWriteNanos, " peer_send_queue_by_index=", peer.PerPeerSendQueueDepth, " peer_priority_queue_by_index=", peer.PerPeerPriorityQueueDepth))
				lastStats = stats
				lastStatsAt = now
			}
		}
	}()
}

func (s *Service) restartCarrier(expected *wltpkg.Carrier, reason string) {
	s.restart.Lock()
	defer s.restart.Unlock()
	ready, detached := s.detachCarrierForRestart(expected)
	if !detached {
		return
	}
	s.restartCarrierGeneration(expected, ready, reason)
}

// scheduleCarrierRestart removes an obsolete carrier from the dial path before
// returning to the interface-monitor callback.  Starting restartCarrier in a
// goroutine directly leaves a scheduler-sized window where new streams can
// still acquire the stale carrier and produce an avoidable mux open failure.
// The restart goroutine receives the exact ready generation it owns so a stale
// restart can never publish over a newer service state.
func (s *Service) scheduleCarrierRestart(expected *wltpkg.Carrier, reason string) {
	if expected == nil {
		go s.restartCarrier(nil, reason)
		return
	}
	ready, detached := s.detachCarrierForRestart(expected)
	if !detached {
		return
	}
	go func() {
		s.restart.Lock()
		defer s.restart.Unlock()
		s.restartCarrierGeneration(expected, ready, reason)
	}()
}

func (s *Service) detachCarrierForRestart(expected *wltpkg.Carrier) (chan struct{}, bool) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.carrier != expected {
		return nil, false
	}
	if expected != nil {
		s.carrier = nil
		s.carrierReady = make(chan struct{})
	} else if s.carrierReady == nil {
		s.carrierReady = make(chan struct{})
	}
	return s.carrierReady, true
}

func (s *Service) restartCarrierGeneration(expected *wltpkg.Carrier, ready chan struct{}, reason string) {
	s.access.RLock()
	currentGeneration := s.carrier == nil && s.carrierReady == ready && !s.stopped
	s.access.RUnlock()
	if !currentGeneration || s.ctx.Err() != nil {
		return
	}

	if s.statsCancel != nil {
		s.statsCancel()
		s.statsCancel = nil
	}
	s.logger.Warn("wlt service restarting carrier reason=", reason)

	if expected != nil {
		stats := expected.Stats()
		// The old UDP/TURN underlay is already unusable after an interface
		// change. A graceful mux/KCP drain retains its TURN allocations for up to
		// ten seconds; starting the replacement concurrently can then exceed the
		// provider's per-user allocation quota. Abort the obsolete carrier first,
		// which releases sockets and allocations without waiting for that drain.
		startedAt := time.Now()
		s.logger.Info("wlt service old carrier abort started active=", stats.ActiveStreams, " opened=", stats.OpenedStreams, " closed=", stats.ClosedStreams, " failed=", stats.FailedStreams, " reconnect_retries=", stats.ReconnectRetries, " reconnect_wait_ms=", stats.ReconnectWaitMillis, " reconnects=", stats.Runtime.FullReconnects, " last_reconnect=", stats.Runtime.LastReconnectReason)
		if err := expected.Abort(); err != nil {
			s.logger.Warn("wlt service old carrier abort error: ", err)
		}
		s.logger.Info("wlt service old carrier aborted elapsed=", time.Since(startedAt).String())
	}

	for attempt := 1; ; attempt++ {
		s.access.RLock()
		currentGeneration = s.carrier == nil && s.carrierReady == ready && !s.stopped
		s.access.RUnlock()
		if !currentGeneration || s.ctx.Err() != nil {
			return
		}
		startedAt := time.Now()
		s.logger.Info("wlt service carrier restart attempt=", attempt, " reason=", reason)
		carrier, err := s.startCarrier(true)
		if err == nil {
			s.access.Lock()
			if s.stopped || s.carrier != nil || s.carrierReady != ready {
				s.access.Unlock()
				_ = carrier.Close()
				return
			}
			s.carrier = carrier
			s.interfaceKey = s.currentInterfaceKey()
			close(ready)
			s.access.Unlock()
			s.startStatsHeartbeat(carrier)
			s.logger.Info("wlt service carrier restarted elapsed=", time.Since(startedAt).String(), " attempts=", attempt)
			return
		}
		if attempt <= wltCarrierRestartRetryMaxLog || attempt%10 == 0 {
			s.logger.Error("wlt service carrier restart failed attempt=", attempt, " error=", err)
		}
		timer := time.NewTimer(carrierRestartRetryDelay(attempt, err))
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func carrierRestartRetryDelay(attempt int, err error) time.Duration {
	if errors.Is(err, carriercommon.ErrHumanChallengeErrorLimit) {
		return wltCarrierRateLimitRetry
	}
	if attempt < 1 {
		attempt = 1
	}
	delay := wltCarrierRestartRetryDelay
	for current := 1; current < attempt && delay < wltCarrierRestartRetryMax; current++ {
		delay *= 2
		if delay > wltCarrierRestartRetryMax {
			delay = wltCarrierRestartRetryMax
		}
	}
	return delay
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
	appendDelta("pressure_reclaims", current.PressureIdleReclaims, previous.PressureIdleReclaims)
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
