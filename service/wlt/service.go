//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	wltAndroidInterfaceFallback  = 5 * time.Second
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
	ctx                  context.Context
	logger               log.ContextLogger
	options              option.WLTServiceOptions
	network              adapter.NetworkManager
	access               sync.RWMutex
	restart              sync.Mutex
	statsAccess          sync.Mutex
	carrier              *wltpkg.Carrier
	carrierGeneration    uint64
	pendingDialLeases    map[*dialLease]struct{}
	dialStateChanged     chan struct{}
	dialStreamHook       func(context.Context, *wltpkg.Carrier, string, string) (io.ReadWriteCloser, error)
	carrierReady         chan struct{}
	interfaceReady       chan struct{}
	interfaceGeneration  uint64
	interfaceUnavailable bool
	initialized          bool
	stopped              bool
	quiescing            bool
	statsCancel          context.CancelFunc
	interfaceKey         string
	restartAttempt       *carrierStartAttempt
	carrierStartCancel   context.CancelFunc
	startCarrierHook     func(context.Context, bool) (*wltpkg.Carrier, error)
	markCarrierReadyHook func(*wltpkg.Carrier)
	abortCarrierHook     func(*wltpkg.Carrier) error
}

type carrierStartAttempt struct {
	cancel     context.CancelFunc
	generation uint64
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
	s := &Service{
		Adapter:        boxService.NewAdapter(C.TypeWLT, tag),
		ctx:            ctx,
		logger:         logger,
		options:        options,
		network:        service.FromContext[adapter.NetworkManager](ctx),
		carrierReady:   make(chan struct{}),
		interfaceReady: closedSignal(),
	}
	service.MustRegister[wltpkg.TrafficReadyProvider](ctx, s)
	return s, nil
}

func (s *Service) WaitWltTrafficReady(ctx context.Context, timeoutMillis int64) error {
	return waitWltTrafficReady(ctx, s.ctx, timeoutMillis, func() (trafficReadyWaiter, bool, <-chan struct{}) {
		s.access.RLock()
		defer s.access.RUnlock()
		if s.interfaceUnavailable {
			return nil, s.stopped, s.interfaceReady
		}
		if s.carrier == nil {
			return nil, s.stopped, s.carrierReady
		}
		return s.carrier, s.stopped, s.carrierReady
	})
}

type trafficReadyWaiter interface {
	WaitTrafficReady(context.Context) error
}

func waitWltTrafficReady(ctx context.Context, serviceCtx context.Context, timeoutMillis int64, snapshot func() (trafficReadyWaiter, bool, <-chan struct{})) error {
	if timeoutMillis <= 0 {
		return context.DeadlineExceeded
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMillis)*time.Millisecond)
	defer cancel()
	for {
		carrier, stopped, ready := snapshot()
		if stopped {
			return errors.New("wlt service is stopped")
		}
		if carrier != nil {
			err := carrier.WaitTrafficReady(waitCtx)
			// The signal belongs to this exact carrier instance. A handover may
			// replace it while the wait is blocked; never accept readiness from
			// the obsolete generation.
			currentCarrier, currentStopped, _ := snapshot()
			if currentStopped {
				return errors.New("wlt service is stopped")
			}
			if currentCarrier != carrier {
				continue
			}
			if err != nil {
				return err
			}
			return waitCtx.Err()
		}
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-serviceCtx.Done():
			return serviceCtx.Err()
		case <-ready:
		}
	}
}

func (s *Service) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateInitialize {
		return nil
	}
	s.restart.Lock()
	defer s.restart.Unlock()
	currentInterfaceKey := s.currentInterfaceKey()
	s.access.Lock()
	if s.carrier != nil {
		s.access.Unlock()
		return nil
	}
	if s.stopped {
		s.access.Unlock()
		return E.New("wlt service is stopped")
	}
	s.initialized = true
	if s.network != nil && currentInterfaceKey == "" && !s.interfaceUnavailable {
		s.interfaceUnavailable = true
		closeSignal(s.interfaceReady)
		s.interfaceReady = make(chan struct{})
		s.interfaceGeneration++
	}
	if s.interfaceUnavailable {
		s.access.Unlock()
		return nil
	}
	s.interfaceKey = currentInterfaceKey
	generation := s.interfaceGeneration
	startCtx, cancelStart := context.WithCancel(s.ctx)
	startAttempt := &carrierStartAttempt{cancel: cancelStart, generation: generation}
	s.restartAttempt = startAttempt
	s.access.Unlock()
	published := false
	defer func() {
		if !published {
			cancelStart()
		}
		s.access.Lock()
		if s.restartAttempt == startAttempt {
			s.restartAttempt = nil
		}
		s.access.Unlock()
	}()

	startedAt := time.Now()
	s.logger.Info("wlt service starting transport=", s.options.Transport)
	// A persisted snapshot is the durable provider identity for unattended
	// restarts. Prefer it even on the first service start; otherwise an expired
	// inline profile snapshot sends the client into anonymous authorization
	// before the core can perform the identity-only refresh.
	carrier, err := s.startCarrier(startCtx, persistentAuthSnapshotAvailable(s.options))
	if err != nil {
		// Keep the sing-box instance available while the WLT provider identity or
		// underlay recovers. WLT outbounds wait on carrierReady with their own dial
		// context, while unrelated/direct outbounds remain usable. Recovery is
		// unattended and serialized by the same restart lock used for handovers.
		s.access.Lock()
		current := s.restartAttempt == startAttempt && !s.stopped && !s.interfaceUnavailable && s.interfaceGeneration == generation && s.carrier == nil && startCtx.Err() == nil
		if current {
			s.stopped = false
			s.interfaceKey = s.currentInterfaceKey()
			s.restartAttempt = nil
		}
		s.access.Unlock()
		cancelStart()
		if !current {
			return nil
		}
		s.logger.Error("wlt service start degraded elapsed=", time.Since(startedAt).String(), " error=", err)
		go s.restartCarrier(nil, "initial startup degraded", generation)
		return nil
	}
	s.access.Lock()
	current := s.restartAttempt == startAttempt && !s.stopped && !s.interfaceUnavailable && s.interfaceGeneration == generation && s.carrier == nil && startCtx.Err() == nil
	if !current {
		s.access.Unlock()
		_ = s.abortCarrier(carrier)
		return nil
	}
	s.carrier = carrier
	s.carrierGeneration++
	s.notifyDialStateLocked()
	s.stopped = false
	s.interfaceKey = s.currentInterfaceKey()
	s.carrierStartCancel = cancelStart
	s.restartAttempt = nil
	close(s.carrierReady)
	s.access.Unlock()
	published = true
	s.markCarrierReady(carrier)
	s.startStatsHeartbeat(carrier)
	s.logger.Info("wlt service started elapsed=", time.Since(startedAt).String())
	return nil
}

func (s *Service) markCarrierReady(carrier *wltpkg.Carrier) {
	if s.markCarrierReadyHook != nil {
		s.markCarrierReadyHook(carrier)
		return
	}
	carrier.MarkSingBoxReady()
}

func (s *Service) abortCarrier(carrier *wltpkg.Carrier) error {
	if s.abortCarrierHook != nil {
		return s.abortCarrierHook(carrier)
	}
	return carrier.Abort()
}

func (s *Service) startCarrier(ctx context.Context, preferPersistedAuth bool) (*wltpkg.Carrier, error) {
	if s.startCarrierHook != nil {
		return s.startCarrierHook(ctx, preferPersistedAuth)
	}
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
	return wltpkg.StartCarrier(ctx, wltpkg.CarrierOptions{
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
	s.statsAccess.Lock()
	if s.statsCancel != nil {
		s.statsCancel()
		s.statsCancel = nil
	}
	s.access.Lock()
	carrier := s.carrier
	restartAttempt := s.restartAttempt
	carrierStartCancel := s.carrierStartCancel
	unavailable := s.interfaceUnavailable
	s.retireDialLeasesLocked(false)
	s.carrier = nil
	s.carrierStartCancel = nil
	s.stopped = true
	s.notifyDialStateLocked()
	s.interfaceUnavailable = false
	s.initialized = false
	closeSignal(s.interfaceReady)
	select {
	case <-s.carrierReady:
	default:
		close(s.carrierReady)
	}
	s.access.Unlock()
	s.statsAccess.Unlock()
	if restartAttempt != nil {
		restartAttempt.cancel()
	}
	if carrier == nil {
		if carrierStartCancel != nil {
			carrierStartCancel()
		}
		return nil
	}
	stats := carrier.Stats()
	var err error
	if unavailable {
		// A concurrent loss callback has already declared this carrier obsolete.
		// Do not let graceful Close win closeOnce before that callback's Abort.
		err = s.abortCarrier(carrier)
	} else {
		err = carrier.Close()
	}
	if carrierStartCancel != nil {
		carrierStartCancel()
	}
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
		interfaceReady := s.interfaceReady
		stopped := s.stopped
		s.access.RUnlock()
		if carrier != nil {
			if interfaceReady != nil {
				select {
				case <-interfaceReady:
				default:
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-s.ctx.Done():
						return nil, s.ctx.Err()
					case <-interfaceReady:
						continue
					}
				}
			}
			// The loss edge closes the previous generation's gate before installing
			// the new closed gate. Revalidate the complete snapshot so a waiter that
			// observed the old closed signal cannot return its stale carrier.
			s.access.RLock()
			current := s.carrier == carrier && s.interfaceReady == interfaceReady && !s.interfaceUnavailable && !s.stopped
			s.access.RUnlock()
			if current {
				return carrier, nil
			}
			continue
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
	// Read the platform-selected identity while holding the same admission lock
	// that commits the generation. A native-loss callback can otherwise clear
	// admission between the network read and beginInterfaceUpdate, allowing a
	// stale recovery callback to reopen the lost generation.
	s.access.Lock()
	currentInterfaceKey := s.currentInterfaceKey()
	carrier, previousInterfaceKey, generation, ignored, cancelAttempt := s.beginInterfaceUpdateLocked(currentInterfaceKey, runtime.GOOS)
	s.access.Unlock()
	if cancelAttempt != nil {
		cancelAttempt()
	}
	s.finishInterfaceUpdate(carrier, previousInterfaceKey, currentInterfaceKey, generation, ignored, interfaceRecoveryGraceFor(runtime.GOOS))
}

func (s *Service) interfaceUpdated(currentInterfaceKey string, goos string, recoveryDelay time.Duration) {
	carrier, previousInterfaceKey, generation, ignored := s.beginInterfaceUpdate(currentInterfaceKey, goos)
	s.finishInterfaceUpdate(carrier, previousInterfaceKey, currentInterfaceKey, generation, ignored, recoveryDelay)
}

func (s *Service) finishInterfaceUpdate(carrier *wltpkg.Carrier, previousInterfaceKey string, currentInterfaceKey string, generation uint64, ignored bool, recoveryDelay time.Duration) {
	if ignored {
		s.logger.Info("wlt service ignoring duplicate Android default interface update")
		return
	}
	if interfaceIdentityChanged(previousInterfaceKey, currentInterfaceKey) {
		s.logger.Info("wlt service default interface identity changed; scheduling carrier replacement after settle delay=", recoveryDelay.String())
	} else {
		s.logger.Info("wlt service default interface changed; preserving active carrier during recovery delay=", recoveryDelay.String())
	}
	// Close the WLT dial gate immediately. The Android client reloads the complete
	// merged runtime after its one-second physical-network debounce so ordinary
	// proxy and URL-test state move together with the WLT carrier. Keep a longer
	// core-side fallback for clients which do not complete that reload; the old
	// service context is canceled before this timer fires during a healthy reload,
	// preventing two carrier replacements from racing each other. Apple retains
	// its allocation-preserving grace.
	go func(expected *wltpkg.Carrier, expectedGeneration uint64) {
		timer := time.NewTimer(recoveryDelay)
		defer timer.Stop()
		select {
		case <-s.ctx.Done():
			s.finishInterfaceRecovery(expectedGeneration)
			return
		case <-timer.C:
		}
		if s.Carrier() != expected || !s.interfaceRecoveryCurrent(expectedGeneration) {
			s.finishInterfaceRecovery(expectedGeneration)
			return
		}
		if expected == nil {
			s.logger.Warn("wlt service starting carrier after interface recovery delay=", recoveryDelay.String())
		} else {
			stats := expected.Stats()
			s.logger.Warn("wlt service replacing carrier after interface recovery delay=", recoveryDelay.String(), " peer_online=", stats.Runtime.Peer.OnlinePeers, " peer_active_data=", stats.Runtime.Peer.ActiveDataPeers, " reconnecting=", stats.Runtime.Reconnecting)
		}
		s.restartCarrier(expected, "default interface changed after recovery delay", expectedGeneration)
		s.finishInterfaceRecovery(expectedGeneration)
	}(carrier, generation)
}

// NetworkUnavailable closes admission for the current interface generation and
// aborts its exact carrier. The carrier pointer remains published behind the
// closed gate so its final raw runtime counters remain observable until a
// replacement generation is committed.
func (s *Service) NetworkUnavailable() {
	s.access.Lock()
	if s.stopped || s.interfaceUnavailable {
		s.access.Unlock()
		return
	}
	s.retireDialLeasesLocked(true)
	s.interfaceUnavailable = true
	s.notifyDialStateLocked()
	closeSignal(s.interfaceReady)
	s.interfaceReady = make(chan struct{})
	s.interfaceGeneration++
	carrier := s.carrier
	restartAttempt := s.restartAttempt
	carrierStartCancel := s.carrierStartCancel
	s.carrierStartCancel = nil
	s.access.Unlock()

	if restartAttempt != nil {
		restartAttempt.cancel()
	}
	if carrier != nil {
		// Abort must win Carrier.closeOnce before canceling the parent start
		// context; its runCtx watcher otherwise begins a graceful drain.
		if err := s.abortCarrier(carrier); err != nil {
			s.logger.Warn("wlt service carrier abort after network loss: ", err)
		}
	}
	if carrierStartCancel != nil {
		carrierStartCancel()
	}
}

func interfaceRecoveryGraceFor(goos string) time.Duration {
	if goos == "android" {
		return wltAndroidInterfaceFallback
	}
	return wltInterfaceRecoveryGrace
}

func ignoreDuplicateInterfaceUpdate(goos string, previous string, current string) bool {
	return goos == "android" && previous != "" && previous == current
}

func (s *Service) beginInterfaceUpdate(currentInterfaceKey string, goos string) (*wltpkg.Carrier, string, uint64, bool) {
	s.access.Lock()
	carrier, previousInterfaceKey, generation, ignored, cancelAttempt := s.beginInterfaceUpdateLocked(currentInterfaceKey, goos)
	s.access.Unlock()
	if cancelAttempt != nil {
		cancelAttempt()
	}
	return carrier, previousInterfaceKey, generation, ignored
}

// beginInterfaceUpdateLocked must be called with s.access held. Keeping the
// current interface lookup and admission commit in this critical section is
// what prevents an in-flight native loss from being resurrected by recovery.
func (s *Service) beginInterfaceUpdateLocked(currentInterfaceKey string, goos string) (*wltpkg.Carrier, string, uint64, bool, context.CancelFunc) {
	var cancelAttempt context.CancelFunc
	carrier := s.carrier
	previousInterfaceKey := s.interfaceKey
	wasUnavailable := s.interfaceUnavailable
	if s.stopped {
		return carrier, previousInterfaceKey, s.interfaceGeneration, true, nil
	}
	if !s.initialized {
		if wasUnavailable && currentInterfaceKey != "" {
			s.interfaceKey = currentInterfaceKey
			s.interfaceUnavailable = false
			closeSignal(s.interfaceReady)
			s.interfaceGeneration++
		}
		return carrier, previousInterfaceKey, s.interfaceGeneration, true, nil
	}
	if currentInterfaceKey == "" {
		return carrier, previousInterfaceKey, s.interfaceGeneration, true, nil
	}
	if !wasUnavailable && (ignoreDuplicateInterfaceUpdate(goos, previousInterfaceKey, currentInterfaceKey) ||
		(carrier == nil && previousInterfaceKey != "" && previousInterfaceKey == currentInterfaceKey)) {
		return carrier, previousInterfaceKey, s.interfaceGeneration, true, nil
	}
	if s.restartAttempt != nil {
		cancelAttempt = s.restartAttempt.cancel
	}
	s.retireDialLeasesLocked(true)
	s.interfaceKey = currentInterfaceKey
	s.interfaceUnavailable = false
	s.notifyDialStateLocked()
	closeSignal(s.interfaceReady)
	s.interfaceReady = make(chan struct{})
	s.interfaceGeneration++
	return carrier, previousInterfaceKey, s.interfaceGeneration, false, cancelAttempt
}

func closedSignal() chan struct{} {
	ready := make(chan struct{})
	close(ready)
	return ready
}

func closeSignal(signal chan struct{}) {
	if signal == nil {
		return
	}
	select {
	case <-signal:
	default:
		close(signal)
	}
}

func (s *Service) beginInterfaceRecovery() uint64 {
	s.access.Lock()
	defer s.access.Unlock()
	closeSignal(s.interfaceReady)
	s.interfaceReady = make(chan struct{})
	s.interfaceGeneration++
	return s.interfaceGeneration
}

func (s *Service) finishInterfaceRecovery(generation uint64) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.interfaceGeneration != generation {
		return
	}
	closeSignal(s.interfaceReady)
	s.notifyDialStateLocked()
}

func (s *Service) interfaceRecoveryCurrent(generation uint64) bool {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.interfaceGeneration == generation
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

func formatWLTStatsHeartbeat(messages ...any) string {
	return fmt.Sprint(messages...)
}

func (s *Service) startStatsHeartbeat(carrier *wltpkg.Carrier) {
	s.statsAccess.Lock()
	s.access.RLock()
	current := s.carrier == carrier && !s.stopped
	s.access.RUnlock()
	if !current {
		s.statsAccess.Unlock()
		return
	}
	if s.statsCancel != nil {
		s.statsCancel()
	}
	statsCtx, cancel := context.WithCancel(s.ctx)
	s.statsCancel = cancel
	s.statsAccess.Unlock()
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
						go s.restartCarrierCurrent(carrier, "reconnect outage: "+rt.LastReconnectReason)
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

func (s *Service) stopStatsHeartbeat() {
	s.statsAccess.Lock()
	if s.statsCancel != nil {
		s.statsCancel()
		s.statsCancel = nil
	}
	s.statsAccess.Unlock()
}

func (s *Service) restartCarrierCurrent(expected *wltpkg.Carrier, reason string) {
	s.access.RLock()
	generation := s.interfaceGeneration
	current := s.carrier == expected && !s.stopped && !s.interfaceUnavailable
	s.access.RUnlock()
	if current {
		s.restartCarrier(expected, reason, generation)
	}
}

func (s *Service) restartCarrier(expected *wltpkg.Carrier, reason string, expectedGeneration uint64) {
	s.restart.Lock()
	defer s.restart.Unlock()

	s.access.Lock()
	if s.interfaceGeneration != expectedGeneration {
		s.access.Unlock()
		return
	}
	if s.stopped || s.interfaceUnavailable {
		s.access.Unlock()
		return
	}
	if s.carrier != expected {
		s.access.Unlock()
		return
	}
	if expected != nil {
		s.retireDialLeasesLocked(false)
		s.carrier = nil
		s.carrierReady = make(chan struct{})
	} else if s.carrierReady == nil {
		s.carrierReady = make(chan struct{})
	}
	previousCarrierStartCancel := s.carrierStartCancel
	s.carrierStartCancel = nil
	attemptCtx, cancelAttempt := context.WithCancel(s.ctx)
	restartAttempt := &carrierStartAttempt{cancel: cancelAttempt, generation: expectedGeneration}
	s.restartAttempt = restartAttempt
	s.access.Unlock()
	published := false
	defer func() {
		if !published {
			cancelAttempt()
		}
		s.access.Lock()
		if s.restartAttempt == restartAttempt {
			s.restartAttempt = nil
		}
		s.access.Unlock()
	}()
	s.stopStatsHeartbeat()

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
		if err := s.abortCarrier(expected); err != nil {
			s.logger.Warn("wlt service old carrier abort error: ", err)
		}
		s.logger.Info("wlt service old carrier aborted elapsed=", time.Since(startedAt).String())
	}
	if previousCarrierStartCancel != nil {
		previousCarrierStartCancel()
	}

	for attemptNumber := 1; ; attemptNumber++ {
		s.access.RLock()
		stopped := s.stopped
		unavailable := s.interfaceUnavailable
		currentAttempt := s.restartAttempt
		generationCurrent := s.interfaceGeneration == restartAttempt.generation
		s.access.RUnlock()
		if stopped || unavailable || currentAttempt != restartAttempt || !generationCurrent || attemptCtx.Err() != nil {
			return
		}
		startedAt := time.Now()
		s.logger.Info("wlt service carrier restart attempt=", attemptNumber, " reason=", reason)
		carrier, err := s.startCarrier(attemptCtx, true)
		if err == nil {
			s.access.Lock()
			current := s.restartAttempt == currentAttempt && !s.stopped && !s.interfaceUnavailable && s.interfaceGeneration == restartAttempt.generation && attemptCtx.Err() == nil
			if !current {
				s.access.Unlock()
				_ = s.abortCarrier(carrier)
				return
			}
			s.carrier = carrier
			s.carrierGeneration++
			s.notifyDialStateLocked()
			s.interfaceKey = s.currentInterfaceKey()
			s.carrierStartCancel = cancelAttempt
			close(s.carrierReady)
			s.restartAttempt = nil
			s.access.Unlock()
			published = true
			s.markCarrierReady(carrier)
			s.startStatsHeartbeat(carrier)
			s.logger.Info("wlt service carrier restarted elapsed=", time.Since(startedAt).String(), " attempts=", attemptNumber)
			return
		}
		if attemptNumber <= wltCarrierRestartRetryMaxLog || attemptNumber%10 == 0 {
			s.logger.Error("wlt service carrier restart failed attempt=", attemptNumber, " error=", err)
		}
		timer := time.NewTimer(carrierRestartRetryDelay(attemptNumber, err))
		select {
		case <-attemptCtx.Done():
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
