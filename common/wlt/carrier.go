//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

type logfSlogHandler struct {
	logf     func(string, ...any)
	observer func(slog.Record)
	attrs    []slog.Attr
	group    string
}

func newLogfSlogLogger(logf func(string, ...any)) *slog.Logger {
	return newLogfSlogLoggerWithObserver(logf, nil)
}

func newLogfSlogLoggerWithObserver(logf func(string, ...any), observer func(slog.Record)) *slog.Logger {
	if logf == nil {
		logf = log.Printf
	}
	return slog.New(&logfSlogHandler{logf: logf, observer: observer})
}

func (h *logfSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *logfSlogHandler) Handle(_ context.Context, record slog.Record) error {
	if h.logf == nil {
		return nil
	}
	if record.Level < slog.LevelInfo {
		return nil
	}
	if h.observer != nil {
		h.observer(record)
	}
	var b strings.Builder
	b.WriteString("wlt-carrier ")
	b.WriteString(strings.ToLower(record.Level.String()))
	b.WriteString(": ")
	b.WriteString(record.Message)
	writeAttr := func(attr slog.Attr) {
		attr.Value = attr.Value.Resolve()
		if attr.Key == "" {
			return
		}
		b.WriteByte(' ')
		b.WriteString(attr.Key)
		b.WriteByte('=')
		b.WriteString(attr.Value.String())
	}
	for _, attr := range h.attrs {
		writeAttr(attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		writeAttr(attr)
		return true
	})
	h.logf("%s", b.String())
	return nil
}

func (h *logfSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *h
	next.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &next
}

func (h *logfSlogHandler) WithGroup(name string) slog.Handler {
	next := *h
	next.group = name
	return &next
}

const (
	defaultCarrierConnectTimeout = 10 * time.Second
	defaultCarrierMaxActive      = 32
	defaultCarrierMaxOpen        = 16
	defaultCarrierMaxPending     = 24
	defaultCarrierQueueTimeout   = 1500 * time.Millisecond
	defaultCarrierIdleTimeout    = 20 * time.Second
	defaultCarrierBufferSize     = 32 * 1024

	defaultCarrierTinyMuxFlowBuffer     = 128
	defaultCarrierTinyMuxFlowSendBuffer = 32
	defaultCarrierTinyMuxControlBuffer  = 64
	defaultCarrierTinyMuxPingTimeout    = 20 * time.Second
	defaultCarrierPeerIncomingBuffer    = 128
	defaultCarrierPeerWriteBuffer       = 32
	defaultCarrierSRTPPacketBuffer      = 256
	defaultCarrierKCPWindowSize         = 512
	defaultCarrierKCPReadWriteBuffer    = 512 * 1024

	carrierConnectRetryInitial = 250 * time.Millisecond
	carrierConnectRetryMax     = 2 * time.Second
	// A platform signaling close can leave one carrier Connect call waiting for
	// its outer deadline even though the underlying attempt is already dead.
	// Keep attempts short so a transient close gets another bounded chance while
	// preserving the configured total startup budget.
	// One carrier attempt must outlive the carrier's ten-second mobile
	// underlay budget. Otherwise the adapter cancels a healthy DTLS/SRTP
	// exchange before its own bounded timeout can make the decision.
	carrierConnectAttemptMax = 20 * time.Second

	carrierReconnectRetryInitial = 20 * time.Millisecond
	carrierReconnectRetryMax     = 250 * time.Millisecond

	defaultAuthSnapshotFetchTimeout   = 5 * time.Second
	defaultAuthSnapshotRefreshTimeout = 15 * time.Second
	authSnapshotRefreshBeforeExpiry   = 5 * time.Minute
	maxAuthSnapshotBytes              = 256 * 1024
	authSnapshotPreviousSuffix        = ".previous"
	providerCooldownSuffix            = ".provider-cooldown"
	providerCooldownVersion           = 1
	providerCooldownInitial           = 6 * time.Hour
	providerCooldownMaximum           = 48 * time.Hour
)

type providerCooldownState struct {
	Version      int   `json:"version"`
	BlockedUntil int64 `json:"blocked_until"`
	Attempts     int   `json:"attempts"`
}

var (
	refreshCarrierAuthSnapshot          = carrierengine.RefreshAuthSnapshotContext
	prewarmCarrierAuthSnapshot          = carrierengine.PrewarmAuthUnattended
	promoteCarrierAuthSnapshot          = carrierengine.PromoteAuthSnapshot
	fetchCarrierAuthSnapshotForRecovery = fetchCarrierAuthSnapshot
	connectCarrierClientForStart        = connectCarrierClient
)

type CarrierOptions struct {
	Config     string
	ConfigFile string

	AuthSnapshot             string
	AuthSnapshotFile         string
	AuthSnapshotURL          string
	AuthSnapshotFetchTimeout time.Duration
	AuthSnapshotOutputFile   string
	// ConfigTrustedAt is accepted for compatibility with older clients. The
	// locally saved profile supplied by the client is the durable trust boundary
	// and does not expire on a calendar deadline.
	ConfigTrustedAt        int64
	AuthSnapshotPreferFile bool
	AuthSnapshotSkipRemote bool

	ConnectTimeout      time.Duration
	MaxActiveStreams    int
	MaxOpenAttempts     int
	DNSOpenReserve      int
	MaxPendingDials     int
	DialQueueTimeout    time.Duration
	IdleTimeout         time.Duration
	PressureIdleTimeout time.Duration
	BufferSize          int

	TinyMuxFlowBuffer            int
	TinyMuxFlowSendBuffer        int
	TinyMuxControlBuffer         int
	TinyMuxRateBurstBytes        int
	TinyMuxPingTimeout           time.Duration
	PeerIncomingBuffer           int
	PeerWriteBuffer              int
	AdaptivePeerData             bool
	AdaptivePeerThresholdBytes   int
	AdaptivePeerIdleTimeout      time.Duration
	SRTPPacketBuffer             int
	KCPWindowSize                int
	KCPReadWriteBuffer           int
	RelayBandwidthBytesPerSecond int
	SocketControl                carriercommon.SocketControlFunc

	Logger func(string, ...any)

	startup *carrierStartupTelemetry
}

type carrierStartupContextKey struct{}

type carrierStartupTelemetry struct {
	startedAt time.Time
	logf      func(string, ...any)
	rateMu    sync.RWMutex
	rateLimit bool

	snapshotChecked      sync.Once
	providerRefreshStart sync.Once
	providerRefreshReady sync.Once
	providerReady        sync.Once
	turnReady            sync.Once
	peerReady            sync.Once
	carrierReady         sync.Once
	singBoxReady         sync.Once
	firstPacket          sync.Once
	trafficReady         sync.Once
}

func newCarrierStartupTelemetry(startedAt time.Time, logf func(string, ...any)) *carrierStartupTelemetry {
	return &carrierStartupTelemetry{startedAt: startedAt, logf: logf}
}

func contextWithCarrierStartup(ctx context.Context, startup *carrierStartupTelemetry) context.Context {
	return context.WithValue(ctx, carrierStartupContextKey{}, startup)
}

func carrierStartupFromContext(ctx context.Context) *carrierStartupTelemetry {
	startup, _ := ctx.Value(carrierStartupContextKey{}).(*carrierStartupTelemetry)
	return startup
}

func (t *carrierStartupTelemetry) mark(once *sync.Once, phase string, detail string) {
	if t == nil || t.logf == nil {
		return
	}
	once.Do(func() {
		if detail == "" {
			t.logf("WLT startup phase=%s elapsed_ms=%d", phase, time.Since(t.startedAt).Milliseconds())
			return
		}
		t.logf("WLT startup phase=%s elapsed_ms=%d %s", phase, time.Since(t.startedAt).Milliseconds(), detail)
	})
}

func (t *carrierStartupTelemetry) observe(record slog.Record) {
	if t == nil {
		return
	}
	switch record.Message {
	case "relay client session phase=platform_authorize_done":
		t.mark(&t.providerReady, "provider_ready", "")
	case "relay client session phase=signaling_turn_refresh_done":
		t.mark(&t.turnReady, "turn_ready", "outcome=refreshed")
	case "relay client session phase=signaling_turn_refresh_unavailable":
		t.mark(&t.turnReady, "turn_ready", "outcome=cached")
	case "relay client session connected":
		t.mark(&t.peerReady, "peer_ready", "")
	}
}

func (t *carrierStartupTelemetry) markSnapshotChecked() {
	if t == nil {
		return
	}
	t.mark(&t.snapshotChecked, "snapshot_checked", "")
}

func (t *carrierStartupTelemetry) markProviderRefreshStarted() {
	if t == nil {
		return
	}
	t.mark(&t.providerRefreshStart, "provider_refresh_started", "")
}

func (t *carrierStartupTelemetry) markProviderRefreshReady(outcome string) {
	if t == nil {
		return
	}
	t.mark(&t.providerRefreshReady, "provider_refresh_ready", "outcome="+outcome)
}

func (t *carrierStartupTelemetry) markProviderRateLimited() {
	if t == nil {
		return
	}
	t.rateMu.Lock()
	t.rateLimit = true
	t.rateMu.Unlock()
}

func (t *carrierStartupTelemetry) providerRateLimited() bool {
	if t == nil {
		return false
	}
	t.rateMu.RLock()
	limited := t.rateLimit
	t.rateMu.RUnlock()
	return limited
}

func (t *carrierStartupTelemetry) markCarrierReady() {
	if t == nil {
		return
	}
	t.mark(&t.carrierReady, "carrier_ready", "")
}

func (t *carrierStartupTelemetry) markSingBoxReady() {
	if t == nil {
		return
	}
	t.mark(&t.singBoxReady, "sing_box_ready", "")
}

func (t *carrierStartupTelemetry) markFirstPacket(direction string) {
	if t == nil {
		return
	}
	t.mark(&t.firstPacket, "first_packet", "direction="+direction)
}

func (t *carrierStartupTelemetry) markTrafficReady() {
	if t == nil {
		return
	}
	t.mark(&t.trafficReady, "traffic_ready", "")
}

type CarrierConfigOptions struct {
	Config     string
	ConfigFile string
}

type CarrierStats struct {
	ActiveStreams        int64
	PeakActiveStreams    int64
	PendingDials         int64
	PeakPendingDials     int64
	OpenAttempts         int64
	DNSOpenRequests      int64
	DNSOpenQueued        int64
	DNSOpenRejected      int64
	OpenedStreams        int64
	ClosedStreams        int64
	QueuedDials          int64
	RejectedStreams      int64
	RejectedQueue        int64
	RejectedActive       int64
	RejectedOpen         int64
	PressureIdleReclaims int64
	FailedStreams        int64
	LastActiveWaitMillis int64
	MaxActiveWaitMillis  int64
	LastOpenWaitMillis   int64
	MaxOpenWaitMillis    int64
	LastDialMillis       int64
	MaxDialMillis        int64
	ReconnectRetries     int64
	ReconnectWaitMillis  int64
	Runtime              carrierconfig.RuntimeStats
}

type Carrier struct {
	client               *carrierengine.Client
	cancel               context.CancelFunc
	restoreSocketControl func()

	dialRoute func(context.Context, string) (net.Conn, error)
	waitReady func(context.Context) error
	logf      func(string, ...any)
	startup   *carrierStartupTelemetry

	connectTimeout      time.Duration
	dialQueueTimeout    time.Duration
	idleTimeout         time.Duration
	pressureIdleTimeout time.Duration
	bufferSize          int
	activeSlots         chan struct{}
	openSlots           chan struct{}
	openGate            *prioritySlotGate
	pendingSlots        chan struct{}
	routeByClass        map[string]string

	activeStreams        atomic.Int64
	peakActiveStreams    atomic.Int64
	pendingDials         atomic.Int64
	peakPendingDials     atomic.Int64
	openAttempts         atomic.Int64
	dnsOpenRequests      atomic.Int64
	dnsOpenQueued        atomic.Int64
	dnsOpenRejected      atomic.Int64
	openedStreams        atomic.Int64
	closedStreams        atomic.Int64
	queuedDials          atomic.Int64
	rejectedStreams      atomic.Int64
	rejectedQueue        atomic.Int64
	rejectedActive       atomic.Int64
	rejectedOpen         atomic.Int64
	failedStreams        atomic.Int64
	lastActiveWait       atomic.Int64
	maxActiveWait        atomic.Int64
	lastOpenWait         atomic.Int64
	maxOpenWait          atomic.Int64
	lastDialDuration     atomic.Int64
	maxDialDuration      atomic.Int64
	reconnectRetries     atomic.Int64
	reconnectWait        atomic.Int64
	closeOnce            sync.Once
	closeErr             error
	streamsMu            sync.Mutex
	streams              map[*carrierConn]struct{}
	pressureIdleReclaims atomic.Int64
}

func StartCarrier(ctx context.Context, options CarrierOptions) (*Carrier, error) {
	logf := options.Logger
	if logf == nil {
		logf = log.Printf
	}
	startedAt := time.Now()
	startup := newCarrierStartupTelemetry(startedAt, logf)
	options.startup = startup
	loadProviderCooldown(options, logf)
	if logf != nil {
		logf("WLT carrier start phase=load_config source=%s", carrierConfigSource(options))
	}
	cfg, err := loadCarrierClientConfig(CarrierConfigOptions{
		Config:     options.Config,
		ConfigFile: options.ConfigFile,
	})
	if err != nil {
		if logf != nil {
			logf("WLT carrier start failed phase=load_config elapsed=%s error=%v", time.Since(startedAt), err)
		}
		return nil, err
	}
	if logf != nil {
		logf("WLT carrier start phase=config_ready type=%s platform=%s routes=%d route_ids=%s peers=%d force_turn=%t proto=%s gateway_configured=%t elapsed=%s",
			safeLogValue(cfg.Type),
			safeLogValue(cfg.PlatformID),
			len(cfg.Routes),
			strings.Join(carrierRouteIDs(cfg), ","),
			cfg.Peers,
			cfg.ForceTurn,
			safeLogValue(cfg.Proto),
			strings.TrimSpace(cfg.Gateway) != "",
			time.Since(startedAt),
		)
	}
	if err := loadCarrierAuthSnapshot(ctx, cfg, options, logf); err != nil {
		if logf != nil {
			logf("WLT carrier start failed phase=auth_snapshot elapsed=%s error=%v", time.Since(startedAt), err)
		}
		return nil, err
	}
	startup.markSnapshotChecked()
	startup.markProviderRefreshReady("not_needed")
	options = withCarrierDefaults(options)
	if options.DNSOpenReserve < 0 || options.DNSOpenReserve >= options.MaxOpenAttempts {
		return nil, fmt.Errorf("dns_open_reserve must be between 0 and max_open_attempts-1 (reserve=%d max_open=%d)", options.DNSOpenReserve, options.MaxOpenAttempts)
	}
	runCtx, cancel := context.WithCancel(contextWithCarrierStartup(ctx, startup))
	var restoreSocketControl func()
	if options.SocketControl != nil {
		restoreSocketControl = carriercommon.SetSocketControl(options.SocketControl)
	}
	transportOptions := applyCarrierRuntimeOptions(options)
	if logf != nil {
		logf("WLT carrier start phase=runtime_options max_active=%d max_open=%d dns_open_reserve=%d max_pending=%d queue_timeout=%s connect_timeout=%s idle_timeout=%s pressure_idle_timeout=%s buffer_size=%d mux_flow_buffer=%d mux_send_buffer=%d mux_control_buffer=%d mux_burst=%d peer_incoming_buffer=%d peer_write_buffer=%d srtp_packet_buffer=%d kcp_window=%d kcp_buffer=%d relay_bandwidth=%d elapsed=%s",
			options.MaxActiveStreams,
			options.MaxOpenAttempts,
			options.DNSOpenReserve,
			options.MaxPendingDials,
			options.DialQueueTimeout,
			options.ConnectTimeout,
			options.IdleTimeout,
			options.PressureIdleTimeout,
			options.BufferSize,
			transportOptions.TinyMuxFlowBuffer,
			transportOptions.TinyMuxFlowSendBuffer,
			transportOptions.TinyMuxControlBuffer,
			transportOptions.TinyMuxRateBurstBytes,
			transportOptions.PeerIncomingBuffer,
			transportOptions.PeerWriteBuffer,
			transportOptions.SRTPPacketBuffer,
			transportOptions.KCPWindowSize,
			transportOptions.KCPReadWriteBuffer,
			transportOptions.RelayBandwidthBytesPerSecond,
			time.Since(startedAt),
		)
	}

	runtimeClient, err := connectCarrierClientForStart(runCtx, cfg, options.ConnectTimeout, logf)
	if carrierAuthRecoveryRequired(err) {
		if logf != nil {
			logf("WLT carrier auth event=reauthorization_required source=current phase=turn_auth_recovery")
		}
		runtimeClient, err = recoverCarrierAuthAfterRejection(runCtx, cfg, options, err, logf)
	}
	if err != nil {
		cancel()
		if restoreSocketControl != nil {
			restoreSocketControl()
		}
		if logf != nil {
			logf("WLT carrier start failed phase=connect elapsed=%s error=%v", time.Since(startedAt), err)
		}
		return nil, err
	}
	if err := promoteCarrierAuthSnapshot(*cfg); err != nil {
		_ = runtimeClient.Stop()
		cancel()
		if restoreSocketControl != nil {
			restoreSocketControl()
		}
		if logf != nil {
			logf("WLT carrier start failed phase=auth_snapshot_promotion elapsed=%s error=%v", time.Since(startedAt), err)
		}
		return nil, fmt.Errorf("promote WLT carrier auth snapshot: %w", err)
	}
	clearProviderCooldown(options, logf)
	if logf != nil {
		logf("WLT carrier auth snapshot promoted after successful connect")
	}
	if err := saveCarrierAuthSnapshot(options, cfg, logf); err != nil && logf != nil {
		logf("WLT carrier auth snapshot save failed error=%v", err)
	}

	carrier := &Carrier{
		client:               runtimeClient,
		cancel:               cancel,
		restoreSocketControl: restoreSocketControl,
		dialRoute:            runtimeClient.DialRouteContext,
		waitReady:            runtimeClient.WaitReady,
		logf:                 logf,
		startup:              startup,
		connectTimeout:       options.ConnectTimeout,
		dialQueueTimeout:     options.DialQueueTimeout,
		idleTimeout:          options.IdleTimeout,
		pressureIdleTimeout:  options.PressureIdleTimeout,
		bufferSize:           options.BufferSize,
		activeSlots:          make(chan struct{}, options.MaxActiveStreams),
		openSlots:            make(chan struct{}, options.MaxOpenAttempts),
		pendingSlots:         make(chan struct{}, options.MaxPendingDials),
		streams:              make(map[*carrierConn]struct{}),
		routeByClass:         carrierRouteMap(cfg),
	}
	if options.DNSOpenReserve > 0 {
		carrier.openGate = newPrioritySlotGate(options.MaxOpenAttempts, options.DNSOpenReserve)
	}
	go func() {
		<-runCtx.Done()
		_ = carrier.Close()
	}()
	logf("WLT carrier started routes=%d classes=%s max_active=%d max_open=%d dns_open_reserve=%d max_pending=%d queue_timeout=%s connect_timeout=%s idle_timeout=%s pressure_idle_timeout=%s buffer_size=%d kcp_window=%d kcp_buffer=%d mux_flow_buffer=%d mux_send_buffer=%d mux_control_buffer=%d mux_burst=%d mux_ping_timeout_ms=%d peer_incoming_buffer=%d peer_write_buffer=%d adaptive_peer_data=%t adaptive_peer_threshold=%d adaptive_peer_idle_ms=%d srtp_packet_buffer=%d relay_bandwidth=%d",
		len(cfg.Routes),
		strings.Join(carrierRouteClasses(carrier.routeByClass), ","),
		options.MaxActiveStreams,
		options.MaxOpenAttempts,
		options.DNSOpenReserve,
		options.MaxPendingDials,
		options.DialQueueTimeout,
		options.ConnectTimeout,
		options.IdleTimeout,
		options.PressureIdleTimeout,
		options.BufferSize,
		transportOptions.KCPWindowSize,
		transportOptions.KCPReadWriteBuffer,
		transportOptions.TinyMuxFlowBuffer,
		transportOptions.TinyMuxFlowSendBuffer,
		transportOptions.TinyMuxControlBuffer,
		transportOptions.TinyMuxRateBurstBytes,
		transportOptions.TinyMuxPingTimeoutMillis,
		transportOptions.PeerIncomingBuffer,
		transportOptions.PeerWriteBuffer,
		transportOptions.AdaptivePeerData,
		transportOptions.AdaptivePeerThresholdBytes,
		transportOptions.AdaptivePeerIdleMillis,
		transportOptions.SRTPPacketBuffer,
		transportOptions.RelayBandwidthBytesPerSecond,
	)
	startup.markCarrierReady()
	return carrier, nil
}

func carrierAuthRecoveryRequired(err error) bool {
	return errors.Is(err, carriercommon.ErrAuthSnapshotReauthorizationRequired) ||
		errors.Is(err, carriercommon.ErrManualCaptchaUnavailable)
}

func connectCarrierClient(ctx context.Context, cfg *carrierconfig.ClientConfig, timeout time.Duration, logf func(string, ...any)) (*carrierengine.Client, error) {
	startedAt := time.Now()
	connectCtx, connectCancel := context.WithTimeout(ctx, timeout)
	defer connectCancel()

	delay := carrierConnectRetryInitial
	var lastErr error
	for attempt := 1; ; attempt++ {
		attemptStartedAt := time.Now()
		runtimeClient := carrierengine.NewClient(*cfg)
		runtimeClient.SetLogger(newLogfSlogLoggerWithObserver(logf, func(record slog.Record) {
			if startup := carrierStartupFromContext(ctx); startup != nil {
				startup.observe(record)
			}
		}))
		if logf != nil {
			logf("WLT carrier connect attempt started attempt=%d timeout=%s routes=%d peers=%d elapsed=%s", attempt, timeout, len(cfg.Routes), cfg.Peers, time.Since(startedAt))
		}

		attemptTimeout := timeout
		if attemptTimeout > carrierConnectAttemptMax {
			attemptTimeout = carrierConnectAttemptMax
		}
		attemptCtx, attemptCancel := context.WithTimeout(connectCtx, attemptTimeout)
		connectDone := make(chan error, 1)
		pendingTimer := time.AfterFunc(3*time.Second, func() {
			if logf != nil {
				logf("WLT carrier connect attempt still pending attempt=%d elapsed=%s total_elapsed=%s", attempt, time.Since(attemptStartedAt), time.Since(startedAt))
			}
		})
		go func() {
			connectDone <- runtimeClient.Connect()
		}()

		select {
		case err := <-connectDone:
			attemptCancel()
			pendingTimer.Stop()
			if err == nil {
				if logf != nil {
					logf("WLT carrier connect attempt succeeded attempt=%d elapsed=%s total_elapsed=%s", attempt, time.Since(attemptStartedAt), time.Since(startedAt))
				}
				if attempt > 1 && logf != nil {
					logf("WLT carrier connect recovered attempts=%d", attempt)
				}
				return runtimeClient, nil
			}
			lastErr = err
			_ = runtimeClient.Stop()
			if isFatalCarrierConnectError(err) {
				if logf != nil {
					logf("WLT carrier connect failed permanently attempt=%d elapsed=%s error=%v", attempt, time.Since(startedAt), err)
				}
				return nil, fmt.Errorf("connect WLT carrier: %w", err)
			}
		case <-attemptCtx.Done():
			pendingTimer.Stop()
			_ = runtimeClient.Stop()
			attemptCancel()
			if connectCtx.Err() == nil {
				lastErr = fmt.Errorf("carrier connect attempt timed out after %s", attemptTimeout)
				if logf != nil {
					logf("WLT carrier connect attempt timed out attempt=%d timeout=%s total_elapsed=%s", attempt, attemptTimeout, time.Since(startedAt))
				}
				break
			}
			if lastErr != nil {
				return nil, fmt.Errorf("connect WLT carrier: %w; last error: %v", connectCtx.Err(), lastErr)
			}
			return nil, fmt.Errorf("connect WLT carrier: %w", connectCtx.Err())
		}

		select {
		case <-connectCtx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("connect WLT carrier: %w; last error: %v", connectCtx.Err(), lastErr)
			}
			return nil, fmt.Errorf("connect WLT carrier: %w", connectCtx.Err())
		default:
		}

		if logf != nil {
			logf("WLT carrier connect failed attempt=%d retry_in=%s error=%v", attempt, delay, lastErr)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-connectCtx.Done():
			timer.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("connect WLT carrier: %w; last error: %v", connectCtx.Err(), lastErr)
			}
			return nil, fmt.Errorf("connect WLT carrier: %w", connectCtx.Err())
		}
		if delay < carrierConnectRetryMax {
			delay *= 2
			if delay > carrierConnectRetryMax {
				delay = carrierConnectRetryMax
			}
		}
	}
}

func isFatalCarrierConnectError(err error) bool {
	return errors.Is(err, carriercommon.ErrManualCaptchaUnavailable) ||
		errors.Is(err, carriercommon.ErrAuthSnapshotReauthorizationRequired)
}

func refreshCarrierAuthSnapshotAfterRejection(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) error {
	if cfg == nil {
		return errors.New("carrier config is required for auth snapshot recovery")
	}
	var (
		raw    []byte
		source string
	)
	if path := strings.TrimSpace(options.AuthSnapshotFile); path != "" {
		content, err := os.ReadFile(path)
		if err == nil && strings.TrimSpace(string(content)) != "" {
			raw = content
			source = "current"
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read rejected auth snapshot: %w", err)
		}
	}
	if len(raw) == 0 {
		if inline := strings.TrimSpace(options.AuthSnapshot); inline != "" {
			raw = []byte(inline)
			source = "inline"
		}
	}
	if len(raw) == 0 {
		return errors.New("auth snapshot is unavailable for TURN recovery")
	}
	refreshCtx, cancel := context.WithTimeout(ctx, defaultAuthSnapshotRefreshTimeout)
	defer cancel()
	refreshed, err := refreshCarrierAuthSnapshot(refreshCtx, *cfg, raw)
	if err != nil {
		if errors.Is(err, carriercommon.ErrAuthSnapshotReauthorizationRequired) && logf != nil {
			logf("WLT carrier auth event=reauthorization_required source=%s", source)
		}
		return fmt.Errorf("refresh rejected TURN auth snapshot source=%s: %w", source, err)
	}
	if err := carrierengine.ImportAuthSnapshotJSON(refreshed); err != nil {
		return fmt.Errorf("import recovered TURN auth snapshot source=%s: %w", source, err)
	}
	if logf != nil {
		logf("WLT carrier start phase=turn_auth_candidate_ready source=%s persistence=deferred", source)
	}
	return nil
}

func recoverCarrierAuthAfterRejection(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, initialErr error, logf func(string, ...any)) (*carrierengine.Client, error) {
	combinedErr := initialErr
	connectCandidate := func(source string) (*carrierengine.Client, bool) {
		if logf != nil {
			logf("WLT carrier auth event=fallback_started source=%s", source)
		}
		client, err := connectCarrierClientForStart(ctx, cfg, options.ConnectTimeout, logf)
		if err == nil {
			if logf != nil {
				logf("WLT carrier auth event=fallback_succeeded source=%s", source)
			}
			return client, true
		}
		combinedErr = errors.Join(combinedErr, fmt.Errorf("connect %s auth snapshot: %w", source, err))
		if logf != nil {
			logf("WLT carrier auth event=fallback_failed source=%s error=%v", source, err)
		}
		return nil, false
	}

	if options.startup.providerRateLimited() {
		if logf != nil {
			logf("WLT carrier auth event=refresh_skipped source=current reason=provider_rate_limited")
		}
	} else if refreshErr := refreshCarrierAuthSnapshotAfterRejection(ctx, cfg, options, logf); refreshErr != nil {
		combinedErr = errors.Join(combinedErr, refreshErr)
		if errors.Is(refreshErr, carriercommon.ErrHumanChallengeErrorLimit) {
			recordProviderRateLimit(options, logf)
		}
		if logf != nil {
			logf("WLT carrier start phase=turn_auth_refresh_unavailable error=%v", refreshErr)
		}
	} else if client, ok := connectCandidate("refreshed"); ok {
		return client, nil
	}

	previousLoaded, previousErr := loadCarrierPreviousAuthSnapshot(cfg, options, logf)
	if previousErr != nil {
		combinedErr = errors.Join(combinedErr, previousErr)
		if logf != nil {
			logf("WLT carrier auth event=previous_unavailable source=previous error=%v", previousErr)
		}
	} else if previousLoaded {
		if client, ok := connectCandidate("previous"); ok {
			return client, nil
		}
	}

	inline := strings.TrimSpace(options.AuthSnapshot)
	if inline != "" {
		if inlineErr := prepareCarrierAuthSnapshot(ctx, cfg, options, []byte(inline), "inline_recovery", logf); inlineErr != nil {
			combinedErr = errors.Join(combinedErr, inlineErr)
			if logf != nil {
				logf("WLT carrier auth event=inline_unavailable source=inline error=%v", inlineErr)
			}
		} else if client, ok := connectCandidate("inline"); ok {
			return client, nil
		}
	}

	// Explicit TURN/signaling rejection invalidates the provider cache in the
	// carrier module. Only after all local last-known-good candidates failed do
	// we permit one full client-local authorization/challenge attempt. PrewarmAuth
	// has its own bounded provider authorization deadline and does not contact the
	// VPN API or auth_snapshot_url.
	if options.startup.providerRateLimited() {
		if logf != nil {
			logf("WLT carrier auth event=fresh_reauthorization_skipped source=client_local reason=provider_rate_limited")
		}
		return nil, errors.Join(combinedErr, carriercommon.ErrHumanChallengeErrorLimit)
	}
	if logf != nil {
		logf("WLT carrier auth event=fresh_reauthorization_started source=client_local")
	}
	fresh, freshErr := prewarmCarrierAuthSnapshot(*cfg)
	if freshErr != nil {
		if errors.Is(freshErr, carriercommon.ErrHumanChallengeErrorLimit) {
			recordProviderRateLimit(options, logf)
		}
		combinedErr = errors.Join(combinedErr, fmt.Errorf("fresh client-local authorization: %w", freshErr))
		if logf != nil {
			logf("WLT carrier auth event=fresh_reauthorization_failed source=client_local error=%v", freshErr)
		}
		return nil, combinedErr
	}
	if err := carrierengine.ImportAuthSnapshotJSON(fresh); err != nil {
		combinedErr = errors.Join(combinedErr, fmt.Errorf("import fresh client-local authorization: %w", err))
		return nil, combinedErr
	}
	if logf != nil {
		logf("WLT carrier auth event=fresh_reauthorization_ready source=client_local persistence=deferred")
	}
	if client, ok := connectCandidate("fresh"); ok {
		return client, nil
	}
	return nil, combinedErr
}

func loadCarrierAuthSnapshot(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) error {
	if options.AuthSnapshotPreferFile {
		loaded, err := loadCarrierAuthSnapshotFile(ctx, cfg, options, options.AuthSnapshotFile, logf)
		if err != nil {
			return err
		}
		if loaded {
			return nil
		}
		loaded, err = loadCarrierPreviousAuthSnapshot(cfg, options, logf)
		if err != nil {
			return err
		}
		if loaded {
			return nil
		}
	}
	raw := strings.TrimSpace(options.AuthSnapshot)
	if raw != "" {
		// A WLT profile must remain bootstrappable when its control-plane host is
		// unreachable on the unprotected network. Import the snapshot delivered
		// with the profile. Remote synchronization is forbidden before the WLT
		// carrier has established traffic because that would create a
		// control-plane/VPN dependency cycle.
		return prepareCarrierAuthSnapshot(ctx, cfg, options, []byte(raw), "inline", logf)
	}
	if !options.AuthSnapshotPreferFile {
		loaded, err := loadCarrierAuthSnapshotFile(ctx, cfg, options, options.AuthSnapshotFile, logf)
		if err != nil {
			return err
		}
		if loaded {
			return nil
		}
		loaded, err = loadCarrierPreviousAuthSnapshot(cfg, options, logf)
		if err != nil {
			return err
		}
		if loaded {
			return nil
		}
	}
	return nil
}

func loadCarrierAuthSnapshotFile(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, path string, logf func(string, ...any)) (bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return false, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if logf != nil {
				logf("WLT carrier start phase=auth_snapshot_missing source=file")
			}
			return false, nil
		}
		return false, fmt.Errorf("read auth snapshot file: %w", err)
	}
	raw := strings.TrimSpace(string(content))
	if raw == "" {
		if logf != nil {
			logf("WLT carrier start phase=auth_snapshot_empty source=file")
		}
		return false, nil
	}
	if err := carrierengine.ImportAuthSnapshotJSON([]byte(raw)); err != nil {
		if logf != nil {
			logf("WLT carrier start phase=auth_snapshot_ignored source=file error=%v", err)
		}
		return false, nil
	}
	if err := prepareCarrierAuthSnapshot(ctx, cfg, options, []byte(raw), "current", logf); err != nil {
		return false, err
	}
	return true, nil
}

func loadCarrierPreviousAuthSnapshot(cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) (bool, error) {
	if cfg == nil {
		return false, errors.New("carrier config is required for previous auth snapshot")
	}
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return false, nil
	}
	content, err := os.ReadFile(path + authSnapshotPreviousSuffix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read previous auth snapshot: %w", err)
	}
	raw := bytes.TrimSpace(content)
	if len(raw) == 0 {
		return false, nil
	}
	if err := carrierengine.ImportAuthSnapshotJSON(raw); err != nil {
		return false, fmt.Errorf("import previous auth snapshot: %w", err)
	}
	if logf != nil {
		remaining, _ := carrierAuthSnapshotRemainingTTL(raw)
		logf("WLT carrier auth event=snapshot_loaded source=previous remaining_ttl_seconds=%d", int64(remaining/time.Second))
	}
	return true, nil
}

func prepareCarrierAuthSnapshot(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, raw []byte, source string, logf func(string, ...any)) error {
	if cfg == nil {
		return errors.New("carrier config is required for auth snapshot")
	}
	if err := carrierengine.ImportAuthSnapshotJSON(raw); err != nil {
		return fmt.Errorf("import auth snapshot source=%s: %w", source, err)
	}
	options.startup.markSnapshotChecked()
	needsRefresh, err := carrierengine.AuthSnapshotNeedsRefresh(*cfg, raw, authSnapshotRefreshBeforeExpiry)
	if err != nil {
		return fmt.Errorf("inspect auth snapshot source=%s: %w", source, err)
	}
	if needsRefresh {
		if options.startup.providerRateLimited() {
			if logf != nil {
				logf("WLT carrier auth event=refresh_skipped source=%s reason=provider_rate_limited", source)
			}
			options.startup.markProviderRefreshReady("cached")
			return reportPreparedCarrierAuthSnapshot(raw, source, logf)
		}
		options.startup.markProviderRefreshStarted()
		if logf != nil {
			remaining, _ := carrierAuthSnapshotRemainingTTL(raw)
			logf("WLT carrier auth event=refresh_started source=%s remaining_ttl_seconds=%d", source, int64(remaining/time.Second))
		}
		refreshCtx, cancel := context.WithTimeout(ctx, defaultAuthSnapshotRefreshTimeout)
		refreshed, refreshErr := refreshCarrierAuthSnapshot(refreshCtx, *cfg, raw)
		cancel()
		if refreshErr != nil {
			if errors.Is(refreshErr, carriercommon.ErrHumanChallengeErrorLimit) {
				recordProviderRateLimit(options, logf)
			}
			// expires_at is a local refresh policy, not proof that the provider
			// revoked the saved signaling/TURN credentials. Restricted mobile
			// networks may also make refresh impossible before the tunnel exists.
			// Continue with the already imported snapshot; Authorize uses its
			// bounded offline reuse window and never enters anonymous/CAPTCHA for
			// this startup. Only a successful carrier connect may promote it.
			if logf != nil {
				logf("WLT carrier auth event=refresh_unavailable source=%s fallback=saved_snapshot error=%v", source, refreshErr)
				if errors.Is(refreshErr, carriercommon.ErrAuthSnapshotReauthorizationRequired) {
					logf("WLT carrier auth event=reauthorization_required source=%s", source)
				}
			}
			options.startup.markProviderRefreshReady("cached")
		} else {
			if err := carrierengine.ImportAuthSnapshotJSON(refreshed); err != nil {
				return fmt.Errorf("import refreshed auth snapshot source=%s: %w", source, err)
			}
			raw = refreshed
			if logf != nil {
				remaining, _ := carrierAuthSnapshotRemainingTTL(raw)
				logf("WLT carrier auth event=refresh_succeeded source=%s remaining_ttl_seconds=%d", source, int64(remaining/time.Second))
			}
			options.startup.markProviderRefreshReady("refreshed")
		}
	}
	return reportPreparedCarrierAuthSnapshot(raw, source, logf)
}

func reportPreparedCarrierAuthSnapshot(raw []byte, source string, logf func(string, ...any)) error {
	if logf != nil {
		remaining, _ := carrierAuthSnapshotRemainingTTL(raw)
		logf("WLT carrier auth event=snapshot_loaded source=%s remaining_ttl_seconds=%d persistence=deferred", source, int64(remaining/time.Second))
	}
	return nil
}

func fetchCarrierAuthSnapshot(ctx context.Context, rawURL string, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		timeout = defaultAuthSnapshotFetchTimeout
	}
	return fetchCarrierAuthSnapshotWithClient(ctx, rawURL, &http.Client{Timeout: timeout})
}

func fetchCarrierAuthSnapshotWithClient(ctx context.Context, rawURL string, client *http.Client) ([]byte, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("auth snapshot URL must be absolute HTTPS")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create auth snapshot request: %w", err)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch auth snapshot: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch auth snapshot: HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxAuthSnapshotBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read auth snapshot: %w", err)
	}
	if len(data) == 0 {
		return nil, errors.New("auth snapshot response is empty")
	}
	if len(data) > maxAuthSnapshotBytes {
		return nil, errors.New("auth snapshot response exceeds size limit")
	}
	return data, nil
}

func saveCarrierAuthSnapshot(options CarrierOptions, cfg *carrierconfig.ClientConfig, logf func(string, ...any)) error {
	data, err := carrierengine.ExportAuthSnapshotJSON(*cfg)
	if err != nil {
		return fmt.Errorf("export auth snapshot: %w", err)
	}
	if err := writeCarrierAuthSnapshot(options, data, true); err != nil {
		return err
	}
	if strings.TrimSpace(options.AuthSnapshotOutputFile) == "" && strings.TrimSpace(options.AuthSnapshotFile) == "" {
		return nil
	}
	if logf != nil {
		logf("WLT carrier auth snapshot saved")
	}
	return nil
}

func carrierAuthSnapshotPath(options CarrierOptions) string {
	path := strings.TrimSpace(options.AuthSnapshotOutputFile)
	if path == "" {
		path = strings.TrimSpace(options.AuthSnapshotFile)
	}
	return path
}

func providerCooldownPath(options CarrierOptions) string {
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return ""
	}
	return path + providerCooldownSuffix
}

func loadProviderCooldown(options CarrierOptions, logf func(string, ...any)) {
	path := providerCooldownPath(options)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) && logf != nil {
			logf("WLT carrier auth event=provider_cooldown_unavailable reason=read_error")
		}
		return
	}
	var state providerCooldownState
	if json.Unmarshal(data, &state) != nil || state.Version != providerCooldownVersion || state.BlockedUntil <= 0 || state.Attempts <= 0 {
		if logf != nil {
			logf("WLT carrier auth event=provider_cooldown_unavailable reason=invalid_state")
		}
		return
	}
	remaining := time.Until(time.Unix(state.BlockedUntil, 0))
	if remaining <= 0 {
		if logf != nil {
			logf("WLT carrier auth event=provider_cooldown_expired attempts=%d", state.Attempts)
		}
		return
	}
	options.startup.markProviderRateLimited()
	if logf != nil {
		logf("WLT carrier auth event=provider_cooldown_active remaining_seconds=%d attempts=%d", int64(remaining/time.Second), state.Attempts)
	}
}

func recordProviderRateLimit(options CarrierOptions, logf func(string, ...any)) {
	options.startup.markProviderRateLimited()
	path := providerCooldownPath(options)
	if path == "" {
		return
	}
	state := providerCooldownState{Version: providerCooldownVersion}
	if data, err := os.ReadFile(path); err == nil {
		var previous providerCooldownState
		if json.Unmarshal(data, &previous) == nil && previous.Version == providerCooldownVersion && previous.Attempts > 0 {
			state.Attempts = previous.Attempts
		}
	}
	state.Attempts++
	delay := providerCooldownInitial
	for attempt := 1; attempt < state.Attempts && delay < providerCooldownMaximum; attempt++ {
		delay *= 2
		if delay > providerCooldownMaximum {
			delay = providerCooldownMaximum
		}
	}
	state.BlockedUntil = time.Now().Add(delay).Unix()
	data, err := json.Marshal(state)
	if err != nil || writeCarrierAuthSnapshotFile(path, data) != nil {
		if logf != nil {
			logf("WLT carrier auth event=provider_cooldown_persist_failed")
		}
		return
	}
	if logf != nil {
		logf("WLT carrier auth event=provider_cooldown_started duration_seconds=%d attempts=%d", int64(delay/time.Second), state.Attempts)
	}
}

func clearProviderCooldown(options CarrierOptions, logf func(string, ...any)) {
	path := providerCooldownPath(options)
	if path == "" {
		return
	}
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		if logf != nil {
			logf("WLT carrier auth event=provider_cooldown_clear_failed")
		}
		return
	}
	if err == nil && logf != nil {
		logf("WLT carrier auth event=provider_cooldown_cleared")
	}
}

func writeCarrierAuthSnapshot(options CarrierOptions, data []byte, rotatePrevious bool) error {
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return nil
	}
	if rotatePrevious {
		current, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read current auth snapshot: %w", err)
		}
		current = bytes.TrimSpace(current)
		if len(current) > 0 && !bytes.Equal(current, bytes.TrimSpace(data)) && json.Valid(current) {
			if err := writeCarrierAuthSnapshotFile(path+authSnapshotPreviousSuffix, current); err != nil {
				return fmt.Errorf("rotate previous auth snapshot: %w", err)
			}
		}
	}
	return writeCarrierAuthSnapshotFile(path, data)
}

func writeCarrierAuthSnapshotFile(path string, data []byte) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create auth snapshot directory: %w", err)
		}
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".wlt-auth-snapshot-*")
	if err != nil {
		return fmt.Errorf("create auth snapshot file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect auth snapshot file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write auth snapshot file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync auth snapshot file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close auth snapshot file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("install auth snapshot file: %w", err)
	}
	return nil
}

func carrierAuthSnapshotRemainingTTL(data []byte) (time.Duration, bool) {
	var timing struct {
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(data, &timing); err != nil || timing.ExpiresAt.IsZero() {
		return 0, false
	}
	return time.Until(timing.ExpiresAt), true
}

func loadCarrierClientConfig(options CarrierConfigOptions) (*carrierconfig.ClientConfig, error) {
	raw := strings.TrimSpace(options.Config)
	if raw == "" && strings.TrimSpace(options.ConfigFile) != "" {
		content, err := os.ReadFile(strings.TrimSpace(options.ConfigFile))
		if err != nil {
			return nil, err
		}
		raw = strings.TrimSpace(string(content))
	}
	if raw == "" {
		return nil, errors.New("carrier config is not configured")
	}
	var (
		cfg *carrierconfig.ClientConfig
		err error
	)
	if strings.HasPrefix(raw, "turnable://") {
		cfg, err = carrierconfig.NewClientConfigFromURL(raw)
	} else {
		cfg, err = carrierconfig.NewClientConfigFromJSON(raw)
	}
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func carrierConfigSource(options CarrierOptions) string {
	if strings.TrimSpace(options.Config) != "" {
		return "inline"
	}
	if strings.TrimSpace(options.ConfigFile) != "" {
		return "file"
	}
	return "missing"
}

func carrierRouteIDs(cfg *carrierconfig.ClientConfig) []string {
	if cfg == nil {
		return nil
	}
	routeIDs := make([]string, 0, len(cfg.Routes))
	for _, route := range cfg.Routes {
		routeIDs = append(routeIDs, safeLogValue(route.RouteID))
	}
	if len(routeIDs) == 0 {
		return []string{"none"}
	}
	return routeIDs
}

func safeLogValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "none"
	}
	return value
}

func (c *Carrier) DialStream(ctx context.Context, routeClass string, target string) (io.ReadWriteCloser, error) {
	if c == nil {
		return nil, errors.New("nil WLT carrier")
	}
	routeID, err := c.routeIDForClass(routeClass)
	if err != nil {
		c.failedStreams.Add(1)
		return nil, err
	}
	activeWaitStartedAt := time.Now()
	releaseActive, err := c.acquireActiveSlot(ctx, routeClass, target)
	c.recordDuration(&c.lastActiveWait, &c.maxActiveWait, time.Since(activeWaitStartedAt))
	if err != nil {
		return nil, err
	}
	openWaitStartedAt := time.Now()
	releaseOpen, err := c.acquireOpenSlot(ctx, routeClass, target)
	c.recordDuration(&c.lastOpenWait, &c.maxOpenWait, time.Since(openWaitStartedAt))
	if err != nil {
		releaseActive()
		return nil, err
	}
	defer releaseOpen()

	dialCtx := ctx
	var cancel context.CancelFunc
	if _, ok := dialCtx.Deadline(); !ok && c.connectTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, c.connectTimeout)
	} else {
		dialCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	dialStartedAt := time.Now()
	var (
		conn                  net.Conn
		reconnectAttempts     int
		reconnectBackoff      = carrierReconnectRetryInitial
		reconnectBackoffTotal time.Duration
	)
reconnectLoop:
	for {
		c.openAttempts.Add(1)
		conn, err = c.dialRoute(dialCtx, routeID)
		if err == nil || !isCarrierReconnectInProgress(err) {
			break
		}
		waitStartedAt := time.Now()
		if c.waitReady != nil {
			if waitErr := c.waitReady(dialCtx); waitErr != nil {
				c.reconnectWait.Add(int64(time.Since(waitStartedAt)))
				err = waitErr
				break
			}
		}
		c.reconnectRetries.Add(1)
		c.reconnectWait.Add(int64(time.Since(waitStartedAt)))
		reconnectAttempts++

		timer := time.NewTimer(reconnectBackoff)
		select {
		case <-timer.C:
			reconnectBackoffTotal += reconnectBackoff
			c.reconnectWait.Add(int64(reconnectBackoff))
		case <-dialCtx.Done():
			timer.Stop()
			err = dialCtx.Err()
			break reconnectLoop
		}
		if reconnectBackoff < carrierReconnectRetryMax {
			reconnectBackoff *= 2
			if reconnectBackoff > carrierReconnectRetryMax {
				reconnectBackoff = carrierReconnectRetryMax
			}
		}
	}
	dialElapsed := time.Since(dialStartedAt)
	c.recordDuration(&c.lastDialDuration, &c.maxDialDuration, dialElapsed)
	if err != nil {
		releaseActive()
		c.failedStreams.Add(1)
		if c.logf != nil {
			c.logf("WLT stream open failed route=%s target=%s elapsed=%s error=%v", routeClass, target, dialElapsed, err)
		}
		return nil, fmt.Errorf("open WLT stream route=%s target=%s: %w", routeClass, target, err)
	}
	if conn == nil {
		releaseActive()
		c.failedStreams.Add(1)
		return nil, errors.New("WLT carrier returned nil stream")
	}
	if dialElapsed > 1500*time.Millisecond {
		if c.logf != nil {
			c.logf("WLT stream open slow route=%s target=%s elapsed=%s", routeClass, target, dialElapsed)
		}
	}
	if reconnectAttempts > 0 && c.logf != nil {
		c.logf("WLT stream open recovered after reconnect wait route=%s target=%s elapsed=%s retries=%d backoff=%s", routeClass, target, dialElapsed, reconnectAttempts, reconnectBackoffTotal)
	}
	if c.logf != nil {
		flowID := uint16(0)
		if flow, loaded := conn.(interface{ FlowID() uint16 }); loaded {
			flowID = flow.FlowID()
		}
		c.logf("WLT stream opened route=%s target=%s flow_id=%d elapsed=%s", routeClass, target, flowID, dialElapsed)
	}
	c.openedStreams.Add(1)
	stream := &carrierConn{
		Conn:          conn,
		carrier:       c,
		releaseActive: releaseActive,
		idleTimeout:   c.idleTimeout,
		routeClass:    normalizeCarrierRouteClass(routeClass),
	}
	c.registerStream(stream)
	return stream, nil
}

func (c *Carrier) Close() error {
	return c.close(false)
}

// MarkSingBoxReady records the point where the owning sing-box service has
// published this carrier to its outbounds. The startup log contains no profile,
// provider, address, or credential values.
func (c *Carrier) MarkSingBoxReady() {
	if c != nil {
		c.startup.markSingBoxReady()
	}
}

// Abort immediately closes the carrier without waiting for a graceful mux/KCP
// drain. A caller should use it only when the current underlay is already
// obsolete, such as after the default network interface changes.
func (c *Carrier) Abort() error {
	return c.close(true)
}

func (c *Carrier) close(immediate bool) error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.client != nil {
			var err error
			if immediate {
				err = c.client.Abort()
			} else {
				err = c.client.Stop()
			}
			if err != nil && !strings.Contains(err.Error(), "not running") {
				c.closeErr = err
			}
		}
		if c.restoreSocketControl != nil {
			c.restoreSocketControl()
		}
	})
	return c.closeErr
}

func (c *Carrier) Stats() CarrierStats {
	if c == nil {
		return CarrierStats{}
	}
	stats := CarrierStats{
		ActiveStreams:        c.activeStreams.Load(),
		PeakActiveStreams:    c.peakActiveStreams.Load(),
		PendingDials:         c.pendingDials.Load(),
		PeakPendingDials:     c.peakPendingDials.Load(),
		OpenAttempts:         c.openAttempts.Load(),
		DNSOpenRequests:      c.dnsOpenRequests.Load(),
		DNSOpenQueued:        c.dnsOpenQueued.Load(),
		DNSOpenRejected:      c.dnsOpenRejected.Load(),
		OpenedStreams:        c.openedStreams.Load(),
		ClosedStreams:        c.closedStreams.Load(),
		QueuedDials:          c.queuedDials.Load(),
		RejectedStreams:      c.rejectedStreams.Load(),
		RejectedQueue:        c.rejectedQueue.Load(),
		RejectedActive:       c.rejectedActive.Load(),
		RejectedOpen:         c.rejectedOpen.Load(),
		PressureIdleReclaims: c.pressureIdleReclaims.Load(),
		FailedStreams:        c.failedStreams.Load(),
		LastActiveWaitMillis: nanosToMillis(c.lastActiveWait.Load()),
		MaxActiveWaitMillis:  nanosToMillis(c.maxActiveWait.Load()),
		LastOpenWaitMillis:   nanosToMillis(c.lastOpenWait.Load()),
		MaxOpenWaitMillis:    nanosToMillis(c.maxOpenWait.Load()),
		LastDialMillis:       nanosToMillis(c.lastDialDuration.Load()),
		MaxDialMillis:        nanosToMillis(c.maxDialDuration.Load()),
		ReconnectRetries:     c.reconnectRetries.Load(),
		ReconnectWaitMillis:  nanosToMillis(c.reconnectWait.Load()),
	}
	if c.client != nil {
		stats.Runtime = c.client.Stats()
	}
	return stats
}

func (c *Carrier) acquireActiveSlot(ctx context.Context, routeClass string, target string) (func(), error) {
	acquired := tryAcquire(c.activeSlots)
	if !acquired {
		// Under saturation, reclaim only connections that have been idle for a
		// separately configured pressure interval. The normal idle timeout is
		// deliberately left untouched so long-lived media is not shortened.
		if c.reclaimPressureIdleStream(routeClass) {
			acquired = tryAcquire(c.activeSlots)
		}
	}
	if !acquired {
		if !tryAcquire(c.pendingSlots) {
			c.rejectedQueue.Add(1)
			c.rejectedStreams.Add(1)
			return nil, fmt.Errorf("WLT carrier pending dial limit reached route=%s target=%s", routeClass, target)
		}
		c.queuedDials.Add(1)
		c.pendingDials.Add(1)
		c.updatePeakPendingDials()
		defer func() {
			c.pendingDials.Add(-1)
			release(c.pendingSlots)
		}()

		waitCtx := ctx
		var cancel context.CancelFunc
		if c.dialQueueTimeout > 0 {
			waitCtx, cancel = context.WithTimeout(ctx, c.dialQueueTimeout)
		} else {
			waitCtx, cancel = context.WithCancel(ctx)
		}
		defer cancel()

		select {
		case c.activeSlots <- struct{}{}:
			acquired = true
		case <-waitCtx.Done():
			c.rejectedActive.Add(1)
			c.rejectedStreams.Add(1)
			return nil, fmt.Errorf("WLT carrier active stream limit reached route=%s target=%s: %w", routeClass, target, waitCtx.Err())
		}
	}

	c.activeStreams.Add(1)
	c.updatePeakActiveStreams()
	activeReleased := false
	return func() {
		if activeReleased {
			return
		}
		activeReleased = true
		release(c.activeSlots)
		c.activeStreams.Add(-1)
	}, nil
}

func (c *Carrier) acquireOpenSlot(ctx context.Context, routeClass string, target string) (func(), error) {
	priority := isDNSOpenTarget(target)
	if priority {
		c.dnsOpenRequests.Add(1)
	}
	if c.tryAcquireOpenSlot(priority) {
		return func() {
			c.releaseOpenSlot(priority)
		}, nil
	}
	if !tryAcquire(c.pendingSlots) {
		c.rejectedQueue.Add(1)
		c.rejectedStreams.Add(1)
		return nil, fmt.Errorf("WLT carrier pending dial limit reached route=%s target=%s", routeClass, target)
	}
	c.queuedDials.Add(1)
	if priority {
		c.dnsOpenQueued.Add(1)
	}
	c.pendingDials.Add(1)
	c.updatePeakPendingDials()
	defer func() {
		c.pendingDials.Add(-1)
		release(c.pendingSlots)
	}()

	waitCtx := ctx
	var cancel context.CancelFunc
	if c.dialQueueTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, c.dialQueueTimeout)
	} else {
		waitCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	if err := c.acquireQueuedOpenSlot(waitCtx, priority); err == nil {
		return func() {
			c.releaseOpenSlot(priority)
		}, nil
	}
	if priority {
		c.dnsOpenRejected.Add(1)
	}
	c.rejectedOpen.Add(1)
	c.rejectedStreams.Add(1)
	return nil, fmt.Errorf("WLT carrier open attempt limit reached route=%s target=%s: %w", routeClass, target, waitCtx.Err())
}

func (c *Carrier) tryAcquireOpenSlot(priority bool) bool {
	if c.openGate != nil {
		return c.openGate.tryAcquire(priority)
	}
	return tryAcquire(c.openSlots)
}

func (c *Carrier) acquireQueuedOpenSlot(ctx context.Context, priority bool) error {
	if c.openGate != nil {
		return c.openGate.acquire(ctx, priority)
	}
	select {
	case c.openSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Carrier) releaseOpenSlot(priority bool) {
	if c.openGate != nil {
		c.openGate.release(priority)
		return
	}
	release(c.openSlots)
}

func isDNSOpenTarget(target string) bool {
	_, port, err := net.SplitHostPort(strings.TrimSpace(target))
	if err != nil {
		return false
	}
	return port == "53" || port == "853"
}

func (c *Carrier) updatePeakActiveStreams() {
	active := c.activeStreams.Load()
	for {
		peak := c.peakActiveStreams.Load()
		if active <= peak || c.peakActiveStreams.CompareAndSwap(peak, active) {
			return
		}
	}
}

func (c *Carrier) updatePeakPendingDials() {
	pending := c.pendingDials.Load()
	for {
		peak := c.peakPendingDials.Load()
		if pending <= peak || c.peakPendingDials.CompareAndSwap(peak, pending) {
			return
		}
	}
}

func (c *Carrier) recordDuration(last *atomic.Int64, max *atomic.Int64, value time.Duration) {
	if value < 0 {
		value = 0
	}
	nanos := int64(value)
	last.Store(nanos)
	updateMaxAtomicInt64(max, nanos)
}

func updateMaxAtomicInt64(slot *atomic.Int64, value int64) {
	for {
		previous := slot.Load()
		if value <= previous || slot.CompareAndSwap(previous, value) {
			return
		}
	}
}

func isCarrierReconnectInProgress(err error) bool {
	return err != nil && strings.Contains(err.Error(), "full reconnect is in progress")
}

func nanosToMillis(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return value / int64(time.Millisecond)
}

func (c *Carrier) routeIDForClass(routeClass string) (string, error) {
	routeClass = normalizeCarrierRouteClass(routeClass)
	if routeClass == "" {
		routeClass = "direct"
	}
	if routeID, ok := c.routeByClass[routeClass]; ok {
		return routeID, nil
	}
	return "", fmt.Errorf("unknown WLT carrier route class: %s", routeClass)
}

func withCarrierDefaults(options CarrierOptions) CarrierOptions {
	if options.ConnectTimeout <= 0 {
		options.ConnectTimeout = defaultCarrierConnectTimeout
	}
	if options.MaxActiveStreams <= 0 {
		options.MaxActiveStreams = defaultCarrierMaxActive
	}
	if options.MaxOpenAttempts <= 0 {
		options.MaxOpenAttempts = defaultCarrierMaxOpen
	}
	if options.MaxPendingDials <= 0 {
		options.MaxPendingDials = defaultCarrierMaxPending
	}
	if options.DialQueueTimeout <= 0 {
		options.DialQueueTimeout = defaultCarrierQueueTimeout
	}
	if options.IdleTimeout <= 0 {
		options.IdleTimeout = defaultCarrierIdleTimeout
	}
	if options.BufferSize <= 0 {
		options.BufferSize = defaultCarrierBufferSize
	}
	return options
}

func applyCarrierRuntimeOptions(options CarrierOptions) carrierconfig.TransportOptions {
	bufferSize := options.BufferSize
	if bufferSize <= 0 {
		bufferSize = defaultCarrierBufferSize
	}
	transportOptions := carrierconfig.TransportOptions{
		TinyMuxFlowBuffer:            clampInt(bufferSize/256, 64, 256),
		TinyMuxFlowSendBuffer:        clampInt(bufferSize/1024, 16, 64),
		TinyMuxControlBuffer:         clampInt(bufferSize/512, 32, 128),
		TinyMuxRateBurstBytes:        options.TinyMuxRateBurstBytes,
		TinyMuxPingTimeoutMillis:     int(defaultCarrierTinyMuxPingTimeout / time.Millisecond),
		PeerIncomingBuffer:           clampInt(bufferSize/256, 64, 256),
		PeerWriteBuffer:              clampInt(bufferSize/1024, 16, 64),
		AdaptivePeerData:             options.AdaptivePeerData,
		AdaptivePeerThresholdBytes:   options.AdaptivePeerThresholdBytes,
		AdaptivePeerIdleMillis:       int(options.AdaptivePeerIdleTimeout / time.Millisecond),
		SRTPPacketBuffer:             clampInt(bufferSize/128, 128, 512),
		KCPWindowSize:                clampInt(bufferSize/64, 256, 768),
		KCPReadWriteBuffer:           scaleClampedInt(bufferSize, 16, 256*1024, 1024*1024),
		RelayBandwidthBytesPerSecond: carrierconfig.Options.Transport.RelayBandwidthBytesPerSecond,
	}
	if options.RelayBandwidthBytesPerSecond > 0 {
		transportOptions.RelayBandwidthBytesPerSecond = options.RelayBandwidthBytesPerSecond
	}
	if options.TinyMuxFlowBuffer > 0 {
		transportOptions.TinyMuxFlowBuffer = options.TinyMuxFlowBuffer
	}
	if options.TinyMuxFlowSendBuffer > 0 {
		transportOptions.TinyMuxFlowSendBuffer = options.TinyMuxFlowSendBuffer
	}
	if options.TinyMuxControlBuffer > 0 {
		transportOptions.TinyMuxControlBuffer = options.TinyMuxControlBuffer
	}
	if options.TinyMuxPingTimeout > 0 {
		transportOptions.TinyMuxPingTimeoutMillis = int(options.TinyMuxPingTimeout / time.Millisecond)
	}
	if options.PeerIncomingBuffer > 0 {
		transportOptions.PeerIncomingBuffer = options.PeerIncomingBuffer
	}
	if options.PeerWriteBuffer > 0 {
		transportOptions.PeerWriteBuffer = options.PeerWriteBuffer
	}
	if options.SRTPPacketBuffer > 0 {
		transportOptions.SRTPPacketBuffer = options.SRTPPacketBuffer
	}
	if options.KCPWindowSize > 0 {
		transportOptions.KCPWindowSize = options.KCPWindowSize
	}
	if options.KCPReadWriteBuffer > 0 {
		transportOptions.KCPReadWriteBuffer = options.KCPReadWriteBuffer
	}
	carrierconfig.Options.Interactive = false
	carrierconfig.Options.Transport = transportOptions
	return transportOptions
}

func clampInt(value int, minimum int, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func scaleClampedInt(value int, scale int, minimum int, maximum int) int {
	if value <= 0 || scale <= 0 {
		return minimum
	}
	if value > maximum/scale {
		return maximum
	}
	return clampInt(value*scale, minimum, maximum)
}

func carrierRouteMap(cfg *carrierconfig.ClientConfig) map[string]string {
	routeByClass := make(map[string]string)
	if cfg == nil {
		return routeByClass
	}
	for _, route := range cfg.Routes {
		routeID := strings.TrimSpace(route.RouteID)
		if routeID == "" {
			continue
		}
		setCarrierRoute(routeByClass, routeID, routeID)
		if key := carrierListenerKey(routeID); key != "" {
			setCarrierRoute(routeByClass, key, routeID)
		}
	}
	if len(cfg.Routes) == 1 {
		routeID := strings.TrimSpace(cfg.Routes[0].RouteID)
		setCarrierRoute(routeByClass, "default", routeID)
		setCarrierRoute(routeByClass, "direct", routeID)
	}
	return routeByClass
}

func setCarrierRoute(routeByClass map[string]string, routeClass string, routeID string) {
	if routeID == "" {
		return
	}
	routeClass = normalizeCarrierRouteClass(routeClass)
	if routeClass == "" {
		return
	}
	if _, exists := routeByClass[routeClass]; !exists {
		routeByClass[routeClass] = routeID
	}
}

func normalizeCarrierRouteClass(routeClass string) string {
	return strings.ToLower(strings.TrimSpace(routeClass))
}

func carrierRouteClasses(routeByClass map[string]string) []string {
	classes := make([]string, 0, len(routeByClass))
	for routeClass := range routeByClass {
		classes = append(classes, routeClass)
	}
	sort.Strings(classes)
	return classes
}

func tryAcquire(ch chan struct{}) bool {
	select {
	case ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func release(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
}

type carrierConn struct {
	net.Conn
	carrier       *Carrier
	releaseActive func()
	idleTimeout   time.Duration
	routeClass    string
	lastActivity  atomic.Int64
	ioMu          sync.RWMutex
	closeOnce     sync.Once
}

// FlowID exposes the diagnostics-safe TinyMux flow identifier while retaining
// the carrierConn lifecycle wrapper. It is zero for test transports that do
// not provide a TinyMux flow.
func (c *carrierConn) FlowID() uint16 {
	if c == nil || c.Conn == nil {
		return 0
	}
	if flow, loaded := c.Conn.(interface{ FlowID() uint16 }); loaded {
		return flow.FlowID()
	}
	return 0
}

func (c *carrierConn) Read(p []byte) (int, error) {
	c.ioMu.RLock()
	defer c.ioMu.RUnlock()
	c.markActivity()
	c.refreshDeadline()
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.markActivity()
		c.refreshDeadline()
		if c.carrier != nil {
			c.carrier.startup.markFirstPacket("read")
			c.carrier.startup.markTrafficReady()
		}
	}
	return n, err
}

func (c *carrierConn) Write(p []byte) (int, error) {
	c.ioMu.RLock()
	defer c.ioMu.RUnlock()
	c.markActivity()
	c.refreshDeadline()
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.markActivity()
		c.refreshDeadline()
		if c.carrier != nil {
			c.carrier.startup.markFirstPacket("write")
		}
	}
	return n, err
}

func (c *carrierConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() {
		c.carrier.unregisterStream(c)
		c.releaseActive()
		c.carrier.closedStreams.Add(1)
	})
	return err
}

func (c *carrierConn) refreshDeadline() {
	if c.idleTimeout <= 0 {
		return
	}
	_ = c.Conn.SetDeadline(time.Now().Add(c.idleTimeout))
}

func (c *carrierConn) markActivity() {
	c.lastActivity.Store(time.Now().UnixNano())
}

func (c *Carrier) registerStream(stream *carrierConn) {
	if stream == nil {
		return
	}
	stream.markActivity()
	c.streamsMu.Lock()
	if c.streams == nil {
		c.streams = make(map[*carrierConn]struct{})
	}
	c.streams[stream] = struct{}{}
	c.streamsMu.Unlock()
}

func (c *Carrier) unregisterStream(stream *carrierConn) {
	if c == nil || stream == nil {
		return
	}
	c.streamsMu.Lock()
	delete(c.streams, stream)
	c.streamsMu.Unlock()
}

func (c *Carrier) reclaimPressureIdleStream(routeClass string) bool {
	if c == nil || c.pressureIdleTimeout <= 0 {
		return false
	}
	cutoff := time.Now().Add(-c.pressureIdleTimeout).UnixNano()
	routeClass = normalizeCarrierRouteClass(routeClass)
	var candidate *carrierConn
	var sameRouteCandidate *carrierConn
	c.streamsMu.Lock()
	for stream := range c.streams {
		lastActivity := stream.lastActivity.Load()
		if lastActivity == 0 || lastActivity > cutoff {
			continue
		}
		if stream.routeClass != routeClass {
			if candidate == nil || lastActivity < candidate.lastActivity.Load() {
				if !stream.ioMu.TryLock() {
					continue
				}
				if candidate != nil {
					candidate.ioMu.Unlock()
				}
				candidate = stream
			}
		} else if sameRouteCandidate == nil || lastActivity < sameRouteCandidate.lastActivity.Load() {
			if !stream.ioMu.TryLock() {
				continue
			}
			if sameRouteCandidate != nil {
				sameRouteCandidate.ioMu.Unlock()
			}
			sameRouteCandidate = stream
		}
	}
	if candidate == nil {
		candidate = sameRouteCandidate
		sameRouteCandidate = nil
	}
	if sameRouteCandidate != nil {
		sameRouteCandidate.ioMu.Unlock()
	}
	if candidate != nil {
		// Remove it before closing so concurrent admission attempts cannot pick
		// the same stream while Close is releasing its active slot.
		delete(c.streams, candidate)
	}
	c.streamsMu.Unlock()
	if candidate == nil {
		return false
	}
	c.pressureIdleReclaims.Add(1)
	if c.logf != nil {
		c.logf("WLT carrier pressure idle reclaim stream route=%s requested_route=%s idle_ms=%d", candidate.routeClass, routeClass, time.Since(time.Unix(0, candidate.lastActivity.Load())).Milliseconds())
	}
	_ = candidate.Close()
	candidate.ioMu.Unlock()
	return true
}
