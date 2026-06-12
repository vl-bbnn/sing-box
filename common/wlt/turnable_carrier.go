package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	turnableconfig "github.com/theairblow/turnable/pkg/config"
	turnableengine "github.com/theairblow/turnable/pkg/engine"
)

type logfSlogHandler struct {
	logf  func(string, ...any)
	attrs []slog.Attr
	group string
}

func newLogfSlogLogger(logf func(string, ...any)) *slog.Logger {
	if logf == nil {
		logf = log.Printf
	}
	return slog.New(&logfSlogHandler{logf: logf})
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
	var b strings.Builder
	b.WriteString("turnable ")
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
	defaultTurnableCarrierConnectTimeout = 10 * time.Second
	defaultTurnableCarrierMaxActive      = 32
	defaultTurnableCarrierMaxOpen        = 16
	defaultTurnableCarrierMaxPending     = 24
	defaultTurnableCarrierQueueTimeout   = 1500 * time.Millisecond
	defaultTurnableCarrierIdleTimeout    = 20 * time.Second
	defaultTurnableCarrierBufferSize     = 32 * 1024

	defaultTurnableTinyMuxFlowBuffer     = 128
	defaultTurnableTinyMuxFlowSendBuffer = 32
	defaultTurnableTinyMuxControlBuffer  = 64
	defaultTurnableTinyMuxPingTimeout    = 20 * time.Second
	defaultTurnablePeerIncomingBuffer    = 128
	defaultTurnablePeerWriteBuffer       = 32
	defaultTurnableSRTPPacketBuffer      = 256
	defaultTurnableKCPWindowSize         = 512
	defaultTurnableKCPReadWriteBuffer    = 512 * 1024

	turnableConnectRetryInitial = 250 * time.Millisecond
	turnableConnectRetryMax     = 2 * time.Second

	turnableReconnectRetryInitial = 20 * time.Millisecond
	turnableReconnectRetryMax     = 250 * time.Millisecond
)

type TurnableCarrierOptions struct {
	Config     string
	ConfigFile string

	ConnectTimeout   time.Duration
	MaxActiveStreams int
	MaxOpenAttempts  int
	MaxPendingDials  int
	DialQueueTimeout time.Duration
	IdleTimeout      time.Duration
	BufferSize       int

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

	Logger func(string, ...any)
}

type TurnableOptions struct {
	Config     string
	ConfigFile string
}

type CarrierStats struct {
	ActiveStreams        int64
	PeakActiveStreams    int64
	PendingDials         int64
	PeakPendingDials     int64
	OpenAttempts         int64
	OpenedStreams        int64
	ClosedStreams        int64
	QueuedDials          int64
	RejectedStreams      int64
	RejectedQueue        int64
	RejectedActive       int64
	RejectedOpen         int64
	FailedStreams        int64
	LastActiveWaitMillis int64
	MaxActiveWaitMillis  int64
	LastOpenWaitMillis   int64
	MaxOpenWaitMillis    int64
	LastDialMillis       int64
	MaxDialMillis        int64
	ReconnectRetries     int64
	ReconnectWaitMillis  int64
	Runtime              turnableconfig.RuntimeStats
}

type TurnableCarrier struct {
	client *turnableengine.TurnableClient
	cancel context.CancelFunc

	dialRoute func(context.Context, string) (net.Conn, error)
	waitReady func(context.Context) error
	logf      func(string, ...any)

	connectTimeout   time.Duration
	dialQueueTimeout time.Duration
	idleTimeout      time.Duration
	bufferSize       int
	activeSlots      chan struct{}
	openSlots        chan struct{}
	pendingSlots     chan struct{}
	routeByClass     map[string]string

	activeStreams     atomic.Int64
	peakActiveStreams atomic.Int64
	pendingDials      atomic.Int64
	peakPendingDials  atomic.Int64
	openAttempts      atomic.Int64
	openedStreams     atomic.Int64
	closedStreams     atomic.Int64
	queuedDials       atomic.Int64
	rejectedStreams   atomic.Int64
	rejectedQueue     atomic.Int64
	rejectedActive    atomic.Int64
	rejectedOpen      atomic.Int64
	failedStreams     atomic.Int64
	lastActiveWait    atomic.Int64
	maxActiveWait     atomic.Int64
	lastOpenWait      atomic.Int64
	maxOpenWait       atomic.Int64
	lastDialDuration  atomic.Int64
	maxDialDuration   atomic.Int64
	reconnectRetries  atomic.Int64
	reconnectWait     atomic.Int64
	closeOnce         sync.Once
	closeErr          error
}

func StartTurnableCarrier(ctx context.Context, options TurnableCarrierOptions) (*TurnableCarrier, error) {
	logf := options.Logger
	if logf == nil {
		logf = log.Printf
	}
	cfg, err := loadTurnableClientConfig(TurnableOptions{
		Config:     options.Config,
		ConfigFile: options.ConfigFile,
	})
	if err != nil {
		return nil, err
	}
	options = withTurnableCarrierDefaults(options)
	runCtx, cancel := context.WithCancel(ctx)
	transportOptions := applyTurnableCarrierRuntimeOptions(options)

	turnableClient, err := connectTurnableClient(runCtx, cfg, options.ConnectTimeout, logf)
	if err != nil {
		cancel()
		return nil, err
	}

	carrier := &TurnableCarrier{
		client:           turnableClient,
		cancel:           cancel,
		dialRoute:        turnableClient.DialRouteContext,
		waitReady:        turnableClient.WaitReady,
		logf:             logf,
		connectTimeout:   options.ConnectTimeout,
		dialQueueTimeout: options.DialQueueTimeout,
		idleTimeout:      options.IdleTimeout,
		bufferSize:       options.BufferSize,
		activeSlots:      make(chan struct{}, options.MaxActiveStreams),
		openSlots:        make(chan struct{}, options.MaxOpenAttempts),
		pendingSlots:     make(chan struct{}, options.MaxPendingDials),
		routeByClass:     turnableCarrierRouteMap(cfg),
	}
	go func() {
		<-runCtx.Done()
		_ = carrier.Close()
	}()
	logf("Turnable carrier started routes=%d classes=%s max_active=%d max_open=%d max_pending=%d queue_timeout=%s idle_timeout=%s buffer_size=%d kcp_window=%d kcp_buffer=%d mux_flow_buffer=%d mux_send_buffer=%d mux_control_buffer=%d mux_burst=%d mux_ping_timeout_ms=%d peer_incoming_buffer=%d peer_write_buffer=%d adaptive_peer_data=%t adaptive_peer_threshold=%d adaptive_peer_idle_ms=%d srtp_packet_buffer=%d relay_bandwidth=%d",
		len(cfg.Routes),
		strings.Join(turnableCarrierRouteClasses(carrier.routeByClass), ","),
		options.MaxActiveStreams,
		options.MaxOpenAttempts,
		options.MaxPendingDials,
		options.DialQueueTimeout,
		options.IdleTimeout,
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
	return carrier, nil
}

func connectTurnableClient(ctx context.Context, cfg *turnableconfig.ClientConfig, timeout time.Duration, logf func(string, ...any)) (*turnableengine.TurnableClient, error) {
	connectCtx, connectCancel := context.WithTimeout(ctx, timeout)
	defer connectCancel()

	delay := turnableConnectRetryInitial
	var lastErr error
	for attempt := 1; ; attempt++ {
		turnableClient := turnableengine.NewTurnableClient(*cfg)
		turnableClient.SetLogger(newLogfSlogLogger(logf))

		connectDone := make(chan error, 1)
		go func() {
			connectDone <- turnableClient.Connect()
		}()

		select {
		case err := <-connectDone:
			if err == nil {
				if attempt > 1 && logf != nil {
					logf("Turnable carrier connect recovered attempts=%d", attempt)
				}
				return turnableClient, nil
			}
			lastErr = err
			_ = turnableClient.Stop()
		case <-connectCtx.Done():
			_ = turnableClient.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("connect Turnable carrier: %w; last error: %v", connectCtx.Err(), lastErr)
			}
			return nil, fmt.Errorf("connect Turnable carrier: %w", connectCtx.Err())
		}

		select {
		case <-connectCtx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("connect Turnable carrier: %w; last error: %v", connectCtx.Err(), lastErr)
			}
			return nil, fmt.Errorf("connect Turnable carrier: %w", connectCtx.Err())
		default:
		}

		if logf != nil {
			logf("Turnable carrier connect failed attempt=%d retry_in=%s error=%v", attempt, delay, lastErr)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-connectCtx.Done():
			timer.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("connect Turnable carrier: %w; last error: %v", connectCtx.Err(), lastErr)
			}
			return nil, fmt.Errorf("connect Turnable carrier: %w", connectCtx.Err())
		}
		if delay < turnableConnectRetryMax {
			delay *= 2
			if delay > turnableConnectRetryMax {
				delay = turnableConnectRetryMax
			}
		}
	}
}

func loadTurnableClientConfig(options TurnableOptions) (*turnableconfig.ClientConfig, error) {
	raw := strings.TrimSpace(options.Config)
	if raw == "" && strings.TrimSpace(options.ConfigFile) != "" {
		content, err := os.ReadFile(strings.TrimSpace(options.ConfigFile))
		if err != nil {
			return nil, err
		}
		raw = strings.TrimSpace(string(content))
	}
	if raw == "" {
		return nil, errors.New("turnable config is not configured")
	}
	var (
		cfg *turnableconfig.ClientConfig
		err error
	)
	if strings.HasPrefix(raw, "turnable://") {
		cfg, err = turnableconfig.NewClientConfigFromURL(raw)
	} else {
		cfg, err = turnableconfig.NewClientConfigFromJSON(raw)
	}
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *TurnableCarrier) DialStream(ctx context.Context, routeClass string, target string) (io.ReadWriteCloser, error) {
	if c == nil {
		return nil, errors.New("nil Turnable carrier")
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
		reconnectBackoff      = turnableReconnectRetryInitial
		reconnectBackoffTotal time.Duration
	)
reconnectLoop:
	for {
		c.openAttempts.Add(1)
		conn, err = c.dialRoute(dialCtx, routeID)
		if err == nil || !isTurnableReconnectInProgress(err) {
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
		if reconnectBackoff < turnableReconnectRetryMax {
			reconnectBackoff *= 2
			if reconnectBackoff > turnableReconnectRetryMax {
				reconnectBackoff = turnableReconnectRetryMax
			}
		}
	}
	dialElapsed := time.Since(dialStartedAt)
	c.recordDuration(&c.lastDialDuration, &c.maxDialDuration, dialElapsed)
	if err != nil {
		releaseActive()
		c.failedStreams.Add(1)
		if c.logf != nil {
			c.logf("Turnable stream open failed route=%s target=%s elapsed=%s error=%v", routeClass, target, dialElapsed, err)
		}
		return nil, fmt.Errorf("open Turnable stream route=%s target=%s: %w", routeClass, target, err)
	}
	if conn == nil {
		releaseActive()
		c.failedStreams.Add(1)
		return nil, errors.New("Turnable carrier returned nil stream")
	}
	if dialElapsed > 1500*time.Millisecond {
		if c.logf != nil {
			c.logf("Turnable stream open slow route=%s target=%s elapsed=%s", routeClass, target, dialElapsed)
		}
	}
	if reconnectAttempts > 0 && c.logf != nil {
		c.logf("Turnable stream open recovered after reconnect wait route=%s target=%s elapsed=%s retries=%d backoff=%s", routeClass, target, dialElapsed, reconnectAttempts, reconnectBackoffTotal)
	}
	c.openedStreams.Add(1)
	return &turnableCarrierConn{
		Conn:          conn,
		carrier:       c,
		releaseActive: releaseActive,
		idleTimeout:   c.idleTimeout,
	}, nil
}

func (c *TurnableCarrier) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.client != nil {
			if err := c.client.Stop(); err != nil && !strings.Contains(err.Error(), "not running") {
				c.closeErr = err
			}
		}
	})
	return c.closeErr
}

func (c *TurnableCarrier) Stats() CarrierStats {
	if c == nil {
		return CarrierStats{}
	}
	stats := CarrierStats{
		ActiveStreams:        c.activeStreams.Load(),
		PeakActiveStreams:    c.peakActiveStreams.Load(),
		PendingDials:         c.pendingDials.Load(),
		PeakPendingDials:     c.peakPendingDials.Load(),
		OpenAttempts:         c.openAttempts.Load(),
		OpenedStreams:        c.openedStreams.Load(),
		ClosedStreams:        c.closedStreams.Load(),
		QueuedDials:          c.queuedDials.Load(),
		RejectedStreams:      c.rejectedStreams.Load(),
		RejectedQueue:        c.rejectedQueue.Load(),
		RejectedActive:       c.rejectedActive.Load(),
		RejectedOpen:         c.rejectedOpen.Load(),
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

func (c *TurnableCarrier) acquireActiveSlot(ctx context.Context, routeClass string, target string) (func(), error) {
	acquired := tryAcquire(c.activeSlots)
	if !acquired {
		if !tryAcquire(c.pendingSlots) {
			c.rejectedQueue.Add(1)
			c.rejectedStreams.Add(1)
			return nil, fmt.Errorf("Turnable carrier pending dial limit reached route=%s target=%s", routeClass, target)
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
			return nil, fmt.Errorf("Turnable carrier active stream limit reached route=%s target=%s: %w", routeClass, target, waitCtx.Err())
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

func (c *TurnableCarrier) acquireOpenSlot(ctx context.Context, routeClass string, target string) (func(), error) {
	if tryAcquire(c.openSlots) {
		return func() {
			release(c.openSlots)
		}, nil
	}
	if !tryAcquire(c.pendingSlots) {
		c.rejectedQueue.Add(1)
		c.rejectedStreams.Add(1)
		return nil, fmt.Errorf("Turnable carrier pending dial limit reached route=%s target=%s", routeClass, target)
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
	case c.openSlots <- struct{}{}:
		return func() {
			release(c.openSlots)
		}, nil
	case <-waitCtx.Done():
		c.rejectedOpen.Add(1)
		c.rejectedStreams.Add(1)
		return nil, fmt.Errorf("Turnable carrier open attempt limit reached route=%s target=%s: %w", routeClass, target, waitCtx.Err())
	}
}

func (c *TurnableCarrier) updatePeakActiveStreams() {
	active := c.activeStreams.Load()
	for {
		peak := c.peakActiveStreams.Load()
		if active <= peak || c.peakActiveStreams.CompareAndSwap(peak, active) {
			return
		}
	}
}

func (c *TurnableCarrier) updatePeakPendingDials() {
	pending := c.pendingDials.Load()
	for {
		peak := c.peakPendingDials.Load()
		if pending <= peak || c.peakPendingDials.CompareAndSwap(peak, pending) {
			return
		}
	}
}

func (c *TurnableCarrier) recordDuration(last *atomic.Int64, max *atomic.Int64, value time.Duration) {
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

func isTurnableReconnectInProgress(err error) bool {
	return err != nil && strings.Contains(err.Error(), "full reconnect is in progress")
}

func nanosToMillis(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return value / int64(time.Millisecond)
}

func (c *TurnableCarrier) routeIDForClass(routeClass string) (string, error) {
	routeClass = normalizeTurnableRouteClass(routeClass)
	if routeClass == "" {
		routeClass = "direct"
	}
	if routeID, ok := c.routeByClass[routeClass]; ok {
		return routeID, nil
	}
	return "", fmt.Errorf("unknown Turnable route class: %s", routeClass)
}

func withTurnableCarrierDefaults(options TurnableCarrierOptions) TurnableCarrierOptions {
	if options.ConnectTimeout <= 0 {
		options.ConnectTimeout = defaultTurnableCarrierConnectTimeout
	}
	if options.MaxActiveStreams <= 0 {
		options.MaxActiveStreams = defaultTurnableCarrierMaxActive
	}
	if options.MaxOpenAttempts <= 0 {
		options.MaxOpenAttempts = defaultTurnableCarrierMaxOpen
	}
	if options.MaxPendingDials <= 0 {
		options.MaxPendingDials = defaultTurnableCarrierMaxPending
	}
	if options.DialQueueTimeout <= 0 {
		options.DialQueueTimeout = defaultTurnableCarrierQueueTimeout
	}
	if options.IdleTimeout <= 0 {
		options.IdleTimeout = defaultTurnableCarrierIdleTimeout
	}
	if options.BufferSize <= 0 {
		options.BufferSize = defaultTurnableCarrierBufferSize
	}
	return options
}

func applyTurnableCarrierRuntimeOptions(options TurnableCarrierOptions) turnableconfig.TransportOptions {
	bufferSize := options.BufferSize
	if bufferSize <= 0 {
		bufferSize = defaultTurnableCarrierBufferSize
	}
	transportOptions := turnableconfig.TransportOptions{
		TinyMuxFlowBuffer:            clampInt(bufferSize/256, 64, 256),
		TinyMuxFlowSendBuffer:        clampInt(bufferSize/1024, 16, 64),
		TinyMuxControlBuffer:         clampInt(bufferSize/512, 32, 128),
		TinyMuxRateBurstBytes:        options.TinyMuxRateBurstBytes,
		TinyMuxPingTimeoutMillis:     int(defaultTurnableTinyMuxPingTimeout / time.Millisecond),
		PeerIncomingBuffer:           clampInt(bufferSize/256, 64, 256),
		PeerWriteBuffer:              clampInt(bufferSize/1024, 16, 64),
		AdaptivePeerData:             options.AdaptivePeerData,
		AdaptivePeerThresholdBytes:   options.AdaptivePeerThresholdBytes,
		AdaptivePeerIdleMillis:       int(options.AdaptivePeerIdleTimeout / time.Millisecond),
		SRTPPacketBuffer:             clampInt(bufferSize/128, 128, 512),
		KCPWindowSize:                clampInt(bufferSize/64, 256, 768),
		KCPReadWriteBuffer:           scaleClampedInt(bufferSize, 16, 256*1024, 1024*1024),
		RelayBandwidthBytesPerSecond: turnableconfig.Options.Transport.RelayBandwidthBytesPerSecond,
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
	turnableconfig.Options.Interactive = false
	turnableconfig.Options.Transport = transportOptions
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

func turnableCarrierRouteMap(cfg *turnableconfig.ClientConfig) map[string]string {
	routeByClass := make(map[string]string)
	if cfg == nil {
		return routeByClass
	}
	for _, route := range cfg.Routes {
		routeID := strings.TrimSpace(route.RouteID)
		if routeID == "" {
			continue
		}
		setTurnableCarrierRoute(routeByClass, routeID, routeID)
		if key := turnableListenerKey(routeID); key != "" {
			setTurnableCarrierRoute(routeByClass, key, routeID)
		}
	}
	if len(cfg.Routes) == 1 {
		routeID := strings.TrimSpace(cfg.Routes[0].RouteID)
		setTurnableCarrierRoute(routeByClass, "default", routeID)
		setTurnableCarrierRoute(routeByClass, "direct", routeID)
	}
	return routeByClass
}

func setTurnableCarrierRoute(routeByClass map[string]string, routeClass string, routeID string) {
	if routeID == "" {
		return
	}
	routeClass = normalizeTurnableRouteClass(routeClass)
	if routeClass == "" {
		return
	}
	if _, exists := routeByClass[routeClass]; !exists {
		routeByClass[routeClass] = routeID
	}
}

func normalizeTurnableRouteClass(routeClass string) string {
	return strings.ToLower(strings.TrimSpace(routeClass))
}

func turnableCarrierRouteClasses(routeByClass map[string]string) []string {
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

type turnableCarrierConn struct {
	net.Conn
	carrier       *TurnableCarrier
	releaseActive func()
	idleTimeout   time.Duration
	closeOnce     sync.Once
}

func (c *turnableCarrierConn) Read(p []byte) (int, error) {
	c.refreshDeadline()
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.refreshDeadline()
	}
	return n, err
}

func (c *turnableCarrierConn) Write(p []byte) (int, error) {
	c.refreshDeadline()
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.refreshDeadline()
	}
	return n, err
}

func (c *turnableCarrierConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() {
		c.releaseActive()
		c.carrier.closedStreams.Add(1)
	})
	return err
}

func (c *turnableCarrierConn) refreshDeadline() {
	if c.idleTimeout <= 0 {
		return
	}
	_ = c.Conn.SetDeadline(time.Now().Add(c.idleTimeout))
}
