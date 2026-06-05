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

const (
	defaultTurnableCarrierConnectTimeout = 10 * time.Second
	defaultTurnableCarrierMaxActive      = 32
	defaultTurnableCarrierMaxOpen        = 16
	defaultTurnableCarrierMaxPending     = 24
	defaultTurnableCarrierQueueTimeout   = 1500 * time.Millisecond
	defaultTurnableCarrierIdleTimeout    = 20 * time.Second
	defaultTurnableCarrierBufferSize     = 32 * 1024
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

	Logger func(string, ...any)
}

type TurnableOptions struct {
	Config     string
	ConfigFile string
}

type CarrierStats struct {
	ActiveStreams     int64
	PeakActiveStreams int64
	PendingDials      int64
	PeakPendingDials  int64
	OpenAttempts      int64
	OpenedStreams     int64
	ClosedStreams     int64
	QueuedDials       int64
	RejectedStreams   int64
	RejectedQueue     int64
	RejectedActive    int64
	RejectedOpen      int64
	FailedStreams     int64
}

type TurnableCarrier struct {
	client *turnableengine.TurnableClient
	cancel context.CancelFunc

	dialRoute func(string) (net.Conn, error)
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
	turnableconfig.Options.Interactive = false
	turnableClient := turnableengine.NewTurnableClient(*cfg)
	turnableClient.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	connectCtx, connectCancel := context.WithTimeout(runCtx, options.ConnectTimeout)
	defer connectCancel()
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- turnableClient.Connect()
	}()
	select {
	case err = <-connectDone:
		if err != nil {
			cancel()
			return nil, err
		}
	case <-connectCtx.Done():
		cancel()
		_ = turnableClient.Stop()
		return nil, fmt.Errorf("connect Turnable carrier: %w", connectCtx.Err())
	}

	carrier := &TurnableCarrier{
		client:           turnableClient,
		cancel:           cancel,
		dialRoute:        turnableClient.DialRoute,
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
	logf("Turnable carrier started routes=%d classes=%s max_active=%d max_open=%d max_pending=%d queue_timeout=%s idle_timeout=%s buffer_size=%d",
		len(cfg.Routes),
		strings.Join(turnableCarrierRouteClasses(carrier.routeByClass), ","),
		options.MaxActiveStreams,
		options.MaxOpenAttempts,
		options.MaxPendingDials,
		options.DialQueueTimeout,
		options.IdleTimeout,
		options.BufferSize,
	)
	return carrier, nil
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
	releaseActive, err := c.acquireActiveSlot(ctx, routeClass, target)
	if err != nil {
		return nil, err
	}
	releaseOpen, err := c.acquireOpenSlot(ctx, routeClass, target)
	if err != nil {
		releaseActive()
		return nil, err
	}
	defer releaseOpen()
	c.openAttempts.Add(1)

	dialCtx := ctx
	var cancel context.CancelFunc
	if _, ok := dialCtx.Deadline(); !ok && c.connectTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, c.connectTimeout)
	} else {
		dialCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	type dialResult struct {
		conn net.Conn
		err  error
	}
	result := make(chan dialResult, 1)
	go func() {
		conn, err := c.dialRoute(routeID)
		select {
		case result <- dialResult{conn: conn, err: err}:
		case <-dialCtx.Done():
			if conn != nil {
				_ = conn.Close()
			}
		}
	}()

	select {
	case result := <-result:
		if result.err != nil {
			releaseActive()
			c.failedStreams.Add(1)
			return nil, result.err
		}
		if result.conn == nil {
			releaseActive()
			c.failedStreams.Add(1)
			return nil, errors.New("Turnable carrier returned nil stream")
		}
		c.openedStreams.Add(1)
		return &turnableCarrierConn{
			Conn:          result.conn,
			carrier:       c,
			releaseActive: releaseActive,
			idleTimeout:   c.idleTimeout,
		}, nil
	case <-dialCtx.Done():
		releaseActive()
		c.failedStreams.Add(1)
		return nil, fmt.Errorf("open Turnable stream route=%s target=%s: %w", routeClass, target, dialCtx.Err())
	}
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
	return CarrierStats{
		ActiveStreams:     c.activeStreams.Load(),
		PeakActiveStreams: c.peakActiveStreams.Load(),
		PendingDials:      c.pendingDials.Load(),
		PeakPendingDials:  c.peakPendingDials.Load(),
		OpenAttempts:      c.openAttempts.Load(),
		OpenedStreams:     c.openedStreams.Load(),
		ClosedStreams:     c.closedStreams.Load(),
		QueuedDials:       c.queuedDials.Load(),
		RejectedStreams:   c.rejectedStreams.Load(),
		RejectedQueue:     c.rejectedQueue.Load(),
		RejectedActive:    c.rejectedActive.Load(),
		RejectedOpen:      c.rejectedOpen.Load(),
		FailedStreams:     c.failedStreams.Load(),
	}
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
