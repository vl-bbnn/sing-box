//go:build with_wlt

package wlt

import (
	"context"
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
	carrierConnectAttemptMax = 8 * time.Second

	carrierReconnectRetryInitial = 20 * time.Millisecond
	carrierReconnectRetryMax     = 250 * time.Millisecond

	defaultAuthSnapshotFetchTimeout = 5 * time.Second
	maxAuthSnapshotBytes            = 256 * 1024
)

type CarrierOptions struct {
	Config     string
	ConfigFile string

	AuthSnapshot             string
	AuthSnapshotFile         string
	AuthSnapshotURL          string
	AuthSnapshotFetchTimeout time.Duration
	AuthSnapshotOutputFile   string

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
	Runtime              carrierconfig.RuntimeStats
}

type Carrier struct {
	client *carrierengine.Client
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

func StartCarrier(ctx context.Context, options CarrierOptions) (*Carrier, error) {
	logf := options.Logger
	if logf == nil {
		logf = log.Printf
	}
	startedAt := time.Now()
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
	if err := loadCarrierAuthSnapshot(ctx, options, logf); err != nil {
		if logf != nil {
			logf("WLT carrier start failed phase=auth_snapshot elapsed=%s error=%v", time.Since(startedAt), err)
		}
		return nil, err
	}
	options = withCarrierDefaults(options)
	runCtx, cancel := context.WithCancel(ctx)
	transportOptions := applyCarrierRuntimeOptions(options)
	if logf != nil {
		logf("WLT carrier start phase=runtime_options max_active=%d max_open=%d max_pending=%d queue_timeout=%s connect_timeout=%s idle_timeout=%s buffer_size=%d mux_flow_buffer=%d mux_send_buffer=%d mux_control_buffer=%d mux_burst=%d peer_incoming_buffer=%d peer_write_buffer=%d srtp_packet_buffer=%d kcp_window=%d kcp_buffer=%d relay_bandwidth=%d elapsed=%s",
			options.MaxActiveStreams,
			options.MaxOpenAttempts,
			options.MaxPendingDials,
			options.DialQueueTimeout,
			options.ConnectTimeout,
			options.IdleTimeout,
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

	runtimeClient, err := connectCarrierClient(runCtx, cfg, options.ConnectTimeout, logf)
	if err != nil {
		if snapshotErr := saveCarrierAuthSnapshot(options, cfg, logf); snapshotErr != nil && logf != nil {
			logf("WLT carrier auth snapshot save after failed connect failed error=%v", snapshotErr)
		}
		cancel()
		if logf != nil {
			logf("WLT carrier start failed phase=connect elapsed=%s error=%v", time.Since(startedAt), err)
		}
		return nil, err
	}
	if err := saveCarrierAuthSnapshot(options, cfg, logf); err != nil && logf != nil {
		logf("WLT carrier auth snapshot save failed error=%v", err)
	}

	carrier := &Carrier{
		client:           runtimeClient,
		cancel:           cancel,
		dialRoute:        runtimeClient.DialRouteContext,
		waitReady:        runtimeClient.WaitReady,
		logf:             logf,
		connectTimeout:   options.ConnectTimeout,
		dialQueueTimeout: options.DialQueueTimeout,
		idleTimeout:      options.IdleTimeout,
		bufferSize:       options.BufferSize,
		activeSlots:      make(chan struct{}, options.MaxActiveStreams),
		openSlots:        make(chan struct{}, options.MaxOpenAttempts),
		pendingSlots:     make(chan struct{}, options.MaxPendingDials),
		routeByClass:     carrierRouteMap(cfg),
	}
	go func() {
		<-runCtx.Done()
		_ = carrier.Close()
	}()
	logf("WLT carrier started routes=%d classes=%s max_active=%d max_open=%d max_pending=%d queue_timeout=%s idle_timeout=%s buffer_size=%d kcp_window=%d kcp_buffer=%d mux_flow_buffer=%d mux_send_buffer=%d mux_control_buffer=%d mux_burst=%d mux_ping_timeout_ms=%d peer_incoming_buffer=%d peer_write_buffer=%d adaptive_peer_data=%t adaptive_peer_threshold=%d adaptive_peer_idle_ms=%d srtp_packet_buffer=%d relay_bandwidth=%d",
		len(cfg.Routes),
		strings.Join(carrierRouteClasses(carrier.routeByClass), ","),
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

func connectCarrierClient(ctx context.Context, cfg *carrierconfig.ClientConfig, timeout time.Duration, logf func(string, ...any)) (*carrierengine.Client, error) {
	startedAt := time.Now()
	connectCtx, connectCancel := context.WithTimeout(ctx, timeout)
	defer connectCancel()

	delay := carrierConnectRetryInitial
	var lastErr error
	for attempt := 1; ; attempt++ {
		attemptStartedAt := time.Now()
		runtimeClient := carrierengine.NewClient(*cfg)
		runtimeClient.SetLogger(newLogfSlogLogger(logf))
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
	return errors.Is(err, carriercommon.ErrManualCaptchaUnavailable)
}

func loadCarrierAuthSnapshot(ctx context.Context, options CarrierOptions, logf func(string, ...any)) error {
	if snapshotURL := strings.TrimSpace(options.AuthSnapshotURL); snapshotURL != "" {
		if raw, err := fetchCarrierAuthSnapshot(ctx, snapshotURL, options.AuthSnapshotFetchTimeout); err != nil {
			if logf != nil {
				logf("WLT carrier auth snapshot remote refresh unavailable error=%v", err)
			}
		} else if err := carrierengine.ImportAuthSnapshotJSON(raw); err != nil {
			if logf != nil {
				logf("WLT carrier auth snapshot remote refresh ignored error=%v", err)
			}
		} else {
			if err := writeCarrierAuthSnapshot(options, raw); err != nil && logf != nil {
				logf("WLT carrier auth snapshot remote cache write failed error=%v", err)
			}
			if logf != nil {
				logf("WLT carrier start phase=auth_snapshot_loaded source=remote")
			}
			return nil
		}
	}
	raw := strings.TrimSpace(options.AuthSnapshot)
	source := ""
	if raw != "" {
		source = "inline"
	} else if path := strings.TrimSpace(options.AuthSnapshotFile); path != "" {
		content, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if logf != nil {
					logf("WLT carrier start phase=auth_snapshot_missing source=file")
				}
				return nil
			}
			return fmt.Errorf("read auth snapshot file: %w", err)
		}
		raw = strings.TrimSpace(string(content))
		source = "file"
	}
	if raw == "" {
		if source == "file" && logf != nil {
			logf("WLT carrier start phase=auth_snapshot_empty source=file")
		}
		return nil
	}
	if err := carrierengine.ImportAuthSnapshotJSON([]byte(raw)); err != nil {
		if source == "file" {
			if logf != nil {
				logf("WLT carrier start phase=auth_snapshot_ignored source=file error=%v", err)
			}
			return nil
		}
		return fmt.Errorf("import auth snapshot: %w", err)
	}
	if logf != nil {
		logf("WLT carrier start phase=auth_snapshot_loaded source=%s", source)
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
	if err := writeCarrierAuthSnapshot(options, data); err != nil {
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

func writeCarrierAuthSnapshot(options CarrierOptions, data []byte) error {
	path := strings.TrimSpace(options.AuthSnapshotOutputFile)
	if path == "" {
		path = strings.TrimSpace(options.AuthSnapshotFile)
	}
	if path == "" {
		return nil
	}
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
	c.openedStreams.Add(1)
	return &carrierConn{
		Conn:          conn,
		carrier:       c,
		releaseActive: releaseActive,
		idleTimeout:   c.idleTimeout,
	}, nil
}

func (c *Carrier) Close() error {
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

func (c *Carrier) acquireActiveSlot(ctx context.Context, routeClass string, target string) (func(), error) {
	acquired := tryAcquire(c.activeSlots)
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
	if tryAcquire(c.openSlots) {
		return func() {
			release(c.openSlots)
		}, nil
	}
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
	case c.openSlots <- struct{}{}:
		return func() {
			release(c.openSlots)
		}, nil
	case <-waitCtx.Done():
		c.rejectedOpen.Add(1)
		c.rejectedStreams.Add(1)
		return nil, fmt.Errorf("WLT carrier open attempt limit reached route=%s target=%s: %w", routeClass, target, waitCtx.Err())
	}
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
	closeOnce     sync.Once
}

func (c *carrierConn) Read(p []byte) (int, error) {
	c.refreshDeadline()
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.refreshDeadline()
	}
	return n, err
}

func (c *carrierConn) Write(p []byte) (int, error) {
	c.refreshDeadline()
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.refreshDeadline()
	}
	return n, err
}

func (c *carrierConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() {
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
