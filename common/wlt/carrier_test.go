//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

func TestCarrierRouteMapUsesRouteClasses(t *testing.T) {
	cfg := &carrierconfig.ClientConfig{Routes: []carrierconfig.ClientRoute{
		{RouteID: "vless-reality-main", Socket: "tcp", Transport: "srtp"},
		{RouteID: "vless-reality-eu", Socket: "tcp", Transport: "srtp"},
		{RouteID: "vless-reality-ru", Socket: "tcp", Transport: "srtp"},
	}}

	routeByClass := carrierRouteMap(cfg)
	want := map[string]string{
		"vless-reality-main": "vless-reality-main",
		"vless-reality-eu":   "vless-reality-eu",
		"vless-reality-ru":   "vless-reality-ru",
		"direct":             "vless-reality-main",
		"eu":                 "vless-reality-eu",
		"ru":                 "vless-reality-ru",
	}
	if !reflect.DeepEqual(routeByClass, want) {
		t.Fatalf("route map=%v, want %v", routeByClass, want)
	}
}

func TestFatalCarrierConnectErrorIncludesManualCaptchaUnavailable(t *testing.T) {
	err := errors.Join(errors.New("connect failed"), carriercommon.ErrManualCaptchaUnavailable)
	if !isFatalCarrierConnectError(err) {
		t.Fatal("manual captcha unavailable error should be fatal for startup connect")
	}
}

func TestLoadCarrierAuthSnapshotImportsSnapshot(t *testing.T) {
	expiresAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	snapshot := fmt.Sprintf(`{
		"version":1,
		"platform_id":"vk.com",
		"call_id":"snapshot-test",
		"username":"tester",
		"expires_at":%q,
		"vk":{
			"messages_access_token":"messages",
			"anonym_token":"anonymous",
			"session_key":"session",
			"device_id":"device",
			"endpoint":"wss://example.invalid/ws",
			"turn_user":"turn-user",
			"turn_pass":"turn-pass",
			"turn_addr":"turn:one.example.invalid",
			"turn_addrs":["turn:one.example.invalid"]
		}
	}`, expiresAt)
	if err := loadCarrierAuthSnapshot(CarrierOptions{AuthSnapshot: snapshot}, nil); err != nil {
		t.Fatal(err)
	}
	exported, err := carrierengine.ExportAuthSnapshotJSON(carrierconfig.ClientConfig{
		PlatformID: "vk.com",
		CallID:     "snapshot-test",
		Username:   "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exported), `"anonym_token":"anonymous"`) {
		t.Fatalf("exported snapshot does not contain imported token: %s", exported)
	}
}

func TestLoadCarrierAuthSnapshotIgnoresMissingFile(t *testing.T) {
	missingPath := t.TempDir() + "/missing-auth-snapshot.json"
	if err := loadCarrierAuthSnapshot(CarrierOptions{AuthSnapshotFile: missingPath}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCarrierDefaultsAreIPhoneBounded(t *testing.T) {
	options := withCarrierDefaults(CarrierOptions{})
	if options.MaxActiveStreams != defaultCarrierMaxActive {
		t.Fatalf("max active=%d, want %d", options.MaxActiveStreams, defaultCarrierMaxActive)
	}
	if options.MaxOpenAttempts != defaultCarrierMaxOpen {
		t.Fatalf("max open=%d, want %d", options.MaxOpenAttempts, defaultCarrierMaxOpen)
	}
	if options.MaxPendingDials != defaultCarrierMaxPending {
		t.Fatalf("max pending=%d, want %d", options.MaxPendingDials, defaultCarrierMaxPending)
	}
	if options.DialQueueTimeout != defaultCarrierQueueTimeout {
		t.Fatalf("queue timeout=%s, want %s", options.DialQueueTimeout, defaultCarrierQueueTimeout)
	}
	if options.IdleTimeout != defaultCarrierIdleTimeout {
		t.Fatalf("idle timeout=%s, want %s", options.IdleTimeout, defaultCarrierIdleTimeout)
	}
	if options.BufferSize != 32*1024 {
		t.Fatalf("buffer size=%d, want 32768", options.BufferSize)
	}
}

func TestCarrierRuntimeOptionsAreBounded(t *testing.T) {
	previousOptions := carrierconfig.Options
	defer func() {
		carrierconfig.Options = previousOptions
	}()

	carrierconfig.Options.Transport.RelayBandwidthBytesPerSecond = 1234
	options := withCarrierDefaults(CarrierOptions{})
	transportOptions := applyCarrierRuntimeOptions(options)
	if !reflect.DeepEqual(transportOptions, carrierconfig.TransportOptions{
		TinyMuxFlowBuffer:            defaultCarrierTinyMuxFlowBuffer,
		TinyMuxFlowSendBuffer:        defaultCarrierTinyMuxFlowSendBuffer,
		TinyMuxControlBuffer:         defaultCarrierTinyMuxControlBuffer,
		TinyMuxPingTimeoutMillis:     int(defaultCarrierTinyMuxPingTimeout / time.Millisecond),
		PeerIncomingBuffer:           defaultCarrierPeerIncomingBuffer,
		PeerWriteBuffer:              defaultCarrierPeerWriteBuffer,
		SRTPPacketBuffer:             defaultCarrierSRTPPacketBuffer,
		KCPWindowSize:                defaultCarrierKCPWindowSize,
		KCPReadWriteBuffer:           defaultCarrierKCPReadWriteBuffer,
		RelayBandwidthBytesPerSecond: 1234,
	}) {
		t.Fatalf("transport options=%+v", transportOptions)
	}

	transportOptions = applyCarrierRuntimeOptions(CarrierOptions{
		BufferSize:                   64 * 1024,
		TinyMuxRateBurstBytes:        512 * 1024,
		TinyMuxPingTimeout:           25 * time.Second,
		RelayBandwidthBytesPerSecond: 5 * 1024 * 1024,
	})
	if transportOptions.TinyMuxFlowBuffer != 256 ||
		transportOptions.TinyMuxFlowSendBuffer != 64 ||
		transportOptions.TinyMuxRateBurstBytes != 512*1024 ||
		transportOptions.TinyMuxPingTimeoutMillis != 25000 ||
		transportOptions.PeerIncomingBuffer != 256 ||
		transportOptions.SRTPPacketBuffer != 512 ||
		transportOptions.KCPWindowSize != 768 ||
		transportOptions.KCPReadWriteBuffer != 1024*1024 ||
		transportOptions.RelayBandwidthBytesPerSecond != 5*1024*1024 {
		t.Fatalf("scaled transport options=%+v", transportOptions)
	}
}

func TestCarrierDialStreamUsesRouteClassAndStats(t *testing.T) {
	carrier := newTestCarrier(2, 2, 1, time.Second, time.Second)
	var (
		mu     sync.Mutex
		dialed []string
	)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		mu.Lock()
		dialed = append(dialed, routeID)
		mu.Unlock()
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	conn, err := carrier.DialStream(context.Background(), "eu", "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	stats := carrier.Stats()
	if stats.OpenAttempts != 1 || stats.OpenedStreams != 1 || stats.ActiveStreams != 1 || stats.PeakActiveStreams != 1 || stats.PendingDials != 0 {
		t.Fatalf("stats after dial=%+v", stats)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	stats = carrier.Stats()
	if stats.ActiveStreams != 0 || stats.PeakActiveStreams != 1 || stats.ClosedStreams != 1 {
		t.Fatalf("stats after close=%+v", stats)
	}
	if want := []string{"route-eu"}; !reflect.DeepEqual(dialed, want) {
		t.Fatalf("dialed routes=%v, want %v", dialed, want)
	}
}

func TestCarrierQueuesOverActiveLimit(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1 && stats.PeakPendingDials == 1 && stats.QueuedDials == 1
	})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	defer second.conn.Close()
	stats := carrier.Stats()
	if stats.RejectedStreams != 0 || stats.ActiveStreams != 1 || stats.PeakActiveStreams != 1 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want queued dial without rejection", stats)
	}
}

func TestCarrierRejectsActiveQueueTimeout(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, 20*time.Millisecond)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := carrier.DialStream(context.Background(), "eu", "two.example:443"); err == nil {
		t.Fatal("expected active queue timeout error")
	}
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedActive != 1 || stats.ActiveStreams != 1 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want one active timeout rejection", stats)
	}
}

func TestCarrierRejectsOverPendingLimit(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	first, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1
	})
	if _, err := carrier.DialStream(context.Background(), "eu", "three.example:443"); err == nil {
		t.Fatal("expected pending limit error")
	}
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedQueue != 1 || stats.PendingDials != 1 {
		t.Fatalf("stats=%+v, want one pending rejection", stats)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	_ = second.conn.Close()
}

func TestCarrierQueuesOverOpenAttemptLimit(t *testing.T) {
	carrier := newTestCarrier(2, 1, 1, time.Second, time.Second)
	started := make(chan struct{})
	unblock := make(chan struct{})
	var (
		mu    sync.Mutex
		calls int
	)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		mu.Lock()
		calls++
		call := calls
		mu.Unlock()
		if call == 1 {
			close(started)
			<-unblock
		}
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	firstDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
		firstDone <- ioResult{conn: conn, err: err}
	}()
	<-started
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1 && stats.PeakPendingDials == 1 && stats.QueuedDials == 1
	})
	close(unblock)
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	defer first.conn.Close()
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	defer second.conn.Close()
	stats := carrier.Stats()
	if stats.RejectedStreams != 0 || stats.RejectedOpen != 0 || stats.OpenAttempts != 2 || stats.OpenedStreams != 2 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want queued open attempts without rejection", stats)
	}
}

func TestCarrierRejectsOverOpenAttemptLimit(t *testing.T) {
	carrier := newTestCarrier(2, 1, 1, time.Second, 20*time.Millisecond)
	started := make(chan struct{})
	unblock := make(chan struct{})
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		select {
		case <-started:
		default:
			close(started)
		}
		<-unblock
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	firstDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
		firstDone <- ioResult{conn: conn, err: err}
	}()
	<-started
	if _, err := carrier.DialStream(context.Background(), "eu", "two.example:443"); err == nil {
		t.Fatal("expected open attempt limit error")
	}
	close(unblock)
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	_ = first.conn.Close()
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedOpen != 1 || stats.OpenAttempts != 1 || stats.QueuedDials != 1 || stats.PeakPendingDials != 1 {
		t.Fatalf("stats=%+v, want one queued open-attempt timeout and one attempted open", stats)
	}
}

func TestCarrierRejectsOverPendingLimitWhileOpenFull(t *testing.T) {
	carrier := newTestCarrier(3, 1, 1, time.Second, time.Second)
	started := make(chan struct{})
	unblock := make(chan struct{})
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		select {
		case <-started:
		default:
			close(started)
		}
		<-unblock
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}

	firstDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "direct", "one.example:443")
		firstDone <- ioResult{conn: conn, err: err}
	}()
	<-started
	secondDone := make(chan ioResult, 1)
	go func() {
		conn, err := carrier.DialStream(context.Background(), "eu", "two.example:443")
		secondDone <- ioResult{conn: conn, err: err}
	}()
	waitForCarrierStat(t, carrier, func(stats CarrierStats) bool {
		return stats.PendingDials == 1 && stats.PeakPendingDials == 1
	})
	if _, err := carrier.DialStream(context.Background(), "eu", "three.example:443"); err == nil {
		t.Fatal("expected pending limit error while open attempt is full")
	}
	stats := carrier.Stats()
	if stats.RejectedStreams != 1 || stats.RejectedQueue != 1 || stats.PendingDials != 1 {
		t.Fatalf("stats=%+v, want one pending rejection while open attempt is full", stats)
	}
	close(unblock)
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	_ = first.conn.Close()
	second := <-secondDone
	if second.err != nil {
		t.Fatal(second.err)
	}
	_ = second.conn.Close()
}

func TestCarrierDialStreamCancelsOpen(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, 30*time.Millisecond, time.Second)
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = routeID
		<-ctx.Done()
		return nil, ctx.Err()
	}

	if _, err := carrier.DialStream(context.Background(), "eu", "slow.example:443"); err == nil {
		t.Fatal("expected open timeout")
	}
	stats := carrier.Stats()
	if stats.FailedStreams != 1 || stats.ActiveStreams != 0 || stats.OpenAttempts != 1 || stats.LastDialMillis <= 0 {
		t.Fatalf("stats=%+v, want failed canceled open with released active slot", stats)
	}
}

func TestCarrierDialStreamWaitsForReconnect(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	var calls int
	ready := make(chan struct{})
	carrier.waitReady = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ready:
			return nil
		}
	}
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		_ = routeID
		calls++
		if calls == 1 {
			return nil, errors.New("full reconnect is in progress")
		}
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ready)
	}()

	conn, err := carrier.DialStream(context.Background(), "eu", "reconnect.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stats := carrier.Stats()
	if calls != 2 || stats.OpenAttempts != 2 || stats.ReconnectRetries != 1 || stats.ReconnectWaitMillis <= 0 || stats.FailedStreams != 0 {
		t.Fatalf("calls=%d stats=%+v, want one reconnect retry and successful open", calls, stats)
	}
}

func TestCarrierDialStreamBacksOffWhenReconnectWaitReturnsEarly(t *testing.T) {
	carrier := newTestCarrier(1, 1, 1, 120*time.Millisecond, time.Second)
	var calls int
	carrier.waitReady = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	carrier.dialRoute = func(ctx context.Context, routeID string) (net.Conn, error) {
		_ = ctx
		_ = routeID
		calls++
		return nil, errors.New("full reconnect is in progress")
	}

	startedAt := time.Now()
	if _, err := carrier.DialStream(context.Background(), "eu", "reconnect.example:443"); err == nil {
		t.Fatal("expected reconnect timeout")
	}
	elapsed := time.Since(startedAt)
	stats := carrier.Stats()
	if calls > 10 || stats.OpenAttempts > 10 {
		t.Fatalf("calls=%d stats=%+v, want bounded reconnect retry loop", calls, stats)
	}
	if stats.ReconnectRetries == 0 || stats.ReconnectWaitMillis == 0 || elapsed < 50*time.Millisecond {
		t.Fatalf("elapsed=%s stats=%+v, want reconnect backoff before timeout", elapsed, stats)
	}
	if stats.FailedStreams != 1 || stats.ActiveStreams != 0 {
		t.Fatalf("stats=%+v, want failed reconnect open with released active slot", stats)
	}
}

func TestCarrierIdleTimeoutClosesStaleTail(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	carrier := newTestCarrier(1, 1, 1, time.Second, time.Second)
	conn := &carrierConn{
		Conn:          left,
		carrier:       carrier,
		releaseActive: func() {},
		idleTimeout:   30 * time.Millisecond,
	}
	defer conn.Close()

	_, err := conn.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected idle read timeout")
	}
	netErr, ok := err.(net.Error)
	if !ok || !netErr.Timeout() {
		t.Fatalf("read error=%v, want timeout", err)
	}
}

type ioResult struct {
	conn io.ReadWriteCloser
	err  error
}

func newTestCarrier(maxActive int, maxOpen int, maxPending int, connectTimeout time.Duration, queueTimeout time.Duration) *Carrier {
	return &Carrier{
		connectTimeout:   connectTimeout,
		dialQueueTimeout: queueTimeout,
		idleTimeout:      defaultCarrierIdleTimeout,
		bufferSize:       defaultCarrierBufferSize,
		activeSlots:      make(chan struct{}, maxActive),
		openSlots:        make(chan struct{}, maxOpen),
		pendingSlots:     make(chan struct{}, maxPending),
		routeByClass: map[string]string{
			"direct": "route-direct",
			"eu":     "route-eu",
		},
	}
}

func waitForCarrierStat(t *testing.T, carrier *Carrier, predicate func(CarrierStats) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if predicate(carrier.Stats()) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for carrier stats, last=%+v", carrier.Stats())
}
