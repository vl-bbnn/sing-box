package connection

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	turnableconfig "github.com/theairblow/turnable/pkg/config"
	"github.com/theairblow/turnable/pkg/internal/transport"
	"golang.org/x/time/rate"
)

const (
	muxControlMagic        = "TMUX"
	muxControlVersion byte = 2

	muxControlTypeOpen       byte = 1
	muxControlTypePing       byte = 2
	muxControlTypePong       byte = 3
	muxControlTypeClose      byte = 4
	muxControlTypeDisconnect byte = 7

	defaultMuxFlowBufSize      = 2048
	defaultMuxWriteCtrlBufSize = 256
	defaultMuxFlowSendBuf      = 256
	defaultMuxOpenReplyBufSize = 256
	muxMaxPacket               = 65535

	muxDRRQuantum = 32768
	muxBurstFloor = 2 * 1420

	muxClientPingInterval = 1 * time.Second
	muxPingTimeout        = 10 * time.Second
)

// muxControlMessage is a control protocol message
type muxControlMessage struct {
	Type     byte
	FlowID   uint16
	RouteIdx byte
}

// MuxChannel is one negotiated data flow
type MuxChannel struct {
	FlowID   uint16
	RouteIdx byte
	Conn     net.Conn
}

// tinyMuxCore represents the shared core implementation of tinymux
type tinyMuxCore struct {
	conn   net.Conn
	flows  sync.Map
	nextID uint16
	nextMu sync.Mutex
	closed atomic.Bool
	done   chan struct{}
	ctrlCh chan []byte

	drFlowsMu sync.RWMutex
	drFlows   []*flowConn
	notifyCh  chan struct{}

	rateLimiter *rate.Limiter
	stageBuf    []byte

	log *slog.Logger

	flowPacketsIn    atomic.Int64
	flowBytesIn      atomic.Int64
	flowPacketsOut   atomic.Int64
	flowBytesOut     atomic.Int64
	flowDrops        atomic.Int64
	controlFramesOut atomic.Int64
	rateWaits        atomic.Int64
	rateWaitNanos    atomic.Int64
	rateBurstBytes   atomic.Int64
	lastReadNano     atomic.Int64
	lastWriteNano    atomic.Int64
}

// newTinyMuxCore creates a new tinymux core
func newTinyMuxCore(conn net.Conn) *tinyMuxCore {
	m := &tinyMuxCore{
		conn:        conn,
		nextID:      1,
		done:        make(chan struct{}),
		ctrlCh:      make(chan []byte, muxWriteCtrlBufferSize()),
		notifyCh:    make(chan struct{}, 1),
		rateLimiter: rate.NewLimiter(rate.Inf, 256*1024),
		stageBuf:    make([]byte, muxMaxPacket+2),
		log:         slog.Default(),
	}
	now := time.Now().UnixNano()
	m.lastReadNano.Store(now)
	m.lastWriteNano.Store(now)
	m.rateBurstBytes.Store(256 * 1024)

	go m.readLoop()
	go m.writeLoop()
	return m
}

// readLoop reads packets from the underlying connection and dispatches them to their flows
func (m *tinyMuxCore) readLoop() {
	defer func() {
		_ = m.Close()
	}()

	buf := make([]byte, muxMaxPacket+2)
	for {
		n, err := m.conn.Read(buf)
		if err != nil {
			return
		}
		if n < 2 {
			continue
		}
		m.lastReadNano.Store(time.Now().UnixNano())

		flowID := binary.BigEndian.Uint16(buf[:2])
		payload := make([]byte, n-2)
		copy(payload, buf[2:n])
		m.flowPacketsIn.Add(1)
		m.flowBytesIn.Add(int64(len(payload)))

		if raw, ok := m.flows.Load(flowID); ok {
			fc := raw.(*flowConn)
			select {
			case fc.incoming <- payload:
			default:
				m.flowDrops.Add(1)
				m.log.Debug("mux flow receive buffer full, dropping packet", "flow_id", flowID)
			}
		}
	}
}

// createFlow creates a flow with the given ID
func (m *tinyMuxCore) createFlow(id uint16) *flowConn {
	fc := &flowConn{
		mux:      m,
		id:       id,
		incoming: make(chan []byte, muxFlowBufferSize()),
		closed:   make(chan struct{}),
	}
	if id != 0 {
		fc.sendCh = make(chan []byte, muxFlowSendBufferSize())
		m.drFlowsMu.Lock()
		m.drFlows = append(m.drFlows, fc)
		m.drFlowsMu.Unlock()
	}
	m.flows.Store(id, fc)
	return fc
}

// allocateFlow allocates the next flow ID and creates the flow
func (m *tinyMuxCore) allocateFlow() *flowConn {
	m.nextMu.Lock()
	id := m.nextID
	m.nextID++
	if m.nextID == 0 {
		m.nextID = 1
	}
	m.nextMu.Unlock()
	return m.createFlow(id)
}

// removeFlow removes a flow by ID
func (m *tinyMuxCore) removeFlow(id uint16) {
	m.flows.Delete(id)
	if id == 0 {
		return
	}
	m.drFlowsMu.Lock()
	for i, fc := range m.drFlows {
		if fc.id == id {
			m.drFlows = append(m.drFlows[:i], m.drFlows[i+1:]...)
			break
		}
	}
	m.drFlowsMu.Unlock()
}

// drainControl flushes all pending control frames
func (m *tinyMuxCore) drainControl() {
	for {
		select {
		case frame := <-m.ctrlCh:
			if _, err := m.conn.Write(frame); err != nil {
				_ = m.Close()
				return
			}
			m.controlFramesOut.Add(1)
			m.flowBytesOut.Add(int64(len(frame)))
			m.lastWriteNano.Store(time.Now().UnixNano())
		default:
			return
		}
	}
}

// setRateLimit configures the aggregate outbound rate limit in bytes/sec
func (m *tinyMuxCore) setRateLimit(bytesPerSec float64) {
	if bytesPerSec <= 0 {
		m.rateLimiter.SetLimit(rate.Inf)
		m.rateLimiter.SetBurst(256 * 1024)
		m.rateBurstBytes.Store(256 * 1024)
	} else {
		m.rateLimiter.SetLimit(rate.Limit(bytesPerSec))
		burst := turnableconfig.Options.Transport.TinyMuxRateBurstBytes
		if burst <= 0 {
			burst = int(bytesPerSec / 20)
		}
		if burst < muxBurstFloor {
			burst = muxBurstFloor
		}
		if turnableconfig.Options.Transport.TinyMuxRateBurstBytes <= 0 && burst > 128*1024 {
			burst = 128 * 1024
		}
		m.rateLimiter.SetBurst(burst)
		m.rateBurstBytes.Store(int64(burst))
	}
}

func muxFlowBufferSize() int {
	return turnableconfig.PositiveOr(turnableconfig.Options.Transport.TinyMuxFlowBuffer, defaultMuxFlowBufSize)
}

func muxWriteCtrlBufferSize() int {
	return turnableconfig.PositiveOr(turnableconfig.Options.Transport.TinyMuxControlBuffer, defaultMuxWriteCtrlBufSize)
}

func muxFlowSendBufferSize() int {
	return turnableconfig.PositiveOr(turnableconfig.Options.Transport.TinyMuxFlowSendBuffer, defaultMuxFlowSendBuf)
}

// rateWait absorbs the rate-limiter delay for n bytes, draining control packets while sleeping.
func (m *tinyMuxCore) rateWait(n int) {
	if m.rateLimiter.Limit() == rate.Inf {
		return
	}
	rsv := m.rateLimiter.ReserveN(time.Now(), n)
	delay := rsv.Delay()
	if delay <= 0 {
		return
	}
	m.rateWaits.Add(1)
	m.rateWaitNanos.Add(int64(delay))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case frame := <-m.ctrlCh:
			if _, err := m.conn.Write(frame); err != nil {
				_ = m.Close()
				return
			}
			m.controlFramesOut.Add(1)
			m.flowBytesOut.Add(int64(len(frame)))
			m.lastWriteNano.Store(time.Now().UnixNano())
		case <-timer.C:
			return
		case <-m.done:
			return
		}
	}
}

// writeLoop drains data flows and gives each one a fair byte quantum per round
func (m *tinyMuxCore) writeLoop() {
	for {
		m.drainControl()

		m.drFlowsMu.RLock()
		flows := append([]*flowConn(nil), m.drFlows...)
		m.drFlowsMu.RUnlock()

		anyActive := false
		for _, fc := range flows {
			if len(fc.sendCh) == 0 {
				if fc.deficit > 0 {
					fc.deficit = 0
				}
				continue
			}
			fc.deficit += muxDRRQuantum

		drainLoop:
			for fc.deficit > 0 {
				select {
				case payload := <-fc.sendCh:
					n := 2 + len(payload)
					if n > len(m.stageBuf) {
						m.stageBuf = make([]byte, n)
					}
					out := m.stageBuf[:n]
					binary.BigEndian.PutUint16(out, fc.id)
					copy(out[2:], payload)
					if _, err := m.conn.Write(out); err != nil {
						_ = m.Close()
						return
					}
					m.flowPacketsOut.Add(1)
					m.flowBytesOut.Add(int64(n))
					m.lastWriteNano.Store(time.Now().UnixNano())
					fc.deficit -= len(payload)
					anyActive = true
					m.drainControl()
					m.rateWait(n)
				default:
					break drainLoop
				}
			}
			if len(fc.sendCh) == 0 && fc.deficit > 0 {
				fc.deficit = 0
			}
		}

		if !anyActive {
			select {
			case frame := <-m.ctrlCh:
				if _, err := m.conn.Write(frame); err != nil {
					_ = m.Close()
					return
				}
				m.controlFramesOut.Add(1)
				m.flowBytesOut.Add(int64(len(frame)))
				m.lastWriteNano.Store(time.Now().UnixNano())
			case <-m.notifyCh:
			case <-m.done:
				return
			}
		}
	}
}

func muxPingTimeoutDuration() time.Duration {
	if value := turnableconfig.Options.Transport.TinyMuxPingTimeoutMillis; value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return muxPingTimeout
}

func (m *tinyMuxCore) hasRecentIO(window time.Duration) bool {
	if m == nil || window <= 0 {
		return false
	}
	cutoff := time.Now().Add(-window).UnixNano()
	return m.lastReadNano.Load() >= cutoff || m.lastWriteNano.Load() >= cutoff
}

func (m *tinyMuxCore) stats() turnableconfig.TinyMuxRuntimeStats {
	if m == nil {
		return turnableconfig.TinyMuxRuntimeStats{}
	}
	return turnableconfig.TinyMuxRuntimeStats{
		FlowPacketsIn:    m.flowPacketsIn.Load(),
		FlowBytesIn:      m.flowBytesIn.Load(),
		FlowPacketsOut:   m.flowPacketsOut.Load(),
		FlowBytesOut:     m.flowBytesOut.Load(),
		FlowDrops:        m.flowDrops.Load(),
		ControlFramesOut: m.controlFramesOut.Load(),
		RateWaits:        m.rateWaits.Load(),
		RateWaitNanos:    m.rateWaitNanos.Load(),
		RateBurstBytes:   m.rateBurstBytes.Load(),
	}
}

func updateMaxAtomicInt64(slot *atomic.Int64, value int64) {
	for {
		previous := slot.Load()
		if value <= previous || slot.CompareAndSwap(previous, value) {
			return
		}
	}
}

func nanosToMillis(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return value / int64(time.Millisecond)
}

// sendControl enqueues a pre-framed control packet for the write goroutine
func (m *tinyMuxCore) sendControl(payload []byte) error {
	frame := make([]byte, 2+len(payload))
	copy(frame[2:], payload)
	select {
	case m.ctrlCh <- frame:
		return nil
	case <-m.done:
		return errors.New("mux: closed")
	}
}

// Close closes the mux and underlying connection
func (m *tinyMuxCore) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(m.done)

	m.flows.Range(func(key, value any) bool {
		_ = value.(*flowConn).Close()
		return true
	})

	return m.conn.Close()
}

// flowConn represents a mux flow's net.Conn
type flowConn struct {
	mux       *tinyMuxCore
	id        uint16
	incoming  chan []byte
	sendCh    chan []byte
	closed    chan struct{}
	deficit   int
	readBuf   []byte
	closeOnce sync.Once
}

// Read reads the next chunk from the flow's receive buffer
func (f *flowConn) Read(p []byte) (int, error) {
	if len(f.readBuf) > 0 {
		n := copy(p, f.readBuf)
		f.readBuf = f.readBuf[n:]
		return n, nil
	}
	select {
	case pkt := <-f.incoming:
		n := copy(p, pkt)
		if n < len(pkt) {
			f.readBuf = pkt[n:]
		}
		return n, nil
	case <-f.closed:
		return 0, io.EOF
	}
}

// Write forwards the payload to the underlying mux connection under this flow's ID
func (f *flowConn) Write(p []byte) (int, error) {
	if f.id == 0 {
		if err := f.mux.sendControl(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	select {
	case f.sendCh <- buf:
		select {
		case f.mux.notifyCh <- struct{}{}:
		default:
		}
		return len(p), nil
	case <-f.closed:
		return 0, errors.New("mux: flow closed")
	case <-f.mux.done:
		return 0, errors.New("mux: closed")
	}
}

// Close removes this flow from the mux and closes it
func (f *flowConn) Close() error {
	f.mux.removeFlow(f.id)
	f.closeOnce.Do(func() {
		close(f.closed)
	})
	return nil
}

// FlowID returns the flow identifier
func (f *flowConn) FlowID() uint16 { return f.id }

// LocalAddr returns the local address of the underlying mux connection
func (f *flowConn) LocalAddr() net.Addr { return f.mux.conn.LocalAddr() }

// RemoteAddr returns the remote address of the underlying mux connection
func (f *flowConn) RemoteAddr() net.Addr { return f.mux.conn.RemoteAddr() }

// SetDeadline is a stub that returns an error
func (f *flowConn) SetDeadline(t time.Time) error {
	return errors.New("setting deadline per flow is not supported")
}

// SetReadDeadline is a stub that returns an error
func (f *flowConn) SetReadDeadline(t time.Time) error {
	return errors.New("setting deadline per flow is not supported")
}

// SetWriteDeadline is a stub that returns an error
func (f *flowConn) SetWriteDeadline(t time.Time) error {
	return errors.New("setting deadline per flow is not supported")
}

// managedFlowConn wraps a flowConn and sends a Close control message when closed
type managedFlowConn struct {
	*flowConn
	writeControl func(muxControlMessage) error
	flowStates   *sync.Map
	closeOnce    sync.Once
}

// Close notifies the peer and tears down the flow.
func (m *managedFlowConn) Close() error {
	m.closeOnce.Do(func() {
		m.flowStates.Delete(m.flowConn.id)
		_ = m.writeControl(muxControlMessage{
			Type:   muxControlTypeClose,
			FlowID: m.flowConn.id,
		})
		_ = m.flowConn.Close()
	})
	return nil
}

// TinyMuxClient represents a client tinymux connection
type TinyMuxClient struct {
	mux       *tinyMuxCore
	control   net.Conn
	controlMu sync.Mutex

	lastPingSent    atomic.Int64
	lastPong        atomic.Int64
	firstUnanswered atomic.Int64
	openRequests    atomic.Int64
	openReplies     atomic.Int64
	openCanceled    atomic.Int64
	openErrors      atomic.Int64
	openPending     atomic.Int64
	peakOpenPending atomic.Int64
	pingSent        atomic.Int64
	pongReceived    atomic.Int64
	pingTimeouts    atomic.Int64
	disconnects     atomic.Int64
	controlErrors   atomic.Int64
	lastOpenLatency atomic.Int64
	maxOpenLatency  atomic.Int64
	lastPingRTT     atomic.Int64
	maxPingRTT      atomic.Int64

	pingCtx    context.Context
	pingCancel context.CancelFunc

	openReplyCh chan uint16
	flowStates  sync.Map
}

// SetRateLimit configures the aggregate outbound rate limit in bytes/sec
func (c *TinyMuxClient) SetRateLimit(bytesPerSec float64) {
	c.mux.setRateLimit(bytesPerSec)
}

// SetLogger changes the slog logger instance
func (c *TinyMuxClient) SetLogger(log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	c.mux.log = log
}

// NewTinyMuxClient creates a client tinymux connection
func NewTinyMuxClient(ctx context.Context, conn net.Conn) (*TinyMuxClient, error) {
	mux := newTinyMuxCore(conn)

	controlFlow := mux.createFlow(0)
	controlKCP, err := transport.WrapControlKCP(controlFlow)
	if err != nil {
		_ = mux.Close()
		return nil, fmt.Errorf("failed to wrap control channel with kcp: %w", err)
	}

	if _, err := controlKCP.Write([]byte(muxControlMagic)); err != nil {
		_ = controlKCP.Close()
		_ = mux.Close()
		return nil, fmt.Errorf("failed to write control preface: %w", err)
	}

	pingCtx, pingCancel := context.WithCancel(ctx)
	client := &TinyMuxClient{
		mux:         mux,
		control:     controlKCP,
		pingCtx:     pingCtx,
		pingCancel:  pingCancel,
		openReplyCh: make(chan uint16, defaultMuxOpenReplyBufSize),
	}

	now := time.Now().UnixNano()
	client.lastPingSent.Store(now)
	client.lastPong.Store(now)

	go client.pingLoop()
	go client.controlReader()
	return client, nil
}

// pingLoop sends periodic pings and closes tinymux if the server stops responding
func (c *TinyMuxClient) pingLoop() {
	ticker := time.NewTicker(muxClientPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.pingCtx.Done():
			return
		case <-ticker.C:
			pingTimeout := muxPingTimeoutDuration()
			if c.lastPong.Load() < c.lastPingSent.Load() {
				if c.firstUnanswered.Load() == 0 {
					c.firstUnanswered.Store(time.Now().UnixNano())
				} else if time.Since(time.Unix(0, c.firstUnanswered.Load())) > pingTimeout {
					if c.mux.hasRecentIO(pingTimeout) {
						c.mux.log.Debug("tinymux client pong timeout deferred because mux has recent data")
						continue
					}
					c.pingTimeouts.Add(1)
					c.mux.log.Debug("tinymux client pong timeout", "timeout", pingTimeout)
					c.pingCancel()
					_ = c.mux.Close()
					return
				}
			} else {
				c.firstUnanswered.Store(0)
			}

			c.controlMu.Lock()
			err := writeControlMessage(c.control, muxControlMessage{Type: muxControlTypePing})
			c.controlMu.Unlock()

			if err == nil {
				c.lastPingSent.Store(time.Now().UnixNano())
				c.pingSent.Add(1)
			}
		}
	}
}

// controlReader reads and dispatches incoming control messages from the server
func (c *TinyMuxClient) controlReader() {
	for {
		msg, err := readControlMessage(c.control)
		if err != nil {
			select {
			case <-c.pingCtx.Done():
			default:
				c.controlErrors.Add(1)
				c.mux.log.Debug("tinymux client cut off unexpectedly", "error", err)
				c.pingCancel()
				_ = c.mux.Close()
			}
			return
		}
		switch msg.Type {
		case muxControlTypePong:
			now := time.Now().UnixNano()
			lastPing := c.lastPingSent.Load()
			if lastPing > 0 && now > lastPing {
				rtt := now - lastPing
				c.lastPingRTT.Store(rtt)
				updateMaxAtomicInt64(&c.maxPingRTT, rtt)
			}
			c.lastPong.Store(now)
			c.pongReceived.Add(1)
		case muxControlTypeOpen:
			c.openReplies.Add(1)
			select {
			case c.openReplyCh <- msg.FlowID:
			case <-c.pingCtx.Done():
				return
			}
		case muxControlTypeClose:
			if raw, ok := c.flowStates.LoadAndDelete(msg.FlowID); ok {
				_ = raw.(*managedFlowConn).flowConn.Close()
			}
		case muxControlTypeDisconnect:
			c.disconnects.Add(1)
			c.mux.log.Debug("tinymux client received disconnect, triggering full reconnect")
			c.pingCancel()
			_ = c.mux.Close()
			return
		}
	}
}

// Done returns a channel closed when the mux session terminates
func (c *TinyMuxClient) Done() <-chan struct{} { return c.pingCtx.Done() }

// OpenChannel requests a new data flow from the server and returns it.
func (c *TinyMuxClient) OpenChannel(routeIdx byte) (net.Conn, error) {
	return c.OpenChannelContext(context.Background(), routeIdx)
}

// OpenChannelContext requests a new data flow from the server and aborts the
// pending request when ctx expires. The current control protocol does not carry
// request IDs, so the mux session is closed on cancellation to avoid stale open
// replies being consumed by later calls.
func (c *TinyMuxClient) OpenChannelContext(ctx context.Context, routeIdx byte) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	c.openRequests.Add(1)
	pending := c.openPending.Add(1)
	updateMaxAtomicInt64(&c.peakOpenPending, pending)
	defer c.openPending.Add(-1)

	if err := c.writeControl(muxControlMessage{Type: muxControlTypeOpen, RouteIdx: routeIdx}); err != nil {
		c.openErrors.Add(1)
		return nil, err
	}
	select {
	case flowID := <-c.openReplyCh:
		latency := time.Since(startedAt).Nanoseconds()
		c.lastOpenLatency.Store(latency)
		updateMaxAtomicInt64(&c.maxOpenLatency, latency)
		fc := c.mux.createFlow(flowID)
		mf := &managedFlowConn{flowConn: fc, writeControl: c.writeControl, flowStates: &c.flowStates}
		c.flowStates.Store(flowID, mf)
		c.mux.log.Debug("tinymux channel opened", "side", "client", "flow_id", flowID)
		return mf, nil
	case <-ctx.Done():
		c.openCanceled.Add(1)
		c.openErrors.Add(1)
		c.mux.log.Debug("tinymux channel open canceled, closing mux to discard stale reply", "route_idx", routeIdx, "elapsed", time.Since(startedAt), "error", ctx.Err())
		c.pingCancel()
		_ = c.mux.Close()
		return nil, ctx.Err()
	case <-c.pingCtx.Done():
		c.openErrors.Add(1)
		return nil, errors.New("tinymux client session closed")
	}
}

// Stats returns diagnostics-safe client mux counters.
func (c *TinyMuxClient) Stats() turnableconfig.TinyMuxRuntimeStats {
	if c == nil {
		return turnableconfig.TinyMuxRuntimeStats{}
	}
	stats := c.mux.stats()
	stats.OpenRequests = c.openRequests.Load()
	stats.OpenReplies = c.openReplies.Load()
	stats.OpenCanceled = c.openCanceled.Load()
	stats.OpenErrors = c.openErrors.Load()
	stats.OpenPending = c.openPending.Load()
	stats.PeakOpenPending = c.peakOpenPending.Load()
	stats.PingSent = c.pingSent.Load()
	stats.PongReceived = c.pongReceived.Load()
	stats.PingTimeouts = c.pingTimeouts.Load()
	stats.Disconnects = c.disconnects.Load()
	stats.ControlReadErrors = c.controlErrors.Load()
	stats.LastOpenLatencyMillis = nanosToMillis(c.lastOpenLatency.Load())
	stats.MaxOpenLatencyMillis = nanosToMillis(c.maxOpenLatency.Load())
	stats.LastPingRTTMillis = nanosToMillis(c.lastPingRTT.Load())
	stats.MaxPingRTTMillis = nanosToMillis(c.maxPingRTT.Load())
	return stats
}

// Disconnect sends a Disconnect message to the server
func (c *TinyMuxClient) Disconnect() error {
	return c.writeControl(muxControlMessage{Type: muxControlTypeDisconnect})
}

// Close tears down the client mux session
func (c *TinyMuxClient) Close() error {
	if c.pingCancel != nil {
		c.pingCancel()
	}
	return errors.Join(
		func() error {
			if c.control == nil {
				return nil
			}
			return c.control.Close()
		}(),
		func() error {
			if c.mux == nil {
				return nil
			}
			return c.mux.Close()
		}(),
	)
}

// writeControl serializes and writes a control message under the control mutex
func (c *TinyMuxClient) writeControl(msg muxControlMessage) error {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	return writeControlMessage(c.control, msg)
}

// TinyMuxServer accepts logical channels from one authenticated connection
type TinyMuxServer struct {
	mux       *tinyMuxCore
	control   net.Conn
	controlMu sync.Mutex

	lastPing   atomic.Int64
	flowStates sync.Map
	doneOnce   sync.Once
	done       chan struct{}
}

// NewTinyMuxServer creates a mux server over the provided packet-centric connection
func NewTinyMuxServer(conn net.Conn) (*TinyMuxServer, error) {
	mux := newTinyMuxCore(conn)

	controlFlow := mux.createFlow(0)
	controlKCP, err := transport.WrapControlKCP(controlFlow)
	if err != nil {
		_ = mux.Close()
		return nil, fmt.Errorf("failed to wrap control channel with kcp: %w", err)
	}

	preface := make([]byte, 4)
	if _, err := io.ReadFull(controlKCP, preface); err != nil {
		_ = controlKCP.Close()
		_ = mux.Close()
		return nil, fmt.Errorf("failed to read control preface: %w", err)
	}

	if string(preface) != muxControlMagic {
		_ = controlKCP.Close()
		_ = mux.Close()
		return nil, errors.New("invalid tinymux control preface")
	}

	return &TinyMuxServer{
		mux:     mux,
		control: controlKCP,
		done:    make(chan struct{}),
	}, nil
}

// AcceptChannels emits fully negotiated data flows
func (s *TinyMuxServer) AcceptChannels(ctx context.Context) <-chan MuxChannel {
	out := make(chan MuxChannel)
	s.lastPing.Store(time.Now().UnixNano())

	go s.pingTimeoutLoop(ctx)
	go func() {
		defer close(out)
		for {
			msg, err := readControlMessage(s.control)
			if err != nil {
				select {
				case <-ctx.Done():
				default:
					s.mux.log.Debug("tinymux server control stopped", "error", err)
				}

				s.doneOnce.Do(func() { close(s.done) })
				_ = s.Close()
				return
			}

			switch msg.Type {
			case muxControlTypePing:
				s.lastPing.Store(time.Now().UnixNano())
				s.controlMu.Lock()
				_ = writeControlMessage(s.control, muxControlMessage{Type: muxControlTypePong})
				s.controlMu.Unlock()
			case muxControlTypeOpen:
				fc := s.mux.allocateFlow()
				sf := &managedFlowConn{flowConn: fc, writeControl: s.writeControl, flowStates: &s.flowStates}
				s.flowStates.Store(fc.id, sf)

				s.controlMu.Lock()
				_ = writeControlMessage(s.control, muxControlMessage{
					Type:   muxControlTypeOpen,
					FlowID: fc.id,
				})
				s.controlMu.Unlock()

				s.mux.log.Debug("tinymux channel opened", "side", "server", "flow_id", fc.id)
				select {
				case out <- MuxChannel{
					FlowID:   fc.id,
					RouteIdx: msg.RouteIdx,
					Conn:     sf,
				}:
				case <-ctx.Done():
					_ = s.Close()
					return
				}
			case muxControlTypeClose:
				if raw, ok := s.flowStates.LoadAndDelete(msg.FlowID); ok {
					_ = raw.(*managedFlowConn).flowConn.Close()
				}
			case muxControlTypeDisconnect:
				s.mux.log.Debug("tinymux server received disconnect")
				s.doneOnce.Do(func() { close(s.done) })
				_ = s.Close()
				return
			}
		}
	}()

	return out
}

// pingTimeoutLoop closes the mux if the client stops sending pings within the timeout window
func (s *TinyMuxServer) pingTimeoutLoop(ctx context.Context) {
	pingTimeout := muxPingTimeoutDuration()
	ticker := time.NewTicker(pingTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			if time.Since(time.Unix(0, s.lastPing.Load())) > pingTimeout {
				if s.mux.hasRecentIO(pingTimeout) {
					s.mux.log.Debug("tinymux server ping timeout deferred because mux has recent data")
					continue
				}
				s.mux.log.Debug("tinymux server ping timeout", "timeout", pingTimeout)
				_ = s.Close()
				return
			}
		}
	}
}

// SetRateLimit configures the aggregate outbound rate limit in bytes/sec; 0 = unlimited.
func (s *TinyMuxServer) SetRateLimit(bytesPerSec float64) {
	s.mux.setRateLimit(bytesPerSec)
}

// SetLogger changes the slog logger instance
func (s *TinyMuxServer) SetLogger(log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	s.mux.log = log
}

// writeControl serializes and writes a control message under the control mutex
func (s *TinyMuxServer) writeControl(msg muxControlMessage) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	return writeControlMessage(s.control, msg)
}

// Disconnect sends a Disconnect message to the client
func (s *TinyMuxServer) Disconnect() error {
	return s.writeControl(muxControlMessage{Type: muxControlTypeDisconnect})
}

// Close tears down the server mux session
func (s *TinyMuxServer) Close() error {
	s.doneOnce.Do(func() { close(s.done) })
	return errors.Join(
		func() error {
			if s.control == nil {
				return nil
			}
			return s.control.Close()
		}(),
		func() error {
			if s.mux == nil {
				return nil
			}
			return s.mux.Close()
		}(),
	)
}

// writeControlMessage writes one 5-byte control frame
func writeControlMessage(w io.Writer, msg muxControlMessage) error {
	frame := make([]byte, 5)
	frame[0] = muxControlVersion
	frame[1] = msg.Type
	binary.BigEndian.PutUint16(frame[2:], msg.FlowID)
	frame[4] = msg.RouteIdx
	_, err := w.Write(frame)
	return err
}

// readControlMessage reads one 5-byte control frame
func readControlMessage(r io.Reader) (muxControlMessage, error) {
	frame := make([]byte, 5)
	if _, err := io.ReadFull(r, frame); err != nil {
		return muxControlMessage{}, err
	}

	if frame[0] != muxControlVersion {
		return muxControlMessage{}, fmt.Errorf("unsupported tinymux control version %d", frame[0])
	}

	msg := muxControlMessage{
		Type:     frame[1],
		FlowID:   binary.BigEndian.Uint16(frame[2:]),
		RouteIdx: frame[4],
	}

	switch msg.Type {
	case muxControlTypeOpen, muxControlTypePing, muxControlTypePong,
		muxControlTypeClose, muxControlTypeDisconnect:
	default:
		return muxControlMessage{}, fmt.Errorf("unknown control message type %d", msg.Type)
	}

	return msg, nil
}
