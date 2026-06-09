package config

import (
	"os"
	"strconv"
	"strings"
)

// GlobalOptions represents global options
type GlobalOptions struct {
	Interactive bool             // Allow interactive operations
	Transport   TransportOptions // Runtime transport tuning
}

// TransportOptions contains optional runtime limits for bounded mobile carriers.
type TransportOptions struct {
	TinyMuxFlowBuffer            int
	TinyMuxFlowSendBuffer        int
	TinyMuxControlBuffer         int
	TinyMuxRateBurstBytes        int
	TinyMuxPingTimeoutMillis     int
	PeerIncomingBuffer           int
	PeerWriteBuffer              int
	SRTPPacketBuffer             int
	KCPWindowSize                int
	KCPReadWriteBuffer           int
	RelayBandwidthBytesPerSecond int
}

var Options GlobalOptions

// RuntimeStats is a bounded, diagnostics-safe snapshot of the active transport.
type RuntimeStats struct {
	Reconnecting        bool
	FullReconnects      int64
	LastReconnectReason string
	Mux                 TinyMuxRuntimeStats
	Peer                PeerRuntimeStats
}

type TinyMuxRuntimeStats struct {
	OpenRequests          int64
	OpenReplies           int64
	OpenCanceled          int64
	OpenErrors            int64
	OpenPending           int64
	PeakOpenPending       int64
	PingSent              int64
	PongReceived          int64
	PingTimeouts          int64
	Disconnects           int64
	ControlReadErrors     int64
	FlowPacketsIn         int64
	FlowBytesIn           int64
	FlowPacketsOut        int64
	FlowBytesOut          int64
	FlowDrops             int64
	ControlFramesOut      int64
	RateWaits             int64
	RateWaitNanos         int64
	RateBurstBytes        int64
	LastOpenLatencyMillis int64
	MaxOpenLatencyMillis  int64
	LastPingRTTMillis     int64
	MaxPingRTTMillis      int64
}

type PeerRuntimeStats struct {
	OnlinePeers        int64
	TotalPeerSlots     int64
	PeerOnlineEvents   int64
	PeerOfflineEvents  int64
	IncomingPackets    int64
	IncomingBytes      int64
	IncomingQueueFull  int64
	OutgoingPackets    int64
	OutgoingBytes      int64
	OutgoingQueueFull  int64
	WriteErrors        int64
	ReconnectAttempts  int64
	ReconnectFailures  int64
	ReconnectSuccesses int64
}

func init() {
	ApplyEnvironmentOptions()
}

// ApplyEnvironmentOptions applies process-level Turnable runtime overrides.
func ApplyEnvironmentOptions() {
	if value := positiveIntEnv("TURNABLE_RELAY_BANDWIDTH_BYTES_PER_SECOND"); value > 0 {
		Options.Transport.RelayBandwidthBytesPerSecond = value
	}
}

// PositiveOr returns value when it is positive, otherwise fallback.
func PositiveOr(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

// EffectiveRelayBandwidth returns the process override or the platform default.
func EffectiveRelayBandwidth(platformDefault float64) float64 {
	if Options.Transport.RelayBandwidthBytesPerSecond > 0 {
		return float64(Options.Transport.RelayBandwidthBytesPerSecond)
	}
	return platformDefault
}

func positiveIntEnv(name string) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0
	}
	return value
}
