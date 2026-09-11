//go:build with_wlt

package option

import (
	"context"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"
	N "github.com/sagernet/sing/common/network"
)

type WLTServiceOptions struct {
	Transport         string `json:"transport,omitempty"`
	CarrierConfig     string `json:"carrier_config,omitempty"`
	CarrierConfigFile string `json:"carrier_config_file,omitempty"`
	AuthSnapshot      string `json:"auth_snapshot,omitempty"`
	AuthSnapshotFile  string `json:"auth_snapshot_file,omitempty"`
	// AuthSnapshotURL is retained for profile compatibility. It is not used
	// before the WLT carrier has established traffic.
	AuthSnapshotURL          string             `json:"auth_snapshot_url,omitempty"`
	AuthSnapshotFetchTimeout badoption.Duration `json:"auth_snapshot_fetch_timeout,omitempty"`
	// AuthSnapshotOutputFile is updated after successful carrier auth.
	AuthSnapshotOutputFile string `json:"auth_snapshot_output_file,omitempty"`
	// ConfigTrustedAt is retained for profile compatibility. A locally
	// validated WLT profile with durable provider identity is sufficient for
	// client-owned credential renewal and does not expire by this timestamp.
	ConfigTrustedAt int64 `json:"config_trusted_at,omitempty"`
	// Deprecated: use carrier_config.
	TurnableConfig string `json:"turnable_config,omitempty"`
	// Deprecated: use carrier_config_file.
	TurnableConfigFile string             `json:"turnable_config_file,omitempty"`
	ConnectTimeout     badoption.Duration `json:"connect_timeout,omitempty"`
	MaxActiveStreams   int                `json:"max_active_streams,omitempty"`
	MaxOpenAttempts    int                `json:"max_open_attempts,omitempty"`
	DNSOpenReserve     int                `json:"dns_open_reserve,omitempty"`
	MaxPendingDials    int                `json:"max_pending_dials,omitempty"`
	DialQueueTimeout   badoption.Duration `json:"dial_queue_timeout,omitempty"`
	IdleTimeout        badoption.Duration `json:"idle_timeout,omitempty"`
	// PressureIdleTimeout enables reclaiming long-idle streams only when the
	// active-stream limit is saturated. It never changes the normal idle
	// timeout used by media and long-lived connections.
	PressureIdleTimeout          badoption.Duration `json:"pressure_idle_timeout,omitempty"`
	BufferSize                   int                `json:"buffer_size,omitempty"`
	TinyMuxFlowBuffer            int                `json:"tiny_mux_flow_buffer,omitempty"`
	TinyMuxFlowSendBuffer        int                `json:"tiny_mux_send_buffer,omitempty"`
	TinyMuxControlBuffer         int                `json:"tiny_mux_control_buffer,omitempty"`
	TinyMuxRateBurstBytes        int                `json:"mux_rate_burst,omitempty"`
	TinyMuxPingTimeout           badoption.Duration `json:"tiny_mux_ping_timeout,omitempty"`
	PeerIncomingBuffer           int                `json:"peer_incoming_buffer,omitempty"`
	PeerWriteBuffer              int                `json:"peer_write_buffer,omitempty"`
	AdaptivePeerData             bool               `json:"adaptive_peer_data,omitempty"`
	AdaptivePeerThresholdBytes   int                `json:"adaptive_peer_threshold_bytes_per_second,omitempty"`
	AdaptivePeerIdleTimeout      badoption.Duration `json:"adaptive_peer_idle_timeout,omitempty"`
	SRTPPacketBuffer             int                `json:"srtp_packet_buffer,omitempty"`
	KCPWindowSize                int                `json:"kcp_window,omitempty"`
	KCPReadWriteBuffer           int                `json:"kcp_buffer,omitempty"`
	RelayBandwidthBytesPerSecond int                `json:"relay_bandwidth_bytes_per_second,omitempty"`
}

func (o *WLTServiceOptions) UnmarshalJSONContext(_ context.Context, content []byte) error {
	type serviceOptions WLTServiceOptions
	err := json.UnmarshalDisallowUnknownFields(content, (*serviceOptions)(o))
	if err != nil {
		return err
	}
	transport := strings.ToLower(strings.TrimSpace(o.Transport))
	if transport == "" {
		transport = "wlt"
		o.Transport = transport
	}
	if transport != "wlt" && transport != "carrier" && transport != "turnable" {
		return E.New("unsupported wlt service transport: ", o.Transport)
	}
	o.CarrierConfig = strings.TrimSpace(o.CarrierConfig)
	o.CarrierConfigFile = strings.TrimSpace(o.CarrierConfigFile)
	o.AuthSnapshot = strings.TrimSpace(o.AuthSnapshot)
	o.AuthSnapshotFile = strings.TrimSpace(o.AuthSnapshotFile)
	o.AuthSnapshotURL = strings.TrimSpace(o.AuthSnapshotURL)
	o.AuthSnapshotOutputFile = strings.TrimSpace(o.AuthSnapshotOutputFile)
	o.TurnableConfig = strings.TrimSpace(o.TurnableConfig)
	o.TurnableConfigFile = strings.TrimSpace(o.TurnableConfigFile)
	if o.CarrierConfig == "" {
		o.CarrierConfig = o.TurnableConfig
	}
	if o.CarrierConfigFile == "" {
		o.CarrierConfigFile = o.TurnableConfigFile
	}
	if o.CarrierConfig == "" && o.CarrierConfigFile == "" {
		return E.New("wlt service requires carrier_config or carrier_config_file")
	}
	if o.MaxActiveStreams < 0 {
		return E.New("wlt service max_active_streams must be non-negative")
	}
	if o.MaxOpenAttempts < 0 {
		return E.New("wlt service max_open_attempts must be non-negative")
	}
	if o.DNSOpenReserve < 0 {
		return E.New("wlt service dns_open_reserve must be non-negative")
	}
	if o.MaxPendingDials < 0 {
		return E.New("wlt service max_pending_dials must be non-negative")
	}
	if o.BufferSize < 0 {
		return E.New("wlt service buffer_size must be non-negative")
	}
	if o.PressureIdleTimeout < 0 {
		return E.New("wlt service pressure_idle_timeout must be non-negative")
	}
	if o.TinyMuxFlowBuffer < 0 {
		return E.New("wlt service tiny_mux_flow_buffer must be non-negative")
	}
	if o.TinyMuxFlowSendBuffer < 0 {
		return E.New("wlt service tiny_mux_send_buffer must be non-negative")
	}
	if o.TinyMuxControlBuffer < 0 {
		return E.New("wlt service tiny_mux_control_buffer must be non-negative")
	}
	if o.TinyMuxRateBurstBytes < 0 {
		return E.New("wlt service mux_rate_burst must be non-negative")
	}
	if o.PeerIncomingBuffer < 0 {
		return E.New("wlt service peer_incoming_buffer must be non-negative")
	}
	if o.PeerWriteBuffer < 0 {
		return E.New("wlt service peer_write_buffer must be non-negative")
	}
	if o.AdaptivePeerThresholdBytes < 0 {
		return E.New("wlt service adaptive_peer_threshold_bytes_per_second must be non-negative")
	}
	if o.SRTPPacketBuffer < 0 {
		return E.New("wlt service srtp_packet_buffer must be non-negative")
	}
	if o.KCPWindowSize < 0 {
		return E.New("wlt service kcp_window must be non-negative")
	}
	if o.KCPReadWriteBuffer < 0 {
		return E.New("wlt service kcp_buffer must be non-negative")
	}
	if o.RelayBandwidthBytesPerSecond < 0 {
		return E.New("wlt service relay_bandwidth_bytes_per_second must be non-negative")
	}
	return nil
}

type WLTOutboundOptions struct {
	Service string      `json:"service,omitempty"`
	Route   string      `json:"route,omitempty"`
	Network NetworkList `json:"network,omitempty"`
}

func (o *WLTOutboundOptions) UnmarshalJSONContext(_ context.Context, content []byte) error {
	type outboundOptions WLTOutboundOptions
	err := json.UnmarshalDisallowUnknownFields(content, (*outboundOptions)(o))
	if err != nil {
		return err
	}
	o.Service = strings.TrimSpace(o.Service)
	o.Route = strings.TrimSpace(o.Route)
	if o.Service == "" {
		return E.New("wlt outbound requires service")
	}
	if o.Route == "" {
		return E.New("wlt outbound requires route")
	}
	for _, network := range o.BuildNetwork() {
		if network != N.NetworkTCP {
			return E.New("wlt outbound supports tcp network only")
		}
	}
	return nil
}

func (o WLTOutboundOptions) BuildNetwork() []string {
	if o.Network == "" {
		return []string{N.NetworkTCP}
	}
	return o.Network.Build()
}
