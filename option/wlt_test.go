//go:build with_wlt

package option_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

func TestWLTConfigUnmarshalAcceptsServiceAndOutbound(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "wlt",
				"tag": "wlt-carrier",
				"transport": "wlt",
				"carrier_config": "{}",
				"auth_snapshot_file": "/tmp/wlt-auth.json",
				"auth_snapshot_url": "https://example.com/wlt-auth-snapshot",
				"auth_snapshot_fetch_timeout": "4s",
				"auth_snapshot_output_file": "/tmp/wlt-auth-next.json",
				"connect_timeout": "10s",
				"max_active_streams": 32,
				"max_open_attempts": 16,
				"max_pending_dials": 24,
				"dial_queue_timeout": "1500ms",
				"idle_timeout": "20s",
				"buffer_size": 32768,
				"tiny_mux_flow_buffer": 512,
				"tiny_mux_send_buffer": 128,
				"tiny_mux_control_buffer": 256,
				"mux_rate_burst": 524288,
				"tiny_mux_ping_timeout": "25s",
				"peer_incoming_buffer": 512,
				"peer_write_buffer": 128,
				"redundant_peer_data": true,
				"adaptive_peer_data": true,
				"adaptive_peer_threshold_bytes_per_second": 2097152,
				"adaptive_peer_idle_timeout": "8s",
				"srtp_packet_buffer": 512,
				"kcp_window": 1024,
				"kcp_buffer": 2097152,
				"relay_bandwidth_bytes_per_second": 5242880
			}
		],
		"outbounds": [
			{
				"type": "wlt",
				"tag": "wlt-eu",
				"service": "wlt-carrier",
				"route": "eu",
				"network": "tcp"
			}
		]
	}`), &options)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.Services) != 1 {
		t.Fatalf("services=%d, want 1", len(options.Services))
	}
	serviceOptions, ok := options.Services[0].Options.(*option.WLTServiceOptions)
	if !ok {
		t.Fatalf("service options type=%T, want *option.WLTServiceOptions", options.Services[0].Options)
	}
	if serviceOptions.MaxPendingDials != 24 {
		t.Fatalf("max pending=%d, want 24", serviceOptions.MaxPendingDials)
	}
	if serviceOptions.RelayBandwidthBytesPerSecond != 5242880 {
		t.Fatalf("relay bandwidth=%d, want 5242880", serviceOptions.RelayBandwidthBytesPerSecond)
	}
	if serviceOptions.TinyMuxFlowBuffer != 512 || serviceOptions.KCPReadWriteBuffer != 2097152 {
		t.Fatalf("debug transport options=%+v", serviceOptions)
	}
	if !serviceOptions.RedundantPeerData || !serviceOptions.AdaptivePeerData || serviceOptions.AdaptivePeerThresholdBytes != 2097152 {
		t.Fatalf("adaptive peer options=%+v", serviceOptions)
	}
	if serviceOptions.CarrierConfig != "{}" {
		t.Fatalf("carrier config=%q, want inline config", serviceOptions.CarrierConfig)
	}
	if serviceOptions.AuthSnapshotFile != "/tmp/wlt-auth.json" || serviceOptions.AuthSnapshotOutputFile != "/tmp/wlt-auth-next.json" {
		t.Fatalf("auth snapshot options=%+v", serviceOptions)
	}
	if serviceOptions.AuthSnapshotURL != "https://example.com/wlt-auth-snapshot" || time.Duration(serviceOptions.AuthSnapshotFetchTimeout) != 4*time.Second {
		t.Fatalf("remote auth snapshot options=%+v", serviceOptions)
	}
	if len(options.Outbounds) != 1 {
		t.Fatalf("outbounds=%d, want 1", len(options.Outbounds))
	}
	outboundOptions, ok := options.Outbounds[0].Options.(*option.WLTOutboundOptions)
	if !ok {
		t.Fatalf("outbound options type=%T, want *option.WLTOutboundOptions", options.Outbounds[0].Options)
	}
	if got, want := outboundOptions.BuildNetwork(), []string{"tcp"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("network=%v, want %v", got, want)
	}
}

func TestWLTConfigUnmarshalAcceptsLegacyTurnableFields(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "wlt",
				"tag": "wlt-turnable",
				"transport": "turnable",
				"turnable_config": "{}"
			}
		]
	}`), &options)
	if err != nil {
		t.Fatal(err)
	}
	serviceOptions, ok := options.Services[0].Options.(*option.WLTServiceOptions)
	if !ok {
		t.Fatalf("service options type=%T, want *option.WLTServiceOptions", options.Services[0].Options)
	}
	if serviceOptions.CarrierConfig != "{}" {
		t.Fatalf("carrier config=%q, want legacy turnable config", serviceOptions.CarrierConfig)
	}
}

func TestWLTServiceUnmarshalRejectsNegativePendingDials(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "wlt",
				"tag": "wlt-carrier",
				"transport": "wlt",
				"carrier_config": "{}",
				"max_pending_dials": -1
			}
		]
	}`), &options)
	if err == nil || !strings.Contains(err.Error(), "max_pending_dials") {
		t.Fatalf("err=%v, want negative max_pending_dials rejection", err)
	}
}

func TestWLTServiceUnmarshalRejectsNegativeRelayBandwidth(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "wlt",
				"tag": "wlt-carrier",
				"transport": "wlt",
				"carrier_config": "{}",
				"relay_bandwidth_bytes_per_second": -1
			}
		]
	}`), &options)
	if err == nil || !strings.Contains(err.Error(), "relay_bandwidth_bytes_per_second") {
		t.Fatalf("err=%v, want negative relay_bandwidth_bytes_per_second rejection", err)
	}
}

func TestWLTServiceUnmarshalRejectsNegativeDebugTransportOption(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"services": [
			{
				"type": "wlt",
				"tag": "wlt-carrier",
				"transport": "wlt",
				"carrier_config": "{}",
				"kcp_buffer": -1
			}
		]
	}`), &options)
	if err == nil || !strings.Contains(err.Error(), "kcp_buffer") {
		t.Fatalf("err=%v, want negative kcp_buffer rejection", err)
	}
}

func TestOrdinaryConfigUnmarshalWithoutWLT(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"log": {"level": "info"},
		"outbounds": [
			{"type": "direct", "tag": "direct"}
		],
		"route": {"final": "direct"}
	}`), &options)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.Services) != 0 {
		t.Fatalf("services=%d, want 0", len(options.Services))
	}
}

func TestWLTOutboundUnmarshalRejectsMissingService(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"outbounds": [
			{"type": "wlt", "tag": "wlt-eu", "route": "eu"}
		]
	}`), &options)
	if err == nil || !strings.Contains(err.Error(), "requires service") {
		t.Fatalf("err=%v, want missing service", err)
	}
}

func TestWLTOutboundUnmarshalRejectsPacketNetwork(t *testing.T) {
	ctx := include.Context(context.Background())
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"outbounds": [
			{"type": "wlt", "tag": "wlt-eu", "service": "wlt-carrier", "route": "eu", "network": ["tcp", "udp"]}
		]
	}`), &options)
	if err == nil || !strings.Contains(err.Error(), "tcp network only") {
		t.Fatalf("err=%v, want tcp-only network rejection", err)
	}
}
