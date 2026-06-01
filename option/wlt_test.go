package option_test

import (
	"context"
	"strings"
	"testing"

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
				"tag": "wlt-turnable",
				"transport": "turnable",
				"turnable_config": "{}",
				"connect_timeout": "10s",
				"max_active_streams": 32,
				"max_open_attempts": 16,
				"idle_timeout": "20s",
				"buffer_size": 32768
			}
		],
		"outbounds": [
			{
				"type": "wlt",
				"tag": "wlt-eu",
				"service": "wlt-turnable",
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
	if _, ok := options.Services[0].Options.(*option.WLTServiceOptions); !ok {
		t.Fatalf("service options type=%T, want *option.WLTServiceOptions", options.Services[0].Options)
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
			{"type": "wlt", "tag": "wlt-eu", "service": "wlt-turnable", "route": "eu", "network": ["tcp", "udp"]}
		]
	}`), &options)
	if err == nil || !strings.Contains(err.Error(), "tcp network only") {
		t.Fatalf("err=%v, want tcp-only network rejection", err)
	}
}
