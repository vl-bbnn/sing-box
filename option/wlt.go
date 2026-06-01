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
	Transport          string             `json:"transport,omitempty"`
	TurnableConfig     string             `json:"turnable_config,omitempty"`
	TurnableConfigFile string             `json:"turnable_config_file,omitempty"`
	ConnectTimeout     badoption.Duration `json:"connect_timeout,omitempty"`
	MaxActiveStreams   int                `json:"max_active_streams,omitempty"`
	MaxOpenAttempts    int                `json:"max_open_attempts,omitempty"`
	IdleTimeout        badoption.Duration `json:"idle_timeout,omitempty"`
	BufferSize         int                `json:"buffer_size,omitempty"`
}

func (o *WLTServiceOptions) UnmarshalJSONContext(_ context.Context, content []byte) error {
	type serviceOptions WLTServiceOptions
	err := json.UnmarshalDisallowUnknownFields(content, (*serviceOptions)(o))
	if err != nil {
		return err
	}
	transport := strings.ToLower(strings.TrimSpace(o.Transport))
	if transport == "" {
		transport = "turnable"
		o.Transport = transport
	}
	if transport != "turnable" {
		return E.New("unsupported wlt service transport: ", o.Transport)
	}
	if strings.TrimSpace(o.TurnableConfig) == "" && strings.TrimSpace(o.TurnableConfigFile) == "" {
		return E.New("wlt service requires turnable_config or turnable_config_file")
	}
	if o.MaxActiveStreams < 0 {
		return E.New("wlt service max_active_streams must be non-negative")
	}
	if o.MaxOpenAttempts < 0 {
		return E.New("wlt service max_open_attempts must be non-negative")
	}
	if o.BufferSize < 0 {
		return E.New("wlt service buffer_size must be non-negative")
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
