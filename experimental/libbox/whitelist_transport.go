package libbox

import (
	"context"
	"fmt"
	"strings"
	"time"

	"2b2n.local/whitelist-transport/pkg/wlt"
)

type WhitelistTransportOptions struct {
	Transport           string
	GatewayAddress      string
	Socks               string
	ConnectTimeoutMS    int64
	StartTimeoutMS      int64
	TelemostLink        string
	TelemostDisplayName string
	TelemostVP8FPS      int32
	TelemostVP8Batch    int32
	TelemostPayloadSize int32
	TurnableConfig      string
	TurnableConfigFile  string
	TurnableListeners   string
}

type WhitelistTransportClient struct {
	cancel context.CancelFunc
	client *wlt.Client
}

func StartWhitelistTransport(options *WhitelistTransportOptions) (*WhitelistTransportClient, error) {
	if options == nil {
		return nil, fmt.Errorf("nil whitelist transport options")
	}
	transport := options.Transport
	if transport == "" {
		transport = "telemost"
	}
	socks := options.Socks
	if socks == "" {
		socks = "direct=127.0.0.1:11080,eu=127.0.0.1:11081,dns=127.0.0.1:11082"
	} else if !strings.Contains(socks, "dns=") {
		socks += ",dns=127.0.0.1:11082"
	}
	displayName := options.TelemostDisplayName
	if displayName == "" {
		displayName = "WLT Client"
	}
	connectTimeout := time.Duration(options.ConnectTimeoutMS) * time.Millisecond
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}
	startTimeout := time.Duration(options.StartTimeoutMS) * time.Millisecond
	if startTimeout <= 0 {
		startTimeout = 45 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(startTimeout, cancel)
	client, err := wlt.StartClient(ctx, wlt.ClientOptions{
		Transport:      transport,
		GatewayAddress: options.GatewayAddress,
		SOCKS:          socks,
		ConnectTimeout: connectTimeout,
		Telemost: wlt.TelemostOptions{
			JoinLink:    options.TelemostLink,
			DisplayName: displayName,
			VP8FPS:      int(options.TelemostVP8FPS),
			VP8Batch:    int(options.TelemostVP8Batch),
			PayloadSize: int(options.TelemostPayloadSize),
		},
		Turnable: wlt.TurnableOptions{
			Config:     options.TurnableConfig,
			ConfigFile: options.TurnableConfigFile,
			Listeners:  options.TurnableListeners,
		},
		Logger: func(format string, args ...any) {
			fmt.Printf("wlt: "+format+"\n", args...)
		},
	})
	if !timer.Stop() && err == nil {
		err = context.Canceled
	}
	if err != nil {
		cancel()
		return nil, err
	}
	return &WhitelistTransportClient{cancel: cancel, client: client}, nil
}

func (c *WhitelistTransportClient) Close() error {
	if c == nil {
		return nil
	}
	if c.cancel != nil {
		c.cancel()
	}
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
