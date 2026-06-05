package libbox

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sagernet/sing-box/common/wlt"
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
	client *wlt.TurnableListenerClient
}

func StartWhitelistTransport(options *WhitelistTransportOptions) (*WhitelistTransportClient, error) {
	if options == nil {
		return nil, fmt.Errorf("nil whitelist transport options")
	}
	transport := options.Transport
	if transport == "" {
		transport = "telemost"
	}
	startTimeout := time.Duration(options.StartTimeoutMS) * time.Millisecond
	if startTimeout <= 0 {
		startTimeout = 45 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(startTimeout, cancel)
	var client *wlt.TurnableListenerClient
	var err error
	if strings.EqualFold(transport, "turnable") {
		client, err = wlt.StartTurnableListenerClient(ctx, wlt.TurnableListenerClientOptions{
			Config:     options.TurnableConfig,
			ConfigFile: options.TurnableConfigFile,
			Listeners:  options.TurnableListeners,
			Logger: func(format string, args ...any) {
				fmt.Printf("wlt: "+format+"\n", args...)
			},
		})
	} else {
		err = fmt.Errorf("legacy whitelist transport %q is not supported by the embedded core WLT build", transport)
	}
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
