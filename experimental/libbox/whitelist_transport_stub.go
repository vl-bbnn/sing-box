//go:build !with_wlt

package libbox

import "fmt"

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
	CarrierConfig       string
	CarrierConfigFile   string
	CarrierListeners    string
	TurnableConfig      string
	TurnableConfigFile  string
	TurnableListeners   string
}

type WhitelistTransportClient struct{}

func StartWhitelistTransport(options *WhitelistTransportOptions) (*WhitelistTransportClient, error) {
	return nil, fmt.Errorf("WLT support is disabled; rebuild libbox with the with_wlt tag")
}

func (c *WhitelistTransportClient) Close() error {
	return nil
}
