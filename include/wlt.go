//go:build with_wlt

package include

import (
	"github.com/sagernet/sing-box/adapter/outbound"
	boxService "github.com/sagernet/sing-box/adapter/service"
	protocolwlt "github.com/sagernet/sing-box/protocol/wlt"
	servicewlt "github.com/sagernet/sing-box/service/wlt"
)

func registerWLTOutbound(registry *outbound.Registry) {
	protocolwlt.RegisterOutbound(registry)
}

func registerWLTService(registry *boxService.Registry) {
	servicewlt.RegisterService(registry)
}
