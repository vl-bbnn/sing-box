//go:build with_wlt

package libbox

import (
	"context"
	"errors"

	commonwlt "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing/service"
)

// WaitWltTrafficReady waits for end-to-end traffic on the active WLT carrier.
func (s *CommandServer) WaitWltTrafficReady(timeoutMillis int64) error {
	if timeoutMillis <= 0 {
		return context.DeadlineExceeded
	}
	instance := s.Instance()
	if instance == nil {
		return errors.New("service is not started")
	}
	provider := service.FromContext[commonwlt.TrafficReadyProvider](instance.Context())
	if provider == nil {
		return errors.New("wlt service unavailable")
	}
	return normalizeWltTrafficReadyError(provider.WaitWltTrafficReady(context.Background(), timeoutMillis))
}

// normalizeWltTrafficReadyError keeps the bounded polling contract stable
// across the Go/libbox bridge. Android uses this sentinel to continue its
// one-second slices; service-stop and transport errors remain distinct.
func normalizeWltTrafficReadyError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("wlt traffic readiness timed out")
	}
	return err
}
