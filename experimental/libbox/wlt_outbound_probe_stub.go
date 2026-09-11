//go:build !with_wlt

package libbox

import E "github.com/sagernet/sing/common/exceptions"

func (s *CommandServer) ProbeWltOutbound(requestJSON string) (string, error) {
	return "", E.New("WLT outbound probe unavailable: rebuild with with_wlt")
}
