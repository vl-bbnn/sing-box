//go:build !with_wlt

package libbox

import "errors"

func (s *CommandServer) WaitWltTrafficReady(timeoutMillis int64) error {
	return errors.New("wlt support is disabled")
}
