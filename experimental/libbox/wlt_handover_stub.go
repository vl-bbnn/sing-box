//go:build !with_wlt

package libbox

import "errors"

func (s *CommandServer) CheckWltHandover(requestJSON, name string, index int64) (bool, error) {
	return false, errors.New("WLT handover proof unavailable: rebuild with with_wlt")
}
