//go:build with_wlt

package wlt

import (
	"context"
	"crypto/sha256"
	"fmt"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"syscall"
)

// Diagnostic only: never call arbitrary Error/Is methods or emit error text,
// destinations, operation names or provider payload. Keep parent links for
// joined errors. Cycles, depth and width are bounded independently of retry.
func boundedDialErrorTree(original error) (string, int, bool) {
	type entry struct {
		err           error
		parent, depth int
	}
	queue := []entry{{err: original}}
	rows := make([]string, 0, 16)
	truncated := false
	for len(queue) > 0 && len(rows) < 16 {
		item := queue[0]
		queue = queue[1:]
		id := len(rows)
		kind := "leaf"
		errno := uint64(0)
		typeName := "nil"
		var children []error
		if item.err == nil {
			kind = "nil"
		} else {
			typeName = reflect.TypeOf(item.err).String()
			switch e := item.err.(type) {
			case *net.OpError:
				kind = "net_op"
				children = []error{e.Err}
			case *os.SyscallError:
				kind = "syscall_op"
				children = []error{e.Err}
			case syscall.Errno:
				kind = "errno"
				errno = uint64(e)
				if errno > 4294967295 {
					errno = 0
					kind = "errno_other"
				}
			case *localDialGenerationRetired:
				kind = "local_retired"
				children = []error{e.Unwrap()}
			default:
				switch item.err {
				case context.Canceled:
					kind = "canceled"
				case context.DeadlineExceeded:
					kind = "deadline"
				case io.EOF:
					kind = "eof"
				case io.ErrUnexpectedEOF:
					kind = "unexpected_eof"
				case io.ErrClosedPipe:
					kind = "closed_pipe"
				case net.ErrClosed:
					kind = "net_closed"
				case carriercommon.ErrOpenServiceStopped:
					kind = "service_stopped"
				default:
					if multi, ok := item.err.(interface{ Unwrap() []error }); ok {
						kind = "join"
						children = multi.Unwrap()
					} else if single, ok := item.err.(interface{ Unwrap() error }); ok {
						kind = "wrap"
						children = []error{single.Unwrap()}
					} else if reflect.TypeOf(item.err) == reflect.TypeOf(io.EOF) {
						// Only the standard library's plain errorString can reach this exact
						// finite vocabulary. Its contents are never copied into the output.
						switch item.err.Error() {
						case "mux: closed":
							kind = "mux_closed"
						case "tinymux client session closed":
							kind = "session_closed"
						case "tinymux pong timeout":
							kind = "pong_timeout"
						case "tinymux remote disconnect":
							kind = "remote_disconnect"
						}
					}
				}
			}
		}
		digest := sha256.Sum256([]byte(typeName))
		rows = append(rows, fmt.Sprintf("%d.%d.%s.%d.%x", id, item.parent, kind, errno, digest[:8]))
		for _, child := range children {
			if child == nil {
				continue
			}
			if item.depth >= 7 || len(rows)+len(queue) >= 16 {
				truncated = true
				break
			}
			queue = append(queue, entry{child, id, item.depth + 1})
		}
	}
	return strings.Join(rows, ","), len(rows), truncated || len(queue) > 0
}
