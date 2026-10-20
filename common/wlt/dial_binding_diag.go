//go:build with_wlt && wlt_dial_binding

package wlt

import (
	"context"
	"math"
	"sync/atomic"
	"time"
)

// DialBinding identifies a local WLT outbound invocation, not a transport flow.
// Flow IDs arrive only after the mux OPEN reply.
type DialBinding struct {
	ID          uint64
	EntryUnixNS int64
	EntryRoute  string
}

type dialBindingKey struct{}

var nextDialBindingID atomic.Uint64

// BeginDialBinding does not change cancellation or deadline semantics. The
// diagnostic counter saturates rather than reusing an ID after uint64 wrap.
func BeginDialBinding(ctx context.Context, route string) (context.Context, DialBinding) {
	for {
		current := nextDialBindingID.Load()
		if current == math.MaxUint64 {
			return ctx, DialBinding{}
		}
		if nextDialBindingID.CompareAndSwap(current, current+1) {
			binding := DialBinding{ID: current + 1, EntryUnixNS: time.Now().UnixNano(), EntryRoute: route}
			return context.WithValue(ctx, dialBindingKey{}, binding), binding
		}
	}
}

func DialBindingFromContext(ctx context.Context) (DialBinding, bool) {
	if ctx == nil {
		return DialBinding{}, false
	}
	binding, ok := ctx.Value(dialBindingKey{}).(DialBinding)
	return binding, ok && binding.ID != 0
}
