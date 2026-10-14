//go:build with_wlt && !wlt_dial_binding

package wlt

import "context"

type DialBinding struct {
	ID          uint64
	EntryUnixNS int64
	EntryRoute  string
}

func BeginDialBinding(ctx context.Context, _ string) (context.Context, DialBinding) {
	return ctx, DialBinding{}
}

func DialBindingFromContext(context.Context) (DialBinding, bool) {
	return DialBinding{}, false
}
