//go:build !with_wlt

package option

import (
	"context"

	E "github.com/sagernet/sing/common/exceptions"
)

type WLTServiceOptions struct{}

func (o *WLTServiceOptions) UnmarshalJSONContext(_ context.Context, _ []byte) error {
	return E.New("WLT service support is disabled; rebuild with the with_wlt tag")
}

type WLTOutboundOptions struct{}

func (o *WLTOutboundOptions) UnmarshalJSONContext(_ context.Context, _ []byte) error {
	return E.New("WLT outbound support is disabled; rebuild with the with_wlt tag")
}
