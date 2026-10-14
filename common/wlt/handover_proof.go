//go:build with_wlt

package wlt

import "context"

// HandoverProof binds one completed negotiated retention to its exact carrier
// and interface generation. It does not prove fresh traffic by itself.
type HandoverProof interface{ Current() bool }
type HandoverProofProvider interface {
	BeginWltHandoverProof(context.Context, string, int) (HandoverProof, error)
}
