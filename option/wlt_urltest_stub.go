//go:build !with_wlt

package option

// No WLT configuration fields are recognized by an ordinary build.
type WLTURLTestOptions struct{}
