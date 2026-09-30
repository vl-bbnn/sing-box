package oomkiller

import "context"

type wltMemoryPressureKey struct{}

// WithWLTMemoryPressure marks a runtime containing WLT for iOS pressure recovery.
// This is internal runtime state; it does not change the saved profile.
func WithWLTMemoryPressure(ctx context.Context) context.Context {
	return context.WithValue(ctx, wltMemoryPressureKey{}, true)
}

func hasWLTMemoryPressure(ctx context.Context) bool {
	marked, _ := ctx.Value(wltMemoryPressureKey{}).(bool)
	return marked
}
