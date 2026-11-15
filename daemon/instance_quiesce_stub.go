//go:build !with_wlt

package daemon

import "context"

func quiesceWLTServices(context.Context) error { return nil }

func hasWLTService(context.Context) bool { return false }
