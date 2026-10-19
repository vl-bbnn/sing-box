//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"net"
)

var errScopedApplicationNeedsHalfClose = errors.New("scoped application read retirement requires data_half_close")
var errScopedApplicationUnsupported = errors.New("WLT stream does not support scoped application read retirement")

type scopedApplicationReadCapability interface {
	EnableScopedApplicationReadRetirement() error
	CloseApplicationScoped() error
}

func validateScopedApplicationReadOptions(options CarrierOptions) error {
	if options.ScopedApplicationReadRetirement && !options.DataHalfClose {
		return errScopedApplicationNeedsHalfClose
	}
	return nil
}

// Selection is explicit and occurs before exposing the stream or starting an
// application read. The original dial context remains authoritative before
// and after activation; no additional timeout or retry budget is created.
func prepareScopedApplicationRead(ctx context.Context, conn net.Conn, selected bool) (bool, error) {
	if !selected {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	capability, ok := conn.(scopedApplicationReadCapability)
	if !ok {
		return false, errScopedApplicationUnsupported
	}
	if err := capability.EnableScopedApplicationReadRetirement(); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

// Rejection retains ownership of the actual original connection and disposes
// it before releasing admission. Abort keeps its original immediate path.
func disposeScopedApplicationAdmission(conn net.Conn) error {
	if aborter, ok := conn.(interface{ Abort() error }); ok {
		return aborter.Abort()
	}
	return conn.Close()
}
func closeSelectedApplicationStream(conn net.Conn, selected bool) error {
	if !selected {
		return closeApplicationStream(conn)
	}
	capability, ok := conn.(scopedApplicationReadCapability)
	if !ok {
		return errScopedApplicationUnsupported
	}
	return capability.CloseApplicationScoped()
}
