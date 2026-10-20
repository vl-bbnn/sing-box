//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type scopedApplicationTestConn struct {
	applicationCloseTestConn
	enabled     atomic.Bool
	enableCalls atomic.Int64
	scopedCalls atomic.Int64
	enable      func() error
}

func (c *scopedApplicationTestConn) EnableScopedApplicationReadRetirement() error {
	c.enableCalls.Add(1)
	if c.enable != nil {
		if e := c.enable(); e != nil {
			return e
		}
	}
	c.enabled.Store(true)
	return nil
}
func (c *scopedApplicationTestConn) CloseApplicationScoped() error {
	c.scopedCalls.Add(1)
	if c.entered != nil {
		close(c.entered)
		<-c.release
	}
	return c.err
}
func (c *scopedApplicationTestConn) Read(p []byte) (int, error) {
	if !c.enabled.Load() {
		return 0, errors.New("application read exposed before activation")
	}
	return c.Conn.Read(p)
}
func scopedApplicationCarrier(t *testing.T, selected bool, transport net.Conn) *Carrier {
	t.Helper()
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.idleTimeout = 0
	c.scopedReadRetirement = selected
	c.dialRoute = func(context.Context, string) (net.Conn, error) { return transport, nil }
	return c
}
func scopedApplicationPipe(t *testing.T) (*scopedApplicationTestConn, net.Conn) {
	t.Helper()
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	return &scopedApplicationTestConn{applicationCloseTestConn: applicationCloseTestConn{Conn: left}}, right
}
func assertScopedAdmissionReleased(t *testing.T, c *Carrier) {
	t.Helper()
	stats := c.Stats()
	if stats.ActiveStreams != 0 || stats.PendingDials != 0 || len(c.activeSlots) != 0 || len(c.openSlots) != 0 || len(c.pendingSlots) != 0 {
		t.Fatal("admission leak or double release", stats)
	}
}
func TestScopedApplicationDefaultKeepsLegacyClose(t *testing.T) {
	transport, _ := scopedApplicationPipe(t)
	raw := errors.New("legacy raw close result")
	transport.err = raw
	c := scopedApplicationCarrier(t, false, transport)
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if e != nil {
		t.Fatal(e)
	}
	if e = stream.Close(); e != raw {
		t.Fatal(e)
	}
	if transport.enableCalls.Load() != 0 || transport.scopedCalls.Load() != 0 || transport.applicationCalls.Load() != 1 || transport.abortCalls.Load() != 0 {
		t.Fatal("default policy changed")
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationActivatesBeforeExposureAndJoinsOneClose(t *testing.T) {
	transport, peer := scopedApplicationPipe(t)
	c := scopedApplicationCarrier(t, true, transport)
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if e != nil {
		t.Fatal(e)
	}
	if !transport.enabled.Load() || transport.enableCalls.Load() != 1 {
		t.Fatal("activation missing before return")
	}
	payload := []byte("actual application bytes after capability admission")
	sent := make(chan error, 1)
	go func() { _, e := peer.Write(payload); sent <- e }()
	got := make([]byte, len(payload))
	if _, e := io.ReadFull(stream, got); e != nil || !bytes.Equal(got, payload) {
		t.Fatal("original data path changed", e)
	}
	if e := <-sent; e != nil {
		t.Fatal(e)
	}
	raw := errors.New("scoped raw retirement result")
	transport.err = raw
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { done <- stream.Close() }()
	}
	for i := 0; i < 8; i++ {
		if e := applicationCloseTestResult(t, done); e != raw {
			t.Fatal("original error identity changed", e)
		}
	}
	if transport.scopedCalls.Load() != 1 || transport.applicationCalls.Load() != 0 || transport.closeCalls.Load() != 0 || transport.abortCalls.Load() != 0 || c.Stats().ClosedStreams != 1 {
		t.Fatal("close owner duplicated or incorrect")
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationUnsupportedRetiresOriginalAndReleasesSlots(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := errors.New("legacy disposal error")
	transport := &applicationCloseLegacyTestConn{Conn: left, err: raw}
	c := scopedApplicationCarrier(t, true, transport)
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if stream != nil || !errors.Is(e, errScopedApplicationUnsupported) || !errors.Is(e, raw) || transport.calls.Load() != 1 {
		t.Fatal("unsupported admission leaked or lost errors", e)
	}
	if c.Stats().OpenedStreams != 0 || c.Stats().FailedStreams != 1 {
		t.Fatal(c.Stats())
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationEnableFailureKeepsRawAndAbortResult(t *testing.T) {
	transport, _ := scopedApplicationPipe(t)
	original, cleanup := errors.New("original capability admission fault"), errors.New("original rejected connection abort fault")
	transport.enable = func() error { return original }
	transport.err = cleanup
	c := scopedApplicationCarrier(t, true, transport)
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if stream != nil || !errors.Is(e, original) || !errors.Is(e, cleanup) || transport.abortCalls.Load() != 1 || transport.scopedCalls.Load() != 0 || transport.applicationCalls.Load() != 0 {
		t.Fatal("raw errors or rejection ownership changed", e)
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationCancellationDuringActivationRejectsExposure(t *testing.T) {
	transport, _ := scopedApplicationPipe(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport.enable = func() error { cancel(); return nil }
	c := scopedApplicationCarrier(t, true, transport)
	stream, e := c.DialStream(ctx, "eu", "example.com:443")
	if stream != nil || !errors.Is(e, context.Canceled) || transport.abortCalls.Load() != 1 {
		t.Fatal("canceled admission exposed", e)
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationActivationKeepsOriginalDialDeadline(t *testing.T) {
	transport, _ := scopedApplicationPipe(t)
	c := scopedApplicationCarrier(t, true, transport)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	c.dialRoute = func(dial context.Context, _ string) (net.Conn, error) {
		actual, _ := dial.Deadline()
		if !actual.Equal(deadline) {
			t.Fatal("dial deadline renewed")
		}
		transport.enable = func() error { <-dial.Done(); return nil }
		return transport, nil
	}
	started := time.Now()
	stream, e := c.DialStream(ctx, "eu", "example.com:443")
	if stream != nil || !errors.Is(e, context.DeadlineExceeded) || transport.abortCalls.Load() != 1 || time.Since(started) > 500*time.Millisecond {
		t.Fatal("original dial budget lost", e)
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationSelectedAbortKeepsImmediateOriginalPath(t *testing.T) {
	transport, _ := scopedApplicationPipe(t)
	raw := errors.New("original abort fault")
	transport.err = raw
	c := scopedApplicationCarrier(t, true, transport)
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if e != nil {
		t.Fatal(e)
	}
	if e := stream.(*carrierConn).Abort(); e != raw {
		t.Fatal(e)
	}
	if transport.abortCalls.Load() != 1 || transport.scopedCalls.Load() != 0 || transport.applicationCalls.Load() != 0 || transport.closeCalls.Load() != 0 {
		t.Fatal("Abort drained application data")
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationConcurrentAbortInterruptsSameCloseOwner(t *testing.T) {
	transport, _ := scopedApplicationPipe(t)
	transport.entered = make(chan struct{})
	transport.release = make(chan struct{})
	defer transport.interruptOnce.Do(func() { close(transport.release) })
	raw := errors.New("original scoped interrupted result")
	transport.err = raw
	c := scopedApplicationCarrier(t, true, transport)
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 2)
	go func() { done <- stream.Close() }()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("scoped close not selected")
	}
	go func() { done <- stream.(*carrierConn).Abort() }()
	for i := 0; i < 2; i++ {
		if e := applicationCloseTestResult(t, done); e != raw {
			t.Fatal(e)
		}
	}
	if transport.scopedCalls.Load() != 1 || transport.applicationCalls.Load() != 0 || transport.abortCalls.Load() != 0 || transport.interruptCalls.Load() != 1 || c.Stats().ClosedStreams != 1 {
		t.Fatal("Close/Abort original owner changed")
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationGenerationClosesDuringActivation(t *testing.T) {
	transport, _ := scopedApplicationPipe(t)
	c := scopedApplicationCarrier(t, true, transport)
	c.done = make(chan struct{})
	transport.enable = func() error { close(c.done); return nil }
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if stream != nil || !errors.Is(e, net.ErrClosed) || transport.scopedCalls.Load() != 1 {
		t.Fatal("generation rejection leaked selected owner", e)
	}
	assertScopedAdmissionReleased(t, c)
}
func TestScopedApplicationRejectsIncompatibleOptionsBeforeConfigOrAuth(t *testing.T) {
	carrier, e := StartCarrier(context.Background(), CarrierOptions{ScopedApplicationReadRetirement: true, Config: "{}"})
	if carrier != nil || e != errScopedApplicationNeedsHalfClose {
		t.Fatal("invalid policy reached config/auth startup", e)
	}
}
