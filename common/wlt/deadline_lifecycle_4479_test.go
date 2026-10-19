//go:build with_wlt && wlt_deadline_fixture

package wlt_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/vl-bbnn/wlt-carrier/pkg/recoveryfixture"
)

type event4479 struct {
	Kind     string
	At       int64
	Deadline int64
	Error    string
	Stack    []string
}
type observed4479 struct {
	net.Conn
	mu     sync.Mutex
	events []event4479
}

func (c *observed4479) record(kind string, d time.Time, e error) {
	pcs := make([]uintptr, 20)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var stack []string
	for {
		f, more := frames.Next()
		stack = append(stack, f.Function)
		if !more {
			break
		}
	}
	row := event4479{Kind: kind, At: time.Now().UnixNano(), Stack: stack}
	if !d.IsZero() {
		row.Deadline = d.UnixNano()
	}
	if e != nil {
		row.Error = e.Error()
	}
	c.mu.Lock()
	c.events = append(c.events, row)
	c.mu.Unlock()
}
func (c *observed4479) SetIdleDeadline(d time.Time) error {
	c.record("SetDeadline", d, nil)
	return c.Conn.(interface{ SetIdleDeadline(time.Time) error }).SetIdleDeadline(d)
}
func (c *observed4479) SetDeadline(d time.Time) error {
	c.record("SetDeadline", d, nil)
	return c.Conn.SetDeadline(d)
}
func (c *observed4479) SetIdleReadDeadline(d time.Time) error {
	c.record("SetReadDeadline", d, nil)
	return c.Conn.(interface{ SetIdleReadDeadline(time.Time) error }).SetIdleReadDeadline(d)
}
func (c *observed4479) SetReadDeadline(d time.Time) error {
	c.record("SetReadDeadline", d, nil)
	return c.Conn.SetReadDeadline(d)
}
func (c *observed4479) SetWriteDeadline(d time.Time) error {
	c.record("SetWriteDeadline", d, nil)
	return c.Conn.SetWriteDeadline(d)
}
func (c *observed4479) Read(p []byte) (int, error) {
	n, e := c.Conn.Read(p)
	c.record("Read", time.Time{}, e)
	return n, e
}
func (c *observed4479) EnableScopedApplicationReadRetirement() error {
	return c.Conn.(interface{ EnableScopedApplicationReadRetirement() error }).EnableScopedApplicationReadRetirement()
}
func (c *observed4479) CloseApplicationScoped() error {
	e := c.Conn.(interface{ CloseApplicationScoped() error }).CloseApplicationScoped()
	c.record("CloseApplicationScoped", time.Time{}, e)
	return e
}
func (c *observed4479) snapshot() []event4479 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]event4479(nil), c.events...)
}
func record4479(t *testing.T, name string, c *observed4479, extra map[string]any) {
	t.Helper()
	root := os.Getenv("WLT_DEADLINE_LIFECYCLE_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("external output required")
	}
	extra["events"] = c.snapshot()
	extra["qualification"] = false
	extra["field_timeout_setter_attributed"] = false
	data, e := json.MarshalIndent(extra, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(filepath.Join(root, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.Write(append(data, '\n')); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
}
func wait4479(t *testing.T, what string, ch <-chan error) error {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal(what + " not joined")
		return nil
	}
}
func prior4479(c net.Conn) error {
	_, _, e := c.(interface{ ScopedApplicationReadOwnershipStats() (int, bool, error) }).ScopedApplicationReadOwnershipStats()
	return e
}

type dial4479 struct{ conn net.Conn }

func (d dial4479) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return d.conn, nil
}
func (d dial4479) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrInvalid
}
func app4479(t *testing.T, idle time.Duration) (net.Conn, net.Conn, net.Conn, *observed4479) {
	t.Helper()
	ck, sk := recoveryfixture.DeadlinePair4479(t)
	o := &observed4479{Conn: ck}
	_, stream := wlt.DeadlineFixtureCarrier4479(t, o, idle)
	cl, e := vless.NewClient("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "", logger.NOP())
	if e != nil {
		t.Fatal(e)
	}
	app, e := cl.DialEarlyConn(stream, M.Socksaddr{Fqdn: "example.com", Port: 443})
	if e != nil {
		t.Fatal(e)
	}
	return app, ck, sk, o
}
func server4479(t *testing.T, sk net.Conn, payload []byte) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, e := vless.ReadRequest(sk)
		if e == nil {
			_, e = sk.Write(append([]byte{0, 0}, payload...))
		}
		if e == nil {
			_, e = io.Copy(io.Discard, sk)
		}
		done <- errors.Join(e, sk.Close())
	}()
	return done
}
func TestDeadlineLifecycle4479CloseReadIsNoop(t *testing.T) {
	app, ck, sk, o := app4479(t, 0)
	if e := N.CloseRead(app); e != nil {
		t.Fatal(e)
	}
	if events := o.snapshot(); len(events) != 0 {
		t.Fatal("CloseRead unexpectedly set a deadline", events)
	}
	done := make(chan error, 1)
	go func() { done <- sk.Close() }()
	if e := app.Close(); e != nil {
		t.Fatal(e)
	}
	if e := wait4479(t, "server close", done); e != nil {
		t.Fatal(e)
	}
	if prior4479(ck) != nil {
		t.Fatal("CloseRead introduced a prior error")
	}
	record4479(t, "close-read-noop", o, map[string]any{"actual_sing_CloseRead_returned_nil": true, "deadline_setter_calls_before_Close": 0, "read_timeout_created": false})
}
func TestDeadlineLifecycle4479ActualManagerEOF(t *testing.T) {
	app, ck, sk, o := app4479(t, 30*time.Second)
	body := []byte("actual server bytes through VLESS and bufio")
	server := server4479(t, sk, body)
	local, source := net.Pipe()
	defer local.Close()
	defer source.Close()
	done := make(chan error, 1)
	manager := route.NewConnectionManager(logger.NOP())
	manager.NewConnection(context.Background(), dial4479{app}, source, adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "example.com", Port: 443}}, func(e error) { done <- e })
	if _, e := local.Write([]byte("application request")); e != nil {
		t.Fatal(e)
	}
	got := make([]byte, len(body))
	if _, e := io.ReadFull(local, got); e != nil || !bytes.Equal(got, body) {
		t.Fatal("original reply", e)
	}
	_ = local.Close()
	_ = wait4479(t, "actual manager copy owners", done)
	if e := wait4479(t, "actual server close", server); e != nil {
		t.Fatal(e)
	}
	if e := prior4479(ck); e != nil {
		t.Fatal("clean EOF introduced prior application error", e)
	}
	var refresh bool
	for _, e := range o.snapshot() {
		if e.Kind == "SetReadDeadline" && !strings.Contains(strings.Join(e.Stack, " "), "wlt.(*carrierConn).refreshScopedApplicationDeadline") {
			t.Fatal("unattributed ReadDeadline setter", e)
		}
		if strings.Contains(strings.Join(e.Stack, " "), "network.CloseRead") {
			t.Fatal("unexpected CloseRead setter", e)
		}
		if e.Kind == "SetDeadline" && strings.Contains(strings.Join(e.Stack, " "), "wlt.(*carrierConn).refreshDeadline") {
			refresh = true
		}
	}
	if !refresh {
		t.Fatal("real carrier deadline setter not observed")
	}
	record4479(t, "actual-manager-eof", o, map[string]any{"actual_route_manager_and_bufio_and_VLESS": true, "reply_exact": true, "prior_application_error_nil": true, "CloseRead_fallback_observed": false, "carrier_refresh_deadline_stack_observed": true})
}
func TestDeadlineLifecycle4479GenuineTimeoutSurvivesIdleActivity(t *testing.T) {
	app, ck, sk, o := app4479(t, 30*time.Second)
	server := server4479(t, sk, []byte("prefix"))
	if _, e := app.Write([]byte("request")); e != nil {
		t.Fatal(e)
	}
	got := make([]byte, 6)
	if _, e := io.ReadFull(app, got); e != nil {
		t.Fatal(e)
	}
	readDone := make(chan error, 1)
	go func() { _, e := app.Read(make([]byte, 1)); readDone <- e }()
	// Wait for the actual managed Read owner before changing its deadline.
	end := time.Now().Add(time.Second)
	for {
		n, _, _ := ck.(interface{ ScopedApplicationReadOwnershipStats() (int, bool, error) }).ScopedApplicationReadOwnershipStats()
		if n == 1 {
			break
		}
		if time.Now().After(end) {
			t.Fatal("ordinary reader not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	originalDeadline := time.Now().Add(100 * time.Millisecond)
	if e := app.SetReadDeadline(originalDeadline); e != nil {
		t.Fatal(e)
	}
	original := wait4479(t, "genuine application deadline", readDone)
	var timeout net.Error
	if !errors.As(original, &timeout) || !timeout.Timeout() || prior4479(ck) != original {
		t.Fatal("original timeout not retained", original)
	}
	// A later real Write refreshes the shared deadline. This must not erase an
	// already-returned Read error even if retirement observes a future deadline.
	if _, e := app.Write([]byte("after-timeout")); e != nil {
		t.Fatal(e)
	}
	closeErr := app.Close()
	if !errors.Is(closeErr, original) {
		t.Fatal("prior timeout suppressed", closeErr, original)
	}
	checkPrior4479(t, ck, originalDeadline, original)
	if e := wait4479(t, "server retirement", server); e != nil {
		t.Fatal(e)
	}
	parts, originalClose, reader, retirement := ck.(interface {
		ApplicationCloseErrorParts() (bool, error, error, error)
	}).ApplicationCloseErrorParts()
	if !parts || originalClose != nil || reader != io.EOF || retirement != nil {
		t.Fatal("not matched four-operand Close shape", parts, originalClose, reader, retirement)
	}
	numeric := map[string]int64{}
	fields := ck.(interface{ DiagnosticFields() []any }).DiagnosticFields()
	for i := 0; i+1 < len(fields); i += 2 {
		if name, ok := fields[i].(string); ok {
			if value, ok := fields[i+1].(int64); ok {
				numeric[name] = value
			}
		}
	}
	if numeric["kcp_application_read_start_deadline_unix_ns"] != originalDeadline.UnixNano() {
		t.Fatal("idle activity revived already expired request", numeric)
	}

	record4479(t, "genuine-timeout-during-idle-activity", o, map[string]any{"original_Read_error": original.Error(), "Close_error": closeErr.Error(), "prior_raw_error_identity_preserved": true, "original_Close_nil": true, "retirement_plain_EOF": true, "retirement_join_nil": true, "subsequent_Write_preserved_original_deadline": true, "original_prior_timeout_preserved_after_idle_activity": true})
}
func TestDeadlineLifecycle4479RequestDeadlinePreserved(t *testing.T) {
	app, ck, sk, o := app4479(t, 30*time.Second)
	server := server4479(t, sk, []byte("prefix"))
	if _, e := app.Write([]byte("request")); e != nil {
		t.Fatal(e)
	}
	got := make([]byte, 6)
	if _, e := io.ReadFull(app, got); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(80 * time.Millisecond)
	if e := app.SetReadDeadline(deadline); e != nil {
		t.Fatal(e)
	}
	start := time.Now()
	_, original := app.Read(make([]byte, 1))
	elapsed := time.Since(start)
	var timeout net.Error
	if !errors.As(original, &timeout) || !timeout.Timeout() || prior4479(ck) != original || elapsed > 500*time.Millisecond {
		t.Fatal("explicit request deadline not preserved", elapsed, original)
	}
	if e := app.Close(); !errors.Is(e, original) {
		t.Fatal("original request timeout not retained in Close", e)
	}
	if e := wait4479(t, "server close", server); e != nil {
		t.Fatal(e)
	}
	record4479(t, "request-deadline-preserved", o, map[string]any{"original_deadline_unix_ns": deadline.UnixNano(), "read_ms": elapsed.Milliseconds(), "original_timeout_identity_preserved": true, "shorter_request_deadline_survives_Read_idle_refresh": true, "whole_field_stability_not_qualified": true})
}

func checkPrior4479(t *testing.T, c net.Conn, deadline time.Time, original error) {
	t.Helper()
	observed, err := c.(interface{ ApplicationPriorReadError() (bool, error) }).ApplicationPriorReadError()
	if !observed || err != original {
		t.Fatal("first raw prior error identity lost")
	}
	fields := c.(interface{ DiagnosticFields() []any }).DiagnosticFields()
	numbers := map[string]int64{}
	for i := 0; i+1 < len(fields); i += 2 {
		if name, ok := fields[i].(string); ok {
			if v, ok := fields[i+1].(int64); ok {
				numbers[name] = v
			}
		}
	}
	for _, name := range []string{"kcp_prior_read_observed", "kcp_prior_read_start_setter_certain", "kcp_prior_read_return_setter_certain"} {
		if numbers[name] != 1 {
			t.Fatal("actual provenance not coherent", name, numbers)
		}
	}
	if numbers["kcp_prior_read_start_setter_origin"] != 3 || numbers["kcp_prior_read_return_setter_origin"] != 2 {
		t.Fatal("idle refresh and actual application setter conflated", numbers)
	}
	if numbers["kcp_prior_read_return_deadline_unix_ns"] != deadline.UnixNano() || numbers["kcp_prior_read_return_sample_unix_ns"] < deadline.UnixNano() {
		t.Fatal("first prior Read snapshot replaced by later future deadline", numbers)
	}
	if numbers["kcp_prior_read_return_setter_sequence"] <= numbers["kcp_prior_read_start_setter_sequence"] {
		t.Fatal("setter sequence not retained", numbers)
	}
	root := os.Getenv("WLT_DEADLINE_LIFECYCLE_ROOT")
	data, e := json.MarshalIndent(numbers, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "first-prior-read-provenance.json"), append(data, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
}

type privatePriorError4479 struct{}

func (privatePriorError4479) Error() string {
	panic("private arbitrary error string must never be formatted")
}
func (privatePriorError4479) Timeout() bool { return true }

type privatePriorReporter4479 struct{}

func (privatePriorReporter4479) ApplicationPriorReadError() (bool, error) {
	return true, privatePriorError4479{}
}
func TestApplicationPriorReadObservationPrivacy4479(t *testing.T) {
	numbers := map[string]int64{"kcp_prior_read_start_setter_origin": 999, "kcp_prior_read_start_setter_certain": 99, "kcp_prior_read_return_sample_unix_ns": -9}
	text := wlt.DeadlineFixturePriorSuffix4479(privatePriorReporter4479{}, numbers)
	for _, s := range []string{"application_prior_read_observed=true", "application_prior_read_timeout=true", "kcp_prior_read_start_setter_origin=0", "kcp_prior_read_start_setter_certain=0", "kcp_prior_read_return_sample_unix_ns=0"} {
		if !strings.Contains(text, s) {
			t.Fatal("bounded observation missing", s)
		}
	}
	if strings.Contains(text, "private") {
		t.Fatal("private error leaked")
	}
}
