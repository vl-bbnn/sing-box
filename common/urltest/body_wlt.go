//go:build with_wlt

package urltest

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
)

// bodyProbeConn retains the actual underlying Close result, including when
// net/http closed the connection first. It never waives a transport failure.
type bodyProbeConn struct {
	net.Conn
	once sync.Once
	err  error
}

func (c *bodyProbeConn) Close() error { c.once.Do(func() { c.err = c.Conn.Close() }); return c.err }

// BodyURLTest reads exactly the configured small response and its EOF. The
// original 15s budget starts before dialing and includes body/connection Close.
// No request replay, redirects, relaxed TLS, or legacy HEAD history is used.
func BodyURLTest(parent context.Context, link string, detour N.Dialer, size int) (delay uint16, err error) {
	if size < 1 || size > 65536 {
		return 0, fmt.Errorf("body probe size outside 1..65536")
	}
	u, err := url.Parse(link)
	if err != nil {
		return 0, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return 0, fmt.Errorf("invalid body probe URL")
	}
	ctx, cancel := context.WithTimeout(parent, C.TCPTimeout)
	defer cancel()
	started := time.Now()
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	raw, err := detour.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(u.Hostname(), port))
	if err != nil {
		return 0, err
	}
	conn := &bodyProbeConn{Conn: raw}
	defer func() {
		err = errors.Join(err, conn.Close(), ctx.Err())
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			err = errors.Join(err, context.DeadlineExceeded)
		}
		if err != nil {
			delay = 0
		} else {
			delay = uint16(time.Since(started) / time.Millisecond)
		}
	}()
	deadline, _ := ctx.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		return 0, err
	}
	var taken atomic.Bool
	transport := &http.Transport{
		Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 16384,
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			if !taken.CompareAndSwap(false, true) {
				return nil, errors.New("body probe refuses a repeated HTTP dial")
			}
			return conn, nil
		},
		TLSClientConfig: &tls.Config{Time: ntp.TimeFuncFromContext(ctx), RootCAs: adapter.RootPoolFromContext(ctx)},
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: C.TCPTimeout}
	defer client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("body probe HTTP status %d", resp.StatusCode)
	}
	// A bounded extra byte distinguishes a complete small payload from a larger
	// response; chunked bodies must also reach EOF within the same total budget.
	n, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, int64(size)+1))
	if readErr != nil {
		return 0, readErr
	}
	if n != int64(size) {
		return 0, fmt.Errorf("body probe response size %d, expected %d", n, size)
	}
	return 0, nil
}
