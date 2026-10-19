//go:build with_wlt

package urltest

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

type bodyTestDialer struct {
	address    string
	calls      atomic.Int32
	closeError error
	closeDelay time.Duration
}

func (d *bodyTestDialer) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	d.calls.Add(1)
	c, err := (&net.Dialer{}).DialContext(ctx, network, d.address)
	if err != nil {
		return nil, err
	}
	return &bodyTestConn{Conn: c, err: d.closeError, delay: d.closeDelay}, nil
}
func (*bodyTestDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("not used")
}

type bodyTestConn struct {
	net.Conn
	err   error
	delay time.Duration
}

func (c *bodyTestConn) Close() error { time.Sleep(c.delay); return errors.Join(c.Conn.Close(), c.err) }

type bodyTestStore struct {
	adapter.CertificateStore
	pool *x509.CertPool
}

func (s *bodyTestStore) Pool() *x509.CertPool { return s.pool }
func bodyTLSContext(server *httptest.Server) context.Context {
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	return service.ContextWith[adapter.CertificateStore](context.Background(), &bodyTestStore{pool: pool})
}

func TestBodyURLTestPreservesHEADAndRejectsFalseHealth(t *testing.T) {
	for _, name := range []string{"full", "chunked-full", "truncated", "stall", "extra", "status", "redirect", "reset"} {
		t.Run(name, func(t *testing.T) {
			var heads, gets atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					heads.Add(1)
					w.WriteHeader(200)
					return
				}
				gets.Add(1)
				switch name {
				case "full":
					w.Header().Set("Content-Length", "4096")
					fmt.Fprint(w, strings.Repeat("b", 4096))
				case "chunked-full":
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					fmt.Fprint(w, strings.Repeat("b", 4096))
				case "truncated":
					w.Header().Set("Content-Length", "4096")
					fmt.Fprint(w, strings.Repeat("b", 2048))
				case "stall":
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				case "extra":
					fmt.Fprint(w, strings.Repeat("b", 4097))
				case "status":
					w.WriteHeader(503)
				case "redirect":
					w.Header().Set("Location", "/full")
					w.WriteHeader(302)
				case "reset":
					c, _, _ := w.(http.Hijacker).Hijack()
					c.Close()
				}
			}))
			defer server.Close()
			dial := &bodyTestDialer{address: server.Listener.Addr().String()}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			if _, err := URLTest(ctx, server.URL, dial); err != nil {
				t.Fatal("legacy HEAD changed", err)
			}
			_, err := BodyURLTest(ctx, server.URL, dial, 4096)
			want := name == "full" || name == "chunked-full"
			if (err == nil) != want {
				t.Fatalf("complete=%v err=%v", want, err)
			}
			if heads.Load() != 1 || gets.Load() != 1 || dial.calls.Load() != 2 {
				t.Fatalf("request replay or lost original HEAD: HEAD=%d GET=%d dials=%d", heads.Load(), gets.Load(), dial.calls.Load())
			}
		})
	}
}

func TestBodyURLTestStrictTLSAndOriginalCloseError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") }))
	defer server.Close()
	address := server.Listener.Addr().String()
	if _, err := BodyURLTest(context.Background(), server.URL, &bodyTestDialer{address: address}, 4); err == nil {
		t.Fatal("untrusted TLS accepted")
	}
	ctx := bodyTLSContext(server)
	if _, err := BodyURLTest(ctx, server.URL, &bodyTestDialer{address: address}, 4); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("original-underlying-close-failed")
	if _, err := BodyURLTest(ctx, server.URL, &bodyTestDialer{address: address, closeError: sentinel}, 4); !errors.Is(err, sentinel) {
		t.Fatalf("original Close lost: %v", err)
	}
}

func TestBodyURLTestCancellationAndCloseInTotalDeadline(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := BodyURLTest(ctx, server.URL, &bodyTestDialer{address: server.Listener.Addr().String()}, 4)
		done <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled body reader remains live")
	}
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") }))
	defer fast.Close()
	short, cancelShort := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelShort()
	_, err := BodyURLTest(short, fast.URL, &bodyTestDialer{address: fast.Listener.Addr().String(), closeDelay: 60 * time.Millisecond}, 4)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late Close was reported healthy: %v", err)
	}
}

func TestBodyURLTestBoundedPayloadAndBadOptions(t *testing.T) {
	var actual atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { actual.Add(1); fmt.Fprint(w, strings.Repeat("x", 65536)) }))
	defer server.Close()
	d := &bodyTestDialer{address: server.Listener.Addr().String()}
	if _, err := BodyURLTest(context.Background(), server.URL, d, 65536); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{-1, 0, 65537} {
		if _, err := BodyURLTest(context.Background(), server.URL, d, size); err == nil {
			t.Fatalf("size %d accepted", size)
		}
	}
	for _, url := range []string{"", "://invalid", "file:///etc/hosts", "http://name:secret@example.com/", "http://example.com/#fragment"} {
		if _, err := BodyURLTest(context.Background(), url, d, 4); err == nil {
			t.Fatal("invalid URL accepted")
		}
	}
	if actual.Load() != 1 || d.calls.Load() != 1 {
		t.Fatal("invalid options reached network")
	}
}
