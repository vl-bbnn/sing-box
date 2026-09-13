//go:build with_wlt

package libbox

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	dnsTransport "github.com/sagernet/sing-box/dns/transport"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"

	mDNS "github.com/miekg/dns"
)

type wltProbeFakeResolver map[string]adapter.Outbound

func (r wltProbeFakeResolver) Outbound(tag string) (adapter.Outbound, bool) {
	outbound, loaded := r[tag]
	return outbound, loaded
}

type wltProbeFakeOutbound struct {
	typ          string
	tag          string
	network      []string
	dependencies []string
	dial         func(context.Context, string, M.Socksaddr) (net.Conn, error)
	dialCount    atomic.Int32
	destination  chan string
}

func (o *wltProbeFakeOutbound) Type() string           { return o.typ }
func (o *wltProbeFakeOutbound) Tag() string            { return o.tag }
func (o *wltProbeFakeOutbound) Network() []string      { return o.network }
func (o *wltProbeFakeOutbound) Dependencies() []string { return o.dependencies }
func (o *wltProbeFakeOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	o.dialCount.Add(1)
	if o.destination != nil {
		select {
		case o.destination <- destination.String():
		default:
		}
	}
	if o.dial == nil {
		return nil, errors.New("trap dial")
	}
	return o.dial(ctx, network, destination)
}
func (o *wltProbeFakeOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unexpected packet listen")
}

type wltProbeFakeGroup struct {
	*wltProbeFakeOutbound
	now string
	all []string
}

func (g *wltProbeFakeGroup) Now() string   { return g.now }
func (g *wltProbeFakeGroup) All() []string { return g.all }

type wltProbeCertificateStore struct{ pool *x509.CertPool }

func (*wltProbeCertificateStore) Name() string                   { return "wlt-probe-test-roots" }
func (*wltProbeCertificateStore) Start(adapter.StartStage) error { return nil }
func (*wltProbeCertificateStore) Close() error                   { return nil }
func (s *wltProbeCertificateStore) Pool() *x509.CertPool         { return s.pool }

func wltHTTPSRequest() wltProbeRequest {
	return wltProbeRequest{
		Schema: 1, ProbeID: "probe-1", Kind: "https", GroupTag: "group",
		OutboundTag: "leaf", WLTTag: "wlt", TimeoutMS: 2_000,
		URL: "https://cp.cloudflare.com/generate_204", ExpectedStatus: 204,
		ExpectedBytes: 0, MaxReadBytes: 1,
	}
}

func wltDNSRequest(server string) wltProbeRequest {
	return wltProbeRequest{
		Schema: 1, ProbeID: "probe-1", Kind: "dns", GroupTag: "group",
		OutboundTag: "leaf", WLTTag: "wlt", TimeoutMS: 2_000,
		Server: server, QueryName: "Fresh.Example.COM.",
	}
}

func wltProbeGraph(dial func(context.Context, string, M.Socksaddr) (net.Conn, error)) (wltProbeFakeResolver, *wltProbeFakeGroup, *wltProbeFakeOutbound, *wltProbeFakeOutbound) {
	groupBase := &wltProbeFakeOutbound{typ: C.TypeURLTest, tag: "group", network: []string{"tcp"}}
	group := &wltProbeFakeGroup{wltProbeFakeOutbound: groupBase, now: "ordinary", all: []string{"ordinary", "leaf"}}
	leaf := &wltProbeFakeOutbound{
		typ: C.TypeVLESS, tag: "leaf", network: []string{"tcp"}, dependencies: []string{"wlt"}, dial: dial,
	}
	wlt := &wltProbeFakeOutbound{typ: C.TypeWLT, tag: "wlt", network: []string{"tcp"}}
	return wltProbeFakeResolver{"group": group, "leaf": leaf, "wlt": wlt}, group, leaf, wlt
}

func executeWLTProbeForTest(t *testing.T, ctx context.Context, key any, resolver wltProbeFakeResolver, request wltProbeRequest) wltProbeResult {
	t.Helper()
	encoded, err := executeWLTProbe(key, ctx, resolver, func() bool { return true }, request, sha256Hex([]byte("request")))
	if err != nil {
		t.Fatalf("execute probe: %v", err)
	}
	var result wltProbeResult
	if err = json.Unmarshal([]byte(encoded), &result); err != nil {
		t.Fatalf("decode probe result: %v", err)
	}
	return result
}

func TestParseWLTProbeRequestRejectsMalformedContracts(t *testing.T) {
	validHTTPS := `{"schema":1,"probe_id":"probe-1","kind":"https","group_tag":"group","outbound_tag":"leaf","wlt_tag":"wlt","timeout_ms":1000,"url":"https://cp.cloudflare.com/generate_204","expected_status":204,"expected_bytes":0,"max_read_bytes":1}`
	validDNS := `{"schema":1,"probe_id":"probe-1","kind":"dns","group_tag":"group","outbound_tag":"leaf","wlt_tag":"wlt","timeout_ms":1000,"server":"192.0.2.1:53","query_name":"fresh.example.com"}`
	cases := map[string]string{
		"invalid JSON":            `{`,
		"extra field":             strings.TrimSuffix(validHTTPS, "}") + `,"extra":true}`,
		"duplicate field":         strings.Replace(validHTTPS, `"schema":1`, `"schema":1,"schema":1`, 1),
		"missing field":           strings.Replace(validHTTPS, `,"max_read_bytes":1`, "", 1),
		"bool timeout":            strings.Replace(validHTTPS, `"timeout_ms":1000`, `"timeout_ms":true`, 1),
		"float timeout":           strings.Replace(validHTTPS, `"timeout_ms":1000`, `"timeout_ms":1.5`, 1),
		"bad probe id":            strings.Replace(validHTTPS, `"probe_id":"probe-1"`, `"probe_id":"bad id"`, 1),
		"bad tag":                 strings.Replace(validHTTPS, `"group_tag":"group"`, `"group_tag":"bad/tag"`, 1),
		"bad URL":                 strings.Replace(validHTTPS, `https://cp.cloudflare.com/generate_204`, `http://cp.cloudflare.com/generate_204`, 1),
		"credentialed URL":        strings.Replace(validHTTPS, `https://cp.cloudflare.com`, `https://user@cp.cloudflare.com`, 1),
		"nonallowlisted URL":      strings.Replace(validHTTPS, `cp.cloudflare.com`, `example.com`, 1),
		"altered status":          strings.Replace(validHTTPS, `"expected_status":204`, `"expected_status":200`, 1),
		"altered size":            strings.Replace(validHTTPS, `"expected_bytes":0`, `"expected_bytes":1`, 1),
		"altered read cap":        strings.Replace(validHTTPS, `"max_read_bytes":1`, `"max_read_bytes":2`, 1),
		"malformed DNS endpoint":  strings.Replace(validDNS, `192.0.2.1:53`, `192.0.2.1`, 1),
		"nonliteral DNS endpoint": strings.Replace(validDNS, `192.0.2.1:53`, `dns.example.com:53`, 1),
		"zero DNS port":           strings.Replace(validDNS, `192.0.2.1:53`, `192.0.2.1:0`, 1),
		"invalid hostname":        strings.Replace(validDNS, `fresh.example.com`, `bad_name.example.com`, 1),
		"wrong kind fields":       strings.Replace(validHTTPS, `"kind":"https"`, `"kind":"dns"`, 1),
		"oversized request":       strings.Repeat("x", wltProbeMaxInputBytes+1),
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := parseWLTProbeRequest(input); err == nil {
				t.Fatal("expected request rejection")
			}
		})
	}
	for name, input := range map[string]string{"https": validHTTPS, "dns": validDNS} {
		t.Run("valid "+name, func(t *testing.T) {
			request, hash, err := parseWLTProbeRequest(input)
			if err != nil {
				t.Fatalf("unexpected rejection: %v", err)
			}
			if request.Kind != name || hash != sha256Hex([]byte(input)) {
				t.Fatalf("unexpected parse result: kind=%q hash=%q", request.Kind, hash)
			}
		})
	}
	bulk := `{"schema":1,"probe_id":"probe-1","kind":"https","group_tag":"group","outbound_tag":"leaf","wlt_tag":"wlt","timeout_ms":90000,"url":"https://speed.cloudflare.com/__down?bytes=1048576&seed=seed-1","expected_status":200,"expected_bytes":1048576,"max_read_bytes":1048577}`
	if _, _, err := parseWLTProbeRequest(bulk); err != nil {
		t.Fatalf("valid bulk contract rejected: %v", err)
	}
}

func TestExecuteWLTProbeRejectsInvalidGraphBeforeDial(t *testing.T) {
	request := wltHTTPSRequest()
	newGraph := func() (wltProbeFakeResolver, *wltProbeFakeGroup, *wltProbeFakeOutbound, *wltProbeFakeOutbound) {
		return wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			t.Fatal("invalid graph reached dial")
			return nil, nil
		})
	}
	tests := map[string]func(wltProbeFakeResolver, *wltProbeFakeGroup, *wltProbeFakeOutbound, *wltProbeFakeOutbound){
		"missing group": func(r wltProbeFakeResolver, _ *wltProbeFakeGroup, _ *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			delete(r, "group")
		},
		"not a group": func(r wltProbeFakeResolver, _ *wltProbeFakeGroup, _ *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			r["group"] = &wltProbeFakeOutbound{typ: "selector", tag: "group"}
		},
		"leaf absent": func(r wltProbeFakeResolver, _ *wltProbeFakeGroup, _ *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			delete(r, "leaf")
		},
		"leaf not child": func(_ wltProbeFakeResolver, g *wltProbeFakeGroup, _ *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			g.all = []string{"ordinary"}
		},
		"ordinary leaf": func(_ wltProbeFakeResolver, _ *wltProbeFakeGroup, l *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			l.typ = "direct"
		},
		"no dependency": func(_ wltProbeFakeResolver, _ *wltProbeFakeGroup, l *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			l.dependencies = nil
		},
		"multiple dependencies": func(_ wltProbeFakeResolver, _ *wltProbeFakeGroup, l *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			l.dependencies = []string{"wlt", "other"}
		},
		"wrong dependency": func(_ wltProbeFakeResolver, _ *wltProbeFakeGroup, l *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			l.dependencies = []string{"other"}
		},
		"dependency absent": func(r wltProbeFakeResolver, _ *wltProbeFakeGroup, _ *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			delete(r, "wlt")
		},
		"dependency not WLT": func(_ wltProbeFakeResolver, _ *wltProbeFakeGroup, _ *wltProbeFakeOutbound, w *wltProbeFakeOutbound) {
			w.typ = "direct"
		},
		"no TCP": func(_ wltProbeFakeResolver, _ *wltProbeFakeGroup, l *wltProbeFakeOutbound, _ *wltProbeFakeOutbound) {
			l.network = []string{"udp"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			resolver, group, leaf, wlt := newGraph()
			mutate(resolver, group, leaf, wlt)
			if _, err := executeWLTProbe(new(int), context.Background(), resolver, func() bool { return true }, request, "hash"); err == nil {
				t.Fatal("expected graph rejection")
			}
			if leaf.dialCount.Load() != 0 || group.dialCount.Load() != 0 || wlt.dialCount.Load() != 0 {
				t.Fatal("invalid graph performed network traffic")
			}
		})
	}
	t.Run("nested leaf", func(t *testing.T) {
		resolver, group, leaf, _ := newGraph()
		resolver["leaf"] = &wltProbeFakeGroup{wltProbeFakeOutbound: leaf, all: []string{"x"}}
		if _, err := executeWLTProbe(new(int), context.Background(), resolver, func() bool { return true }, request, "hash"); err == nil {
			t.Fatal("expected nested leaf rejection")
		}
		if group.dialCount.Load() != 0 || leaf.dialCount.Load() != 0 {
			t.Fatal("nested leaf rejection dialed")
		}
	})
	t.Run("nested WLT", func(t *testing.T) {
		resolver, _, leaf, wlt := newGraph()
		resolver["wlt"] = &wltProbeFakeGroup{wltProbeFakeOutbound: wlt, all: []string{"x"}}
		if _, err := executeWLTProbe(new(int), context.Background(), resolver, func() bool { return true }, request, "hash"); err == nil {
			t.Fatal("expected nested WLT rejection")
		}
		if leaf.dialCount.Load() != 0 || wlt.dialCount.Load() != 0 {
			t.Fatal("nested WLT rejection dialed")
		}
	})
}

func TestExecuteWLTProbeAcceptsSelectorGroupWithoutDialingIt(t *testing.T) {
	resolver, group, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		return nil, errors.New("leaf-only expected failure")
	})
	group.typ = "selector"
	result := executeWLTProbeForTest(t, context.Background(), new(int), resolver, wltHTTPSRequest())
	if result.ErrorCode != "https_request_failed" || group.dialCount.Load() != 0 {
		t.Fatalf("selector group was rejected or dialed: %+v group_dials=%d", result, group.dialCount.Load())
	}
}

func newWLTProbeTLSContext(t *testing.T, handler http.Handler) (context.Context, string, func()) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "WLT probe test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"cp.cloudflare.com", "speed.cloudflare.com"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tlsCertificate(t, der, key)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	pool := x509.NewCertPool()
	certificateLeaf, err := x509.ParseCertificate(der)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	pool.AddCert(certificateLeaf)
	store := &wltProbeCertificateStore{pool: pool}
	ctx := service.ContextWith[adapter.CertificateStore](context.Background(), store)
	return ctx, server.Listener.Addr().String(), server.Close
}

func tlsCertificate(t *testing.T, der []byte, key *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestWLTHTTPSProbeSuccessIsolationAndSchema(t *testing.T) {
	tests := []struct {
		name    string
		request wltProbeRequest
		body    []byte
	}{
		{name: "204", request: wltHTTPSRequest()},
		{name: "one MiB", request: func() wltProbeRequest {
			request := wltHTTPSRequest()
			request.URL = "https://speed.cloudflare.com/__down?bytes=1048576&seed=seed-1"
			request.ExpectedStatus = 200
			request.ExpectedBytes = wltProbeBulkBytes
			request.MaxReadBytes = wltProbeBulkBytes + 1
			return request
		}(), body: make([]byte, wltProbeBulkBytes)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, localAddress, closeServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(test.request.ExpectedStatus)
				if len(test.body) != 0 {
					_, _ = writer.Write(test.body)
				}
			}))
			defer closeServer()
			resolver, group, leaf, wlt := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, localAddress)
			})
			leaf.destination = make(chan string, 1)
			trap := &wltProbeFakeOutbound{typ: "direct", tag: "default"}
			resolver["default"] = trap
			result := executeWLTProbeForTest(t, ctx, new(int), resolver, test.request)
			if result.Status != "success" || result.ErrorCode != "" || result.HTTPStatus == nil || *result.HTTPStatus != test.request.ExpectedStatus || result.BytesRead == nil || *result.BytesRead != test.request.ExpectedBytes {
				t.Fatalf("unexpected result: %+v", result)
			}
			if leaf.dialCount.Load() != 1 || group.dialCount.Load() != 0 || wlt.dialCount.Load() != 0 || trap.dialCount.Load() != 0 {
				t.Fatalf("wrong dial path: leaf=%d group=%d wlt=%d default=%d", leaf.dialCount.Load(), group.dialCount.Load(), wlt.dialCount.Load(), trap.dialCount.Load())
			}
			expectedHost := "cp.cloudflare.com:443"
			if test.name == "one MiB" {
				expectedHost = "speed.cloudflare.com:443"
			}
			if destination := <-leaf.destination; destination != expectedHost {
				t.Fatalf("dialed %q, want %q", destination, expectedHost)
			}
			if result.DestinationSHA256 != sha256Hex([]byte(test.request.URL)) || result.RequestSHA256 != sha256Hex([]byte("request")) {
				t.Fatal("incorrect request or destination digest")
			}
			assertWLTCommonResult(t, result, "https")
		})
	}
}

func TestWLTHTTPSProbeFailuresAreBoundedAndSanitized(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		contentLen int
		errorCode  string
		location   string
		raw204Body bool
	}{
		{name: "redirect", status: http.StatusFound, location: "https://cp.cloudflare.com/generate_204", errorCode: "https_status_mismatch"},
		{name: "wrong status", status: http.StatusOK, errorCode: "https_status_mismatch"},
		{name: "204 extra byte", raw204Body: true, errorCode: "https_bytes_mismatch"},
		{name: "extra byte", status: http.StatusOK, body: strings.Repeat("x", int(wltProbeBulkBytes+1)), errorCode: "https_bytes_mismatch"},
		{name: "short body", status: http.StatusOK, contentLen: 2, body: "x", errorCode: "https_read_failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := wltHTTPSRequest()
			if test.name == "short body" || test.name == "extra byte" {
				request.URL = "https://speed.cloudflare.com/__down?bytes=1048576&seed=seed-1"
				request.ExpectedStatus = 200
				request.ExpectedBytes = wltProbeBulkBytes
				request.MaxReadBytes = wltProbeBulkBytes + 1
			}
			ctx, localAddress, closeServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				if test.raw204Body {
					conn, buffered, hijackErr := writer.(http.Hijacker).Hijack()
					if hijackErr != nil {
						return
					}
					_, _ = buffered.WriteString("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\nx")
					_ = buffered.Flush()
					_ = conn.Close()
					return
				}
				if test.location != "" {
					writer.Header().Set("Location", test.location)
				}
				if test.contentLen != 0 {
					writer.Header().Set("Content-Length", fmt.Sprint(test.contentLen))
				}
				writer.WriteHeader(test.status)
				if test.body != "" {
					_, _ = io.WriteString(writer, test.body)
				}
			}))
			defer closeServer()
			resolver, _, _, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, localAddress)
			})
			encoded, err := executeWLTProbe(new(int), ctx, resolver, func() bool { return true }, request, sha256Hex([]byte("raw request")))
			if err != nil {
				t.Fatal(err)
			}
			var result wltProbeResult
			if err = json.Unmarshal([]byte(encoded), &result); err != nil {
				t.Fatal(err)
			}
			if result.Status != "failed" || result.ErrorCode != test.errorCode {
				t.Fatalf("unexpected result: %+v", result)
			}
			if strings.Contains(encoded, request.URL) || strings.Contains(encoded, localAddress) || len(encoded) >= wltProbeMaxOutputBytes {
				t.Fatalf("unsanitized or oversized result: %s", encoded)
			}
		})
	}
	t.Run("dial error hidden", func(t *testing.T) {
		resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			return nil, errors.New("SECRET raw network detail")
		})
		encoded, err := executeWLTProbe(new(int), context.Background(), resolver, func() bool { return true }, wltHTTPSRequest(), "hash")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(encoded, "SECRET") || !strings.Contains(encoded, `"error_code":"https_request_failed"`) {
			t.Fatalf("unexpected error result: %s", encoded)
		}
	})
}

type wltDNSResponder func(*mDNS.Msg) (*mDNS.Msg, []byte)

func startWLTTCPDNSServer(t *testing.T, responder wltDNSResponder) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				query, readErr := dnsTransport.ReadMessage(conn)
				if readErr != nil {
					return
				}
				response, raw := responder(query)
				if raw != nil {
					_, _ = conn.Write(raw)
					return
				}
				if response != nil {
					response.Compress = true
					packed, packErr := response.Pack()
					if packErr != nil {
						return
					}
					framed := make([]byte, 2+len(packed))
					binary.BigEndian.PutUint16(framed, uint16(len(packed)))
					copy(framed[2:], packed)
					_, _ = conn.Write(framed)
				}
			}()
		}
	}()
	return listener.Addr().String(), func() { _ = listener.Close(); <-done }
}

func wltDNSReply(query *mDNS.Msg) *mDNS.Msg {
	response := new(mDNS.Msg)
	response.SetReply(query)
	return response
}

func TestWLTDNSProbeSuccessIsolationAndSanitization(t *testing.T) {
	for _, cname := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct A", true: "CNAME to A"}[cname], func(t *testing.T) {
			server, closeServer := startWLTTCPDNSServer(t, func(query *mDNS.Msg) (*mDNS.Msg, []byte) {
				response := wltDNSReply(query)
				owner := query.Question[0].Name
				if cname {
					response.Answer = append(response.Answer, &mDNS.CNAME{Hdr: mDNS.RR_Header{Name: strings.ToUpper(owner), Rrtype: mDNS.TypeCNAME, Class: mDNS.ClassINET, Ttl: 1}, Target: "Alias.Example.COM."})
					owner = "ALIAS.EXAMPLE.COM."
				}
				response.Answer = append(response.Answer, &mDNS.A{Hdr: mDNS.RR_Header{Name: owner, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET, Ttl: 1}, A: net.ParseIP("192.0.2.44")})
				return response, nil
			})
			defer closeServer()
			destination := make(chan string, 1)
			resolver, group, leaf, wlt := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server)
			})
			leaf.destination = destination
			trap := &wltProbeFakeOutbound{typ: "direct", tag: "default"}
			resolver["default"] = trap
			request := wltDNSRequest("192.0.2.53:5353")
			encoded, err := executeWLTProbe(new(int), context.Background(), resolver, func() bool { return true }, request, "request-hash")
			if err != nil {
				t.Fatal(err)
			}
			var result wltProbeResult
			if err = json.Unmarshal([]byte(encoded), &result); err != nil {
				t.Fatal(err)
			}
			if result.Status != "success" || result.DNSAnswerCount == nil || *result.DNSAnswerCount != 1 || result.DNSRCode == nil || *result.DNSRCode != 0 {
				t.Fatalf("unexpected result: %+v", result)
			}
			if got := <-destination; got != request.Server {
				t.Fatalf("dialed %q, want exact saved endpoint %q", got, request.Server)
			}
			if leaf.dialCount.Load() != 1 || group.dialCount.Load() != 0 || wlt.dialCount.Load() != 0 || trap.dialCount.Load() != 0 {
				t.Fatal("probe dialed group, WLT dependency, or unrelated/default outbound")
			}
			if strings.Contains(encoded, request.Server) || strings.Contains(encoded, "192.0.2.44") || result.DNSServerSHA256 != sha256Hex([]byte(request.Server)) || result.DNSQuestionSHA256 != sha256Hex([]byte("fresh.example.com")) {
				t.Fatalf("DNS result leaked values or used wrong digest: %s", encoded)
			}
			assertWLTCommonResult(t, result, "dns")
		})
	}
}

func TestWLTDNSProbeRejectsInvalidResponses(t *testing.T) {
	tests := map[string]struct {
		mutate    func(*mDNS.Msg, *mDNS.Msg)
		raw       []byte
		errorCode string
	}{
		"id mismatch":       {mutate: func(_ *mDNS.Msg, response *mDNS.Msg) { response.Id++ }, errorCode: "dns_id_mismatch"},
		"QR false":          {mutate: func(_ *mDNS.Msg, response *mDNS.Msg) { response.Response = false }, errorCode: "dns_response_invalid"},
		"wrong opcode":      {mutate: func(_ *mDNS.Msg, response *mDNS.Msg) { response.Opcode = mDNS.OpcodeStatus }, errorCode: "dns_response_invalid"},
		"truncated":         {mutate: func(_ *mDNS.Msg, response *mDNS.Msg) { response.Truncated = true }, errorCode: "dns_response_invalid"},
		"wrong question":    {mutate: func(_ *mDNS.Msg, response *mDNS.Msg) { response.Question[0].Name = "other.example.com." }, errorCode: "dns_question_mismatch"},
		"non success rcode": {mutate: func(_ *mDNS.Msg, response *mDNS.Msg) { response.Rcode = mDNS.RcodeNameError }, errorCode: "dns_rcode_mismatch"},
		"CNAME without A": {mutate: func(query *mDNS.Msg, response *mDNS.Msg) {
			response.Answer = []mDNS.RR{&mDNS.CNAME{Hdr: mDNS.RR_Header{Name: query.Question[0].Name, Rrtype: mDNS.TypeCNAME, Class: mDNS.ClassINET}, Target: "alias.example.com."}}
		}, errorCode: "dns_answer_missing"},
		"unrelated A": {mutate: func(_ *mDNS.Msg, response *mDNS.Msg) {
			response.Answer = []mDNS.RR{&mDNS.A{Hdr: mDNS.RR_Header{Name: "other.example.com.", Rrtype: mDNS.TypeA, Class: mDNS.ClassINET}, A: net.ParseIP("192.0.2.1")}}
		}, errorCode: "dns_answer_missing"},
		"non IN A": {mutate: func(query *mDNS.Msg, response *mDNS.Msg) {
			response.Answer = []mDNS.RR{&mDNS.A{Hdr: mDNS.RR_Header{Name: query.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassCHAOS}, A: net.ParseIP("192.0.2.1")}}
		}, errorCode: "dns_answer_missing"},
		"malformed frame":       {raw: []byte{0, 12, 1, 2, 3}, errorCode: "dns_read_failed"},
		"large malformed frame": {raw: append([]byte{0xff, 0xff}, make([]byte, 65_535)...), errorCode: "dns_read_failed"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			server, closeServer := startWLTTCPDNSServer(t, func(query *mDNS.Msg) (*mDNS.Msg, []byte) {
				if test.raw != nil {
					return nil, test.raw
				}
				response := wltDNSReply(query)
				test.mutate(query, response)
				return response, nil
			})
			defer closeServer()
			resolver, _, _, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server)
			})
			result := executeWLTProbeForTest(t, context.Background(), new(int), resolver, wltDNSRequest("192.0.2.53:53"))
			if result.Status != "failed" || result.ErrorCode != test.errorCode {
				t.Fatalf("unexpected result: %+v", result)
			}
		})
	}
	t.Run("dial failure", func(t *testing.T) {
		resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			return nil, errors.New("secret endpoint")
		})
		result := executeWLTProbeForTest(t, context.Background(), new(int), resolver, wltDNSRequest("192.0.2.53:53"))
		if result.ErrorCode != "dns_dial_failed" {
			t.Fatalf("unexpected result: %+v", result)
		}
	})
	t.Run("write failure", func(t *testing.T) {
		resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			return &wltProbeErrorConn{writeErr: errors.New("write")}, nil
		})
		result := executeWLTProbeForTest(t, context.Background(), new(int), resolver, wltDNSRequest("192.0.2.53:53"))
		if result.ErrorCode != "dns_write_failed" {
			t.Fatalf("unexpected result: %+v", result)
		}
	})
	t.Run("read failure", func(t *testing.T) {
		resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			return &wltProbeErrorConn{readErr: errors.New("read")}, nil
		})
		result := executeWLTProbeForTest(t, context.Background(), new(int), resolver, wltDNSRequest("192.0.2.53:53"))
		if result.ErrorCode != "dns_read_failed" {
			t.Fatalf("unexpected result: %+v", result)
		}
	})
}

func TestMatchingWLTAAnswersBoundsCNAMEsAndRequiresIPv4IN(t *testing.T) {
	query := "fresh.example.com"
	invalidAddress := []mDNS.RR{&mDNS.A{
		Hdr: mDNS.RR_Header{Name: mDNS.Fqdn(query), Rrtype: mDNS.TypeA, Class: mDNS.ClassINET},
		A:   net.ParseIP("2001:db8::1"),
	}}
	if count := matchingWLTAAnswers(invalidAddress, query); count != 0 {
		t.Fatalf("counted non-IPv4 A payload: %d", count)
	}
	answers := make([]mDNS.RR, 0, wltProbeMaxCNAMEHops+2)
	owner := mDNS.Fqdn(query)
	for index := range wltProbeMaxCNAMEHops + 1 {
		target := fmt.Sprintf("hop-%d.example.com.", index)
		answers = append(answers, &mDNS.CNAME{
			Hdr: mDNS.RR_Header{Name: owner, Rrtype: mDNS.TypeCNAME, Class: mDNS.ClassINET}, Target: target,
		})
		owner = target
	}
	answers = append(answers, &mDNS.A{
		Hdr: mDNS.RR_Header{Name: owner, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET}, A: net.ParseIP("192.0.2.1"),
	})
	if count := matchingWLTAAnswers(answers, query); count != 0 {
		t.Fatalf("followed CNAME chain beyond hop bound: %d", count)
	}
}

type wltProbeErrorConn struct {
	readErr     error
	writeErr    error
	deadlineErr error
	closed      atomic.Bool
	written     atomic.Int64
}

func (c *wltProbeErrorConn) Read([]byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	return 0, io.EOF
}
func (c *wltProbeErrorConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.written.Add(int64(len(p)))
	return len(p), nil
}
func (c *wltProbeErrorConn) Close() error                   { c.closed.Store(true); return nil }
func (*wltProbeErrorConn) LocalAddr() net.Addr              { return wltProbeTestAddr("local") }
func (*wltProbeErrorConn) RemoteAddr() net.Addr             { return wltProbeTestAddr("remote") }
func (c *wltProbeErrorConn) SetDeadline(time.Time) error    { return c.deadlineErr }
func (*wltProbeErrorConn) SetReadDeadline(time.Time) error  { return nil }
func (*wltProbeErrorConn) SetWriteDeadline(time.Time) error { return nil }

type wltProbeTestAddr string

func (a wltProbeTestAddr) Network() string { return string(a) }
func (a wltProbeTestAddr) String() string  { return string(a) }

func TestWLTProbeBusyGateAndTimeoutOwnership(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
		if calls.Add(1) == 1 {
			started <- struct{}{}
			<-release
		}
		return nil, errors.New("dial stopped")
	})
	key := new(int)
	request := wltHTTPSRequest()
	request.TimeoutMS = 1_000
	firstDone := make(chan error, 1)
	go func() {
		_, err := executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash")
		firstDone <- err
	}()
	<-started
	if _, err := executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash"); err == nil {
		t.Fatal("same-instance concurrent probe was not rejected")
	}
	if _, err := executeWLTProbe(new(int), context.Background(), resolver, func() bool { return true }, request, "hash"); err != nil {
		t.Fatalf("different instance was incorrectly busy: %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first probe failed as Go error: %v", err)
	}
	if _, err := executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash"); err != nil {
		t.Fatalf("instance remained busy after worker exit: %v", err)
	}
}

func TestWLTProbeTimeoutCancellationAndInstanceChange(t *testing.T) {
	t.Run("context-aware dial timeout", func(t *testing.T) {
		resolver, _, _, _ := wltProbeGraph(func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		request := wltHTTPSRequest()
		request.TimeoutMS = 20
		started := time.Now()
		result := executeWLTProbeForTest(t, context.Background(), new(int), resolver, request)
		if (result.ErrorCode != "timeout" && result.ErrorCode != "timeout_cleanup_pending") || time.Since(started) > time.Second {
			t.Fatalf("timeout was not bounded: %+v elapsed=%v", result, time.Since(started))
		}
	})
	t.Run("instance replaced", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resolver, _, _, _ := wltProbeGraph(func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) { return nil, ctx.Err() })
		encoded, err := executeWLTProbe(new(int), ctx, resolver, func() bool { return false }, wltHTTPSRequest(), "hash")
		if err != nil {
			t.Fatal(err)
		}
		var result wltProbeResult
		if err = json.Unmarshal([]byte(encoded), &result); err != nil {
			t.Fatal(err)
		}
		if result.ErrorCode != "instance_changed" || result.InstanceCurrent {
			t.Fatalf("unexpected instance-change result: %+v", result)
		}
	})
	t.Run("busy retained until cancellation-ignoring dial exits", func(t *testing.T) {
		release := make(chan struct{})
		started := make(chan struct{}, 1)
		var calls atomic.Int32
		resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			if calls.Add(1) == 1 {
				started <- struct{}{}
				<-release
			}
			return nil, errors.New("done")
		})
		request := wltHTTPSRequest()
		request.TimeoutMS = 20
		key := new(int)
		callStarted := time.Now()
		encoded, err := executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash")
		if err != nil {
			t.Fatal(err)
		}
		<-started
		if time.Since(callStarted) > time.Second || !strings.Contains(encoded, `"error_code":"timeout_cleanup_pending"`) {
			t.Fatalf("pending cleanup timeout was not bounded: elapsed=%v result=%s", time.Since(callStarted), encoded)
		}
		if _, err = executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash"); err == nil {
			t.Fatal("busy token released before worker exit")
		}
		close(release)
		deadline := time.Now().Add(time.Second)
		for {
			_, err = executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash")
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("busy token not released after worker exit")
			}
			time.Sleep(time.Millisecond)
		}
	})
}

func TestWLTProbeRechecksInstanceAfterWorkerCompletion(t *testing.T) {
	server, closeServer := startWLTTCPDNSServer(t, func(query *mDNS.Msg) (*mDNS.Msg, []byte) {
		response := wltDNSReply(query)
		response.Answer = []mDNS.RR{&mDNS.A{
			Hdr: mDNS.RR_Header{Name: query.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET},
			A:   net.ParseIP("192.0.2.1"),
		}}
		return response, nil
	})
	defer closeServer()
	resolver, _, _, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server)
	})
	var checks atomic.Int32
	encoded, err := executeWLTProbe(new(int), context.Background(), resolver, func() bool {
		return checks.Add(1) == 1
	}, wltDNSRequest("192.0.2.53:53"), "hash")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(encoded, `"instance_current":false`) || !strings.Contains(encoded, `"error_code":"instance_changed"`) || strings.Contains(encoded, `"status":"success"`) {
		t.Fatalf("caller did not recheck instance after worker completion: %s", encoded)
	}
}

type wltProbeLaggingDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c wltProbeLaggingDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (wltProbeLaggingDeadlineContext) Done() <-chan struct{}         { return nil }
func (wltProbeLaggingDeadlineContext) Err() error                    { return nil }

func TestFinishWLTProbeResultRejectsElapsedDeadlineWhenContextErrLags(t *testing.T) {
	ctx := wltProbeLaggingDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond)}
	result := wltProbeResult{Status: "success", InstanceCurrent: true}
	finishWLTProbeResult(&result, ctx, func() bool { return true }, time.Now().Add(-time.Second), "timeout")
	if result.Status != "failed" || result.ErrorCode != "timeout" {
		t.Fatalf("elapsed deadline remained successful: %+v", result)
	}
}

type wltProbeBlockingCloseConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	closed  atomic.Bool
}

func (c *wltProbeBlockingCloseConn) Close() error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestWLTProbeTimeoutTracksPendingCleanupAndNeverReturnsLateSuccess(t *testing.T) {
	server, closeServer := startWLTTCPDNSServer(t, func(query *mDNS.Msg) (*mDNS.Msg, []byte) {
		response := wltDNSReply(query)
		response.Answer = []mDNS.RR{&mDNS.A{
			Hdr: mDNS.RR_Header{Name: query.Question[0].Name, Rrtype: mDNS.TypeA, Class: mDNS.ClassINET},
			A:   net.ParseIP("192.0.2.1"),
		}}
		return response, nil
	})
	defer closeServer()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var conn *wltProbeBlockingCloseConn
	resolver, _, _, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		inner, err := (&net.Dialer{}).DialContext(ctx, network, server)
		if err != nil {
			return nil, err
		}
		conn = &wltProbeBlockingCloseConn{Conn: inner, entered: entered, release: release}
		return conn, nil
	})
	request := wltDNSRequest("192.0.2.53:53")
	request.TimeoutMS = 20
	key := new(int)
	returned := make(chan string, 1)
	go func() {
		encoded, _ := executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash")
		returned <- encoded
	}()
	<-entered
	encoded := <-returned
	if !strings.Contains(encoded, `"error_code":"timeout_cleanup_pending"`) || strings.Contains(encoded, `"status":"success"`) {
		t.Fatalf("pending cleanup returned success or wrong code: %s", encoded)
	}
	if conn.closed.Load() {
		t.Fatal("connection unexpectedly completed cleanup before release")
	}
	if _, err := executeWLTProbe(key, context.Background(), resolver, func() bool { return true }, request, "hash"); err == nil {
		t.Fatal("busy token released while connection cleanup was pending")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for !conn.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !conn.closed.Load() {
		t.Fatal("tracked connection cleanup did not finish after release")
	}
	for {
		_, busy := wltProbeBusy.Load(key)
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("busy token remained after tracked cleanup finished")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWLTProbeDNSConnectionClosed(t *testing.T) {
	conn := &wltProbeErrorConn{readErr: io.EOF}
	resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) { return conn, nil })
	_ = executeWLTProbeForTest(t, context.Background(), new(int), resolver, wltDNSRequest("192.0.2.53:53"))
	if !conn.closed.Load() {
		t.Fatal("DNS connection was not closed")
	}
}

func TestWLTProbeRejectsUnsupportedDeadlineBeforeProtocolIO(t *testing.T) {
	for _, request := range []wltProbeRequest{wltHTTPSRequest(), wltDNSRequest("192.0.2.53:53")} {
		t.Run(request.Kind, func(t *testing.T) {
			conn := &wltProbeErrorConn{deadlineErr: errors.New("unsupported")}
			resolver, _, _, _ := wltProbeGraph(func(context.Context, string, M.Socksaddr) (net.Conn, error) { return conn, nil })
			result := executeWLTProbeForTest(t, context.Background(), new(int), resolver, request)
			if result.ErrorCode != "deadline_unavailable" || conn.written.Load() != 0 || !conn.closed.Load() {
				t.Fatalf("unsupported deadline was not rejected before I/O: result=%+v written=%d closed=%v", result, conn.written.Load(), conn.closed.Load())
			}
		})
	}
}

type wltProbeAbortConn struct {
	*wltProbeErrorConn
	aborted atomic.Bool
}

func (c *wltProbeAbortConn) Abort() error {
	c.aborted.Store(true)
	return nil
}

type wltProbeUpstreamConn struct {
	*wltProbeErrorConn
	upstream any
}

func (c *wltProbeUpstreamConn) Upstream() any { return c.upstream }

func TestCloseWLTProbeConnUsesPerStreamAbortThroughWrappers(t *testing.T) {
	abortConn := &wltProbeAbortConn{wltProbeErrorConn: &wltProbeErrorConn{}}
	outer := &wltProbeUpstreamConn{wltProbeErrorConn: &wltProbeErrorConn{}, upstream: abortConn}
	if err := closeWLTProbeConn(outer); err != nil {
		t.Fatal(err)
	}
	if !abortConn.aborted.Load() || outer.closed.Load() || abortConn.closed.Load() {
		t.Fatalf("did not use scoped abort: aborted=%v outer_closed=%v inner_closed=%v", abortConn.aborted.Load(), outer.closed.Load(), abortConn.closed.Load())
	}
}

func assertWLTCommonResult(t *testing.T, result wltProbeResult, kind string) {
	t.Helper()
	if result.Schema != 1 || result.Scope != wltProbeScope || result.ProbeID != "probe-1" || result.Kind != kind ||
		result.GroupTag != "group" || result.OutboundTag != "leaf" || result.WLTTag != "wlt" ||
		result.Network != "tcp" || result.Attempt != "primary" || result.FallbackAttempted ||
		result.SelectionTouched || result.ProfileTouched || !result.InstanceCurrent || result.DurationMS < 0 {
		t.Fatalf("invalid common result contract: %+v", result)
	}
	if kind == "https" {
		if result.HTTPStatus == nil || result.BytesRead == nil || result.DestinationSHA256 == "" || result.DNSRCode != nil || result.DNSAnswerCount != nil || result.DNSQuestionSHA256 != "" || result.DNSServerSHA256 != "" {
			t.Fatalf("invalid HTTPS result schema: %+v", result)
		}
	} else if result.HTTPStatus != nil || result.BytesRead != nil || result.DestinationSHA256 != "" || result.DNSRCode == nil || result.DNSAnswerCount == nil || result.DNSQuestionSHA256 == "" || result.DNSServerSHA256 == "" {
		t.Fatalf("invalid DNS result schema: %+v", result)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	expectedFieldCount := 20
	if kind == "dns" {
		expectedFieldCount = 21
	}
	if len(fields) != expectedFieldCount {
		t.Fatalf("unexpected %s result field set (%d fields): %s", kind, len(fields), encoded)
	}
}

func TestWLTProbeRequestAndResultBounds(t *testing.T) {
	request := wltHTTPSRequest()
	request.ProbeID = strings.Repeat("a", 64)
	result := newWLTProbeResult(request, strings.Repeat("f", 64))
	result.Status = "success"
	encoded, err := encodeWLTProbeResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= wltProbeMaxOutputBytes || strings.Contains(encoded, request.URL) {
		t.Fatalf("result is oversized or contains raw URL: len=%d result=%s", len(encoded), encoded)
	}
	if _, err = netip.ParseAddrPort("[2001:db8::1]:53"); err != nil {
		t.Fatal(err)
	}
}
