//go:build with_wlt

package libbox

import (
	"context"
	"errors"
	commonwlt "github.com/sagernet/sing-box/common/wlt"
	M "github.com/sagernet/sing/common/metadata"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type handoverProofFixture struct {
	current atomic.Bool
	blocked bool
}

func (p *handoverProofFixture) Current() bool { return p.current.Load() }
func (p *handoverProofFixture) BeginWltHandoverProof(ctx context.Context, _ string, _ int) (commonwlt.HandoverProof, error) {
	if p.blocked {
		return nil, errors.New("no negotiated original hold")
	}
	return p, nil
}
func newCurrentHandoverProof() *handoverProofFixture {
	p := &handoverProofFixture{}
	p.current.Store(true)
	return p
}
func TestWLTHandoverProofAlwaysPerformsFreshTLSRequest(t *testing.T) {
	var responses atomic.Int32
	root, address, closeServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { responses.Add(1); w.WriteHeader(204) }))
	defer closeServer()
	resolver, group, leaf, raw := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	})
	proof := newCurrentHandoverProof()
	req := wltHTTPSRequest()
	for range 2 {
		ctx, cancel := context.WithTimeout(root, time.Second)
		good, err := executeWLTHandoverProof(ctx, proof, "wlan0", 5, req, "hash", func() bool { return true }, func(valid func() bool) (string, error) {
			return executeWLTProbe(new(int), ctx, resolver, valid, req, "hash")
		})
		cancel()
		if !good || err != nil {
			t.Fatal("fresh TLS traffic failed", good, err)
		}
	}
	if responses.Load() != 2 || leaf.dialCount.Load() != 2 || group.dialCount.Load() != 0 || raw.dialCount.Load() != 0 || group.now != "ordinary" {
		t.Fatal("fresh traffic reused/skipped or route selection mutated", responses.Load(), leaf.dialCount.Load())
	}
}
func TestWLTHandoverProofRejectsUnretainedWithoutDial(t *testing.T) {
	proof := newCurrentHandoverProof()
	proof.blocked = true
	calls := 0
	good, err := executeWLTHandoverProof(context.Background(), proof, "wlan0", 5, wltHTTPSRequest(), "hash", func() bool { return true }, func(func() bool) (string, error) { calls++; return "", nil })
	if good || err == nil || calls != 0 {
		t.Fatal("startup readiness authorized a fresh dial/reload skip")
	}
}
func TestWLTHandoverProofRejectsChangeDuringFreshTLSResponse(t *testing.T) {
	proof := newCurrentHandoverProof()
	root, address, closeServer := newWLTProbeTLSContext(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proof.current.Store(false); w.WriteHeader(204) }))
	defer closeServer()
	resolver, _, leaf, _ := wltProbeGraph(func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	})
	ctx, cancel := context.WithTimeout(root, time.Second)
	defer cancel()
	req := wltHTTPSRequest()
	good, err := executeWLTHandoverProof(ctx, proof, "wlan0", 5, req, "hash", func() bool { return true }, func(valid func() bool) (string, error) {
		return executeWLTProbe(new(int), ctx, resolver, valid, req, "hash")
	})
	if good || err == nil || leaf.dialCount.Load() != 1 {
		t.Fatal("obsolete original ownership accepted")
	}
}
func TestWLTHandoverProofKeepsFailureAndOriginalDeadline(t *testing.T) {
	proof := newCurrentHandoverProof()
	cause := errors.New("actual wire failure")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	good, err := executeWLTHandoverProof(ctx, proof, "wlan0", 5, wltHTTPSRequest(), "hash", func() bool { return true }, func(valid func() bool) (string, error) {
		got, _ := ctx.Deadline()
		if !got.Equal(deadline) {
			t.Fatal("deadline renewed")
		}
		return "", cause
	})
	if good || !errors.Is(err, cause) {
		t.Fatal("actual wire failure hidden", err)
	}
}
