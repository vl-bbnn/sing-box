//go:build with_wlt

package libbox

import (
	"context"
	"encoding/json"
	"errors"
	commonwlt "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing/service"
	"math"
	"time"
)

// CheckWltHandover requires a negotiated original-session recovery and one fresh
// HTTPS response on that same carrier/interface/instance. No profile/API refresh,
// cached startup signal, selector mutation or application-request replay occurs.
func (s *CommandServer) CheckWltHandover(requestJSON, name string, index int64) (bool, error) {
	request, requestHash, err := parseWLTProbeRequest(requestJSON)
	if err != nil {
		return false, err
	}
	if request.Kind != "https" || request.URL != "https://cp.cloudflare.com/generate_204" || request.TimeoutMS > 5000 || index <= 0 || index > math.MaxInt32 {
		return false, errors.New("invalid handover proof request")
	}
	instance := s.StartedService.Instance()
	if instance == nil || instance.Box() == nil {
		return false, errors.New("active instance unavailable")
	}
	provider := service.FromContext[commonwlt.HandoverProofProvider](instance.Context())
	if provider == nil {
		return false, errors.New("handover retention unavailable")
	}
	ctx, cancel := context.WithTimeout(instance.Context(), time.Duration(request.TimeoutMS)*time.Millisecond)
	defer cancel()
	current := func() bool { return s.StartedService.Instance() == instance }
	return executeWLTHandoverProof(ctx, provider, name, int(index), request, requestHash, current, func(valid func() bool) (string, error) {
		return executeWLTProbe(instance, ctx, instance.Box().Outbound(), valid, request, requestHash)
	})
}
func executeWLTHandoverProof(ctx context.Context, provider commonwlt.HandoverProofProvider, name string, index int, request wltProbeRequest, requestHash string, instanceCurrent func() bool, probe func(func() bool) (string, error)) (bool, error) {
	proof, err := provider.BeginWltHandoverProof(ctx, name, index)
	if err != nil {
		return false, err
	}
	if proof == nil {
		return false, errors.New("handover proof unavailable")
	}
	current := func() bool { return ctx.Err() == nil && instanceCurrent() && proof.Current() }
	if !current() {
		return false, errors.New("handover original ownership changed")
	}
	// Always create fresh work. WaitWltTrafficReady's once-closed startup signal
	// is intentionally not consulted anywhere in this path.
	raw, err := probe(current)
	if err != nil {
		return false, err
	}
	var result wltProbeResult
	if json.Unmarshal([]byte(raw), &result) != nil || !current() {
		return false, errors.New("handover fresh traffic ownership failed")
	}
	if result.Schema != 1 || result.Scope != wltProbeScope || result.ProbeID != request.ProbeID || result.RequestSHA256 != requestHash || result.Status != "success" || !result.InstanceCurrent || result.FallbackAttempted || result.ProfileTouched || result.SelectionTouched || result.HTTPStatus == nil || *result.HTTPStatus != 204 || result.BytesRead == nil || *result.BytesRead != 0 {
		return false, errors.New("handover fresh traffic failed")
	}
	return true, nil
}
