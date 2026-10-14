//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"strings"
)

type retainedHandoverProof struct {
	service                                *Service
	scope                                  context.Context
	carrier                                *wltpkg.Carrier
	carrierGeneration, interfaceGeneration uint64
	interfaceKey                           string
}

func (p *retainedHandoverProof) Current() bool {
	if p == nil || p.scope.Err() != nil {
		return false
	}
	s := p.service
	s.access.RLock()
	defer s.access.RUnlock()
	return s.ctx.Err() == nil && !s.stopped && !s.quiescing && !s.interfaceUnavailable && s.networkHold == nil && s.carrier == p.carrier && s.carrierGeneration == p.carrierGeneration && s.retainedCarrier == p.carrier && s.retainedCarrierGeneration == p.carrierGeneration && s.retentionRejectedGeneration != p.carrierGeneration && s.interfaceGeneration == p.interfaceGeneration && s.interfaceKey == p.interfaceKey && interfaceGateOpenUnlockedForProof(s)
}
func interfaceGateOpenUnlockedForProof(s *Service) bool {
	select {
	case <-s.interfaceReady:
		return true
	default:
		return false
	}
}

// A startup traffic-ready signal cannot authorize skipping a native reload.
// Wait only for this exact hold; never follow a replacement carrier/generation.
func (s *Service) BeginWltHandoverProof(ctx context.Context, name string, index int) (wltpkg.HandoverProof, error) {
	if ctx == nil || name == "" || len(name) > 64 || strings.ContainsAny(name, "|\r\n") || index <= 0 {
		return nil, errors.New("invalid handover scope")
	}
	prefix := fmt.Sprintf("%d|%s|", index, name)
	s.access.RLock()
	carrier, generation, iface := s.carrier, s.carrierGeneration, s.interfaceGeneration
	s.access.RUnlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.access.RLock()
		if s.ctx.Err() != nil || s.stopped || s.quiescing || carrier == nil || generation == 0 || s.carrier != carrier || s.carrierGeneration != generation || s.interfaceGeneration != iface || s.retentionRejectedGeneration == generation || !strings.HasPrefix(s.interfaceKey, prefix) {
			s.access.RUnlock()
			return nil, errors.New("handover original ownership unavailable")
		}
		if s.networkHold != nil {
			h := s.networkHold
			valid := h.carrier == carrier && h.generation == generation && !h.failed && !h.completed
			done := h.done
			s.access.RUnlock()
			if !valid {
				return nil, errors.New("handover hold unavailable")
			}
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.ctx.Done():
				return nil, s.ctx.Err()
			}
		}
		valid := !s.interfaceUnavailable && s.retainedCarrier == carrier && s.retainedCarrierGeneration == generation && interfaceGateOpenUnlockedForProof(s)
		key := s.interfaceKey
		s.access.RUnlock()
		if !valid {
			return nil, errors.New("handover negotiated retention unavailable")
		}
		proof := &retainedHandoverProof{s, ctx, carrier, generation, iface, key}
		if !proof.Current() {
			return nil, errors.New("handover original ownership changed")
		}
		return proof, nil
	}
}
