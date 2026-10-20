//go:build with_wlt

package group

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
)

type wltBodyProof struct {
	realTag    string
	generation uint64
	time       time.Time
	delay      uint16
}
type wltBodyProbe struct {
	options            option.WLTBodyProbeOptions
	fallback           adapter.Outbound
	ctx                context.Context
	cancel             context.CancelFunc
	access             sync.Mutex
	health             map[string]wltBodyProof // bounded by configured member count, not leaf churn
	lastAttempt        time.Time
	nextProbe          int
	lastGeneration     uint64
	generationAttempts int
	recentAttempts     []time.Time
	currentCancel      context.CancelFunc
	currentGeneration  uint64
	active             sync.WaitGroup
	closing            bool
}

// validateWLTBodyProbeOptions also runs in the public constructor, so
// `sing-box check` rejects invalid options before starting any network work.
func validateWLTBodyProbeOptions(options option.WLTURLTestOptions, tags []string, interval time.Duration) error {
	if options.WLTBodyProbe == nil {
		return nil
	}
	cfg := options.WLTBodyProbe
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("wlt_body_probe requires an explicit HTTPS URL without credentials or fragment")
	}
	if cfg.Bytes < 1 || cfg.Bytes > 65536 {
		return fmt.Errorf("wlt_body_probe bytes must be 1..65536")
	}
	if interval == 0 {
		interval = C.DefaultURLTestInterval
	}
	if interval < time.Minute {
		return fmt.Errorf("wlt_body_probe interval must be at least 1m")
	}
	if len(tags) < 2 || len(tags) > 8 {
		return fmt.Errorf("wlt_body_probe requires 2..8 members")
	}
	seen := make(map[string]bool)
	found := false
	for _, tag := range tags {
		if seen[tag] {
			return fmt.Errorf("duplicate wlt_body_probe member")
		}
		seen[tag] = true
		if tag == cfg.UnverifiedFallback {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("wlt_body_probe unverified_fallback must name a configured member")
	}
	return nil
}
func (g *URLTestGroup) configureWLTBodyProbe(options option.WLTURLTestOptions) error {
	if options.WLTBodyProbe == nil {
		return nil
	}
	tags := make([]string, len(g.outbounds))
	for i, out := range g.outbounds {
		tags[i] = out.Tag()
	}
	if err := validateWLTBodyProbeOptions(options, tags, g.interval); err != nil {
		return err
	}
	if g.wltBodyProbe != nil {
		return fmt.Errorf("wlt_body_probe already configured")
	}
	cfg := *options.WLTBodyProbe
	var fallback adapter.Outbound
	for _, out := range g.outbounds {
		if out.Tag() == cfg.UnverifiedFallback {
			fallback = out
		}
	}
	ctx, cancel := context.WithCancel(g.ctx)
	g.wltBodyProbe = &wltBodyProbe{options: cfg, fallback: fallback, ctx: ctx, cancel: cancel, health: make(map[string]wltBodyProof)}
	return nil
}

func (g *URLTestGroup) closeWLTBodyProbe() {
	p := g.wltBodyProbe
	if p == nil {
		return
	}
	p.access.Lock()
	p.closing = true
	p.cancel()
	p.access.Unlock()
	p.active.Wait()
}

func (g *URLTestGroup) selectWLTBodyProbe(network string, excluded adapter.Outbound) (adapter.Outbound, bool) {
	p := g.wltBodyProbe
	generation := g.interfaceUpdateGeneration.Load()
	excludedReal := ""
	if excluded != nil {
		excludedReal = g.wltBodyLeafTag(excluded)
	}
	var selected adapter.Outbound
	var delay uint16
	for _, out := range g.outbounds {
		real := g.wltBodyLeafTag(out)
		if out == excluded || (excluded != nil && real == excludedReal) || !common.Contains(out.Network(), network) {
			continue
		}
		p.access.Lock()
		proof, ok := p.health[out.Tag()]
		closing := p.closing
		p.access.Unlock()
		if closing || p.ctx.Err() != nil {
			return nil, false
		}
		if real == "" || !ok || proof.realTag != real || proof.generation != generation || time.Since(proof.time) >= g.interval {
			continue
		}
		if selected == nil || (!g.preferFirstAvailable && uint32(proof.delay)+uint32(g.tolerance) < uint32(delay)) {
			selected = out
			delay = proof.delay
		}
		if g.preferFirstAvailable {
			break
		}
	}
	if g.interfaceUpdateGeneration.Load() != generation {
		return g.bodyFallback(network, excluded), false
	}
	if selected != nil {
		return selected, true
	}
	return g.bodyFallback(network, excluded), false
}
func (g *URLTestGroup) bodyFallback(network string, excluded adapter.Outbound) adapter.Outbound {
	p := g.wltBodyProbe
	p.access.Lock()
	closing := p.closing
	p.access.Unlock()
	if closing || p.ctx.Err() != nil || p.fallback == excluded || !common.Contains(p.fallback.Network(), network) {
		return nil
	}
	if fallbackReal := g.wltBodyLeafTag(p.fallback); excluded != nil && fallbackReal != "" && fallbackReal == g.wltBodyLeafTag(excluded) {
		return nil
	}
	return p.fallback
}
func (g *URLTestGroup) alternativeWLTBodyProbe(network string, unavailable adapter.Outbound) adapter.Outbound {
	p := g.wltBodyProbe
	real := g.wltBodyLeafTag(unavailable)
	p.access.Lock()
	for tag, proof := range p.health {
		if tag == unavailable.Tag() || proof.realTag == real {
			delete(p.health, tag)
		}
	}
	p.access.Unlock()
	selected, _ := g.selectWLTBodyProbe(network, unavailable)
	return selected
}

func (g *URLTestGroup) tryWLTBodyProbe(caller context.Context, force bool) (map[string]uint16, error, bool) {
	p := g.wltBodyProbe
	result := make(map[string]uint16)
	if err := caller.Err(); err != nil {
		return result, err, true
	}
	generation := g.interfaceUpdateGeneration.Load()
	p.access.Lock()
	if p.closing || p.ctx.Err() != nil {
		p.access.Unlock()
		return result, context.Canceled, true
	}
	// Steady checks keep the configured interval. A changed interface may
	// borrow one settled follow-up, with a hard rolling 2 batches/minute and
	// 5s spacing for the entire group; force/GUI cannot bypass that cap.
	now := time.Now()
	recent := p.recentAttempts[:0]
	for _, at := range p.recentAttempts {
		if now.Sub(at) < time.Minute {
			recent = append(recent, at)
		}
	}
	p.recentAttempts = recent
	interfaceCheck := force && generation > 0 && (generation != p.lastGeneration || p.generationAttempts < 2)
	if !p.lastAttempt.IsZero() && now.Sub(p.lastAttempt) < g.interval && !interfaceCheck {
		p.access.Unlock()
		return result, nil, true
	}
	if len(recent) >= 2 || (!p.lastAttempt.IsZero() && now.Sub(p.lastAttempt) < 5*time.Second) {
		p.access.Unlock()
		return result, nil, true
	}
	p.recentAttempts = append(p.recentAttempts, now)
	if generation != p.lastGeneration {
		p.generationAttempts = 0
	}
	p.lastGeneration = generation
	if p.generationAttempts < 2 {
		p.generationAttempts++
	}
	p.lastAttempt = now
	first := p.nextProbe
	p.nextProbe = (first + 1) % len(g.outbounds)
	p.active.Add(1)
	p.access.Unlock()
	defer p.active.Done()
	// One absolute 15s batch budget, including semaphore wait and every Close.
	deadline := time.Now().Add(C.TCPTimeout)
	if callerDeadline, ok := caller.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	// The group context owns trust roots, NTP and other service values. A caller
	// contributes cancellation/deadline, never replaces that runtime context.
	ctx, cancel := context.WithDeadline(p.ctx, deadline)
	defer cancel()
	stop := context.AfterFunc(caller, cancel)
	defer stop()
	if caller.Err() != nil {
		cancel()
	}
	p.access.Lock()
	p.currentCancel = cancel
	p.currentGeneration = generation
	if p.closing || g.interfaceUpdateGeneration.Load() != generation {
		cancel()
	}
	p.access.Unlock()
	defer func() { p.access.Lock(); p.currentCancel = nil; p.access.Unlock() }()

	slots := make(chan struct{}, 2)
	var workers sync.WaitGroup
	var resultAccess sync.Mutex
	checked := make(map[string]bool)
probeLoop:
	for offset := range g.outbounds {
		detour := g.outbounds[(first+offset)%len(g.outbounds)]
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
		tag := detour.Tag()
		out, loaded := g.wltBodyLeaf(detour)
		if !loaded {
			p.access.Lock()
			delete(p.health, tag)
			p.access.Unlock()
			continue
		}
		real := out.Tag()
		if checked[real] {
			continue
		}
		checked[real] = true
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break probeLoop
		}
		// A socket deadline can release a slot before the context timer runs.
		// Do not start queued work after the same absolute batch deadline.
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			<-slots
			break
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			if ctx.Err() != nil || !time.Now().Before(deadline) {
				return
			}
			delay, err := urltest.BodyURLTest(ctx, p.options.URL, out, p.options.Bytes)
			sameLeaf := g.wltBodyLeafTag(detour) == real
			var tags []string
			if sameLeaf {
				for _, member := range g.outbounds {
					if g.wltBodyLeafTag(member) == real {
						tags = append(tags, member.Tag())
					}
				}
			}
			accepted := false
			p.access.Lock()
			if !p.closing && ctx.Err() == nil && g.interfaceUpdateGeneration.Load() == generation && sameLeaf {
				if err == nil {
					for _, memberTag := range tags {
						p.health[memberTag] = wltBodyProof{realTag: real, generation: generation, time: time.Now(), delay: delay}
					}
					accepted = true
				} else {
					for memberTag, proof := range p.health {
						if proof.realTag == real {
							delete(p.health, memberTag)
						}
					}
				}
			}
			p.access.Unlock()
			if accepted {
				resultAccess.Lock()
				for _, memberTag := range tags {
					result[memberTag] = delay
				}
				resultAccess.Unlock()
				g.logger.Debug("outbound ", tag, " body available: ", delay, "ms bytes=", p.options.Bytes)
			} else {
				g.logger.Debug("outbound ", tag, " body unavailable: ", err)
			}
		}()
	}
	workers.Wait()
	g.performUpdateCheck()
	batchErr := errors.Join(ctx.Err(), caller.Err())
	// A socket deadline can fire before the context timer is scheduled. Keep
	// the actual absolute deadline authoritative even in that ordering.
	if !time.Now().Before(deadline) {
		batchErr = errors.Join(batchErr, context.DeadlineExceeded)
	}
	return result, batchErr, true
}

// Retire a check started on an old physical network promptly. Its actual error
// is retained; no old result is copied into the new interface's health epoch.
func (g *URLTestGroup) invalidateWLTBodyProbeInterface() {
	p := g.wltBodyProbe
	if p == nil {
		return
	}
	p.access.Lock()
	cancel := p.currentCancel
	old := p.currentGeneration != g.interfaceUpdateGeneration.Load()
	p.access.Unlock()
	if cancel != nil && old {
		cancel()
	}
}

// Resolve all observable client-side group selections. A parent group tag is
// not a leaf identity: its nested selector can change without an interface
// notification. The original global RealTag/HEAD semantics remain unchanged.
func (g *URLTestGroup) wltBodyLeaf(out adapter.Outbound) (adapter.Outbound, bool) {
	seen := make(map[string]bool)
	for depth := 0; out != nil && depth < 32; depth++ {
		tag := out.Tag()
		if seen[tag] {
			return nil, false
		}
		seen[tag] = true
		child, isGroup := out.(adapter.OutboundGroup)
		if !isGroup {
			return out, true
		}
		next := child.Now()
		if next == "" {
			// A cold single-member wrapper has no choice to infer. Probe its
			// sole configured route without waiting for weak HEAD history.
			// Multiple unresolved choices still cannot establish a leaf.
			members := child.All()
			if len(members) != 1 || members[0] == "" {
				return nil, false
			}
			next = members[0]
		}
		var ok bool
		out, ok = g.outbound.Outbound(next)
		if !ok {
			return nil, false
		}
	}
	return nil, false
}
func (g *URLTestGroup) wltBodyLeafTag(out adapter.Outbound) string {
	leaf, ok := g.wltBodyLeaf(out)
	if !ok {
		return ""
	}
	return leaf.Tag()
}
