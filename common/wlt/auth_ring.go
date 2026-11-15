//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

const (
	authSnapshotReserveSuffix    = ".reserve"
	authSnapshotQuarantineSuffix = ".quarantine"
	authSnapshotRejectOnceSuffix = ".test-reject-active-once"
	authSnapshotBootstrapSuffix  = ".bootstrap-consumed"
	authReserveReadyWait         = 30 * time.Second
	authReserveRetryInitial      = 15 * time.Minute
	authReserveRetryMaximum      = 6 * time.Hour
)

type CarrierAuthRingStatus struct {
	Version           int  `json:"version"`
	ActivePresent     bool `json:"active_present"`
	PreviousPresent   bool `json:"previous_present"`
	ReservePresent    bool `json:"reserve_present"`
	QuarantinePresent bool `json:"quarantine_present"`
	FaultArmed        bool `json:"fault_armed"`
	BootstrapConsumed bool `json:"bootstrap_consumed"`
}

type carrierAuthRingFault struct {
	Version int    `json:"version"`
	Action  string `json:"action"`
}

type carrierAuthRingBootstrapMarker struct {
	Version int `json:"version"`
}

const carrierAuthRingRejectActiveOnceAction = "reject_active_identity_once"

type carrierAuthCandidate struct {
	source string
	path   string
}

// CarrierAuthRingStatusJSON returns only lifecycle booleans. It never returns
// provider identity, cookies, tokens, endpoints, or snapshot contents.
func CarrierAuthRingStatusJSON(snapshotPath string) (string, error) {
	snapshotPath = filepath.Clean(snapshotPath)
	if snapshotPath == "." || !filepath.IsAbs(snapshotPath) {
		return "", errors.New("absolute auth snapshot path is required")
	}
	present := func(path string) (bool, error) {
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			return false, err
		}
		if !info.Mode().IsRegular() {
			return false, fmt.Errorf("auth ring path is not a regular file: %s", filepath.Base(path))
		}
		return true, nil
	}
	paths := []string{
		snapshotPath,
		snapshotPath + authSnapshotPreviousSuffix,
		snapshotPath + authSnapshotReserveSuffix,
		snapshotPath + authSnapshotQuarantineSuffix,
		snapshotPath + authSnapshotRejectOnceSuffix,
		snapshotPath + authSnapshotBootstrapSuffix,
	}
	values := make([]bool, len(paths))
	for index, path := range paths {
		value, err := present(path)
		if err != nil {
			return "", err
		}
		values[index] = value
	}
	content, err := json.Marshal(CarrierAuthRingStatus{
		Version:           1,
		ActivePresent:     values[0],
		PreviousPresent:   values[1],
		ReservePresent:    values[2],
		QuarantinePresent: values[3],
		FaultArmed:        values[4],
		BootstrapConsumed: values[5],
	})
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// ArmCarrierAuthRingTestRejectActiveOnce creates a one-shot local marker used
// by Dev qualification builds. StartCarrier consumes it before any injected
// rejection and refuses the fault unless an independent reserve is available.
func ArmCarrierAuthRingTestRejectActiveOnce(snapshotPath string) error {
	snapshotPath = filepath.Clean(snapshotPath)
	if snapshotPath == "." || !filepath.IsAbs(snapshotPath) {
		return errors.New("absolute auth snapshot path is required")
	}
	for _, path := range []string{snapshotPath, snapshotPath + authSnapshotReserveSuffix} {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("auth ring prerequisite unavailable: %w", err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("auth ring prerequisite is not a regular file: %s", filepath.Base(path))
		}
	}
	content, err := json.Marshal(carrierAuthRingFault{Version: 1, Action: carrierAuthRingRejectActiveOnceAction})
	if err != nil {
		return err
	}
	return writeCarrierAuthSnapshotFile(snapshotPath+authSnapshotRejectOnceSuffix, content)
}

func consumeCarrierAuthRingTestRejectActiveOnce(cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) ([]byte, bool, error) {
	markerPath := carrierAuthSnapshotPath(options) + authSnapshotRejectOnceSuffix
	if carrierAuthSnapshotPath(options) == "" {
		return nil, false, nil
	}
	content, err := os.ReadFile(markerPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read auth ring fault marker: %w", err)
	}
	// Consume before validation so a malformed or stale marker can never loop.
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("consume auth ring fault marker: %w", err)
	}
	var marker carrierAuthRingFault
	if err := json.Unmarshal(content, &marker); err != nil || marker.Version != 1 || marker.Action != carrierAuthRingRejectActiveOnceAction {
		return nil, false, errors.New("invalid auth ring fault marker")
	}
	independent, err := carrierAuthReserveIndependent(cfg, options)
	if err != nil {
		return nil, false, fmt.Errorf("validate auth ring reserve before fault: %w", err)
	}
	if !independent {
		return nil, false, errors.New("auth ring fault requires an independent reserve")
	}
	rejected, err := os.ReadFile(carrierAuthSnapshotPath(options))
	if err != nil {
		return nil, false, fmt.Errorf("read active auth identity before fault: %w", err)
	}
	if logf != nil {
		logf("WLT carrier auth ring test event=active_rejection_injected scope=identity once=true")
	}
	return rejected, true, nil
}

func carrierAuthReservePath(options CarrierOptions) string {
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return ""
	}
	return path + authSnapshotReserveSuffix
}

func carrierAuthBootstrapMarkerPath(options CarrierOptions) string {
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return ""
	}
	return path + authSnapshotBootstrapSuffix
}

// bootstrapCarrierAuthRing consumes an independently authorized inline reserve
// exactly once. The reserve is installed before the active snapshot: if the
// process stops between the two atomic writes, the still-inline active identity
// and the private reserve remain sufficient to complete startup and retry the
// idempotent import. No provider or control-plane request is made here.
func bootstrapCarrierAuthRing(cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) (bool, error) {
	reserve := bytes.TrimSpace([]byte(options.AuthReserveSnapshot))
	if len(reserve) == 0 {
		return false, nil
	}
	if cfg == nil {
		return true, errors.New("carrier config is required for auth ring bootstrap")
	}
	path := carrierAuthSnapshotPath(options)
	markerPath := carrierAuthBootstrapMarkerPath(options)
	if path == "" || markerPath == "" {
		return true, errors.New("persistent auth snapshot path is required for auth ring bootstrap")
	}
	marker, markerErr := os.ReadFile(markerPath)
	if markerErr == nil {
		var consumed carrierAuthRingBootstrapMarker
		if json.Unmarshal(marker, &consumed) != nil || consumed.Version != 1 {
			return true, errors.New("auth ring bootstrap marker is invalid")
		}
		if logf != nil {
			logf("WLT carrier auth ring event=bootstrap_skipped reason=already_consumed")
		}
		return true, nil
	} else if !errors.Is(markerErr, os.ErrNotExist) {
		return true, fmt.Errorf("inspect auth ring bootstrap marker: %w", markerErr)
	}

	inline := bytes.TrimSpace([]byte(options.AuthSnapshot))
	if len(inline) == 0 {
		return true, errors.New("inline active auth snapshot is required for auth ring bootstrap")
	}
	for _, candidate := range []struct {
		label    string
		snapshot []byte
	}{{"active", inline}, {"reserve", reserve}} {
		if err := carrierengine.ImportAuthSnapshotJSON(candidate.snapshot); err != nil {
			return true, fmt.Errorf("validate auth ring bootstrap %s snapshot: %w", candidate.label, err)
		}
	}
	independent, err := carrierAuthSnapshotsIndependent(*cfg, inline, reserve)
	if err != nil {
		return true, errors.New("validate auth ring bootstrap independence failed")
	}
	if !independent {
		return true, errors.New("auth ring bootstrap identities are not independent")
	}

	activePath := path
	reservePath := carrierAuthReservePath(options)
	active, activeErr := os.ReadFile(activePath)
	if activeErr != nil && !errors.Is(activeErr, os.ErrNotExist) {
		return true, fmt.Errorf("read existing active auth snapshot: %w", activeErr)
	}
	reserveExisting, reserveErr := os.ReadFile(reservePath)
	if reserveErr != nil && !errors.Is(reserveErr, os.ErrNotExist) {
		return true, fmt.Errorf("read existing reserve auth snapshot: %w", reserveErr)
	}
	active = bytes.TrimSpace(active)
	reserveExisting = bytes.TrimSpace(reserveExisting)
	if len(active) > 0 && carrierengine.ImportAuthSnapshotJSON(active) != nil {
		active = nil
	}
	if len(reserveExisting) > 0 && carrierengine.ImportAuthSnapshotJSON(reserveExisting) != nil {
		reserveExisting = nil
	}

	selectedActive := active
	selectedReserve := reserveExisting
	if len(selectedActive) > 0 && len(selectedReserve) > 0 {
		validExisting, compareErr := carrierAuthSnapshotsIndependent(*cfg, selectedActive, selectedReserve)
		if compareErr == nil && validExisting {
			if logf != nil {
				logf("WLT carrier auth ring event=bootstrap_preserved_existing identities=2 independent=true")
			}
		} else {
			selectedReserve = nil
		}
	}
	if len(selectedActive) > 0 && len(selectedReserve) == 0 {
		for _, candidate := range [][]byte{reserve, inline} {
			candidateIndependent, compareErr := carrierAuthSnapshotsIndependent(*cfg, selectedActive, candidate)
			if compareErr == nil && candidateIndependent {
				selectedReserve = candidate
				break
			}
		}
		if len(selectedReserve) == 0 {
			return true, errors.New("auth ring bootstrap has no reserve independent from the existing active identity")
		}
		if err := writeCarrierAuthSnapshotFile(reservePath, selectedReserve); err != nil {
			return true, fmt.Errorf("install auth ring bootstrap reserve: %w", err)
		}
	}
	if len(selectedActive) == 0 && len(selectedReserve) > 0 {
		for _, candidate := range [][]byte{inline, reserve} {
			candidateIndependent, compareErr := carrierAuthSnapshotsIndependent(*cfg, candidate, selectedReserve)
			if compareErr == nil && candidateIndependent {
				selectedActive = candidate
				break
			}
		}
		if len(selectedActive) == 0 {
			return true, errors.New("auth ring bootstrap has no active identity independent from the existing reserve")
		}
		if err := writeCarrierAuthSnapshotFile(activePath, selectedActive); err != nil {
			return true, fmt.Errorf("install auth ring bootstrap active: %w", err)
		}
	}
	if len(selectedActive) == 0 && len(selectedReserve) == 0 {
		selectedActive = inline
		selectedReserve = reserve
		if err := writeCarrierAuthSnapshotFile(reservePath, selectedReserve); err != nil {
			return true, fmt.Errorf("install auth ring bootstrap reserve: %w", err)
		}
		if err := writeCarrierAuthSnapshotFile(activePath, selectedActive); err != nil {
			return true, fmt.Errorf("install auth ring bootstrap active: %w", err)
		}
	}
	if err := writeCarrierAuthSnapshotFile(markerPath, []byte(`{"version":1}`)); err != nil {
		return true, fmt.Errorf("mark auth ring bootstrap consumed: %w", err)
	}
	if err := carrierengine.ImportAuthSnapshotJSON(selectedActive); err != nil {
		return true, fmt.Errorf("restore auth ring bootstrap active: %w", err)
	}
	if logf != nil {
		logf("WLT carrier auth ring event=bootstrap_consumed identities=2 independent=true persistence=local")
	}
	return true, nil
}

func carrierAuthQuarantinePath(options CarrierOptions) string {
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return ""
	}
	return path + authSnapshotQuarantineSuffix
}

func localCarrierAuthRecoveryCandidates(options CarrierOptions) []carrierAuthCandidate {
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return nil
	}
	return []carrierAuthCandidate{
		{source: "reserve", path: path + authSnapshotReserveSuffix},
		{source: "reserve_previous", path: path + authSnapshotReserveSuffix + authSnapshotPreviousSuffix},
		{source: "previous", path: path + authSnapshotPreviousSuffix},
	}
}

func currentCarrierAuthIdentity(cfg *carrierconfig.ClientConfig, options CarrierOptions) ([]byte, error) {
	path := carrierAuthSnapshotPath(options)
	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil && len(bytes.TrimSpace(data)) > 0 {
			return data, nil
		}
	}
	if cfg == nil {
		return nil, errors.New("carrier config is required to export the active auth identity")
	}
	data, err := carrierengine.ExportAuthSnapshotJSON(*cfg)
	if err != nil {
		return nil, fmt.Errorf("export active auth identity: %w", err)
	}
	return data, nil
}

func importCarrierAuthCandidate(cfg *carrierconfig.ClientConfig, candidate carrierAuthCandidate, rejectedIdentity []byte, logf func(string, ...any)) (bool, []byte, error) {
	if cfg == nil {
		return false, nil, errors.New("carrier config is required for auth ring")
	}
	data, err := os.ReadFile(candidate.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("read %s auth snapshot: %w", candidate.source, err)
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return false, nil, nil
	}
	if len(rejectedIdentity) > 0 {
		independent, err := carrierAuthSnapshotsIndependent(*cfg, rejectedIdentity, data)
		if err != nil {
			return false, nil, fmt.Errorf("compare %s auth identity: %w", candidate.source, err)
		}
		if !independent {
			if logf != nil {
				logf("WLT carrier auth ring test event=candidate_rejected_same_identity source=%s", candidate.source)
			}
			return false, nil, nil
		}
	}
	if err := carrierengine.ImportAuthSnapshotJSON(data); err != nil {
		return false, nil, fmt.Errorf("import %s auth snapshot: %w", candidate.source, err)
	}
	if logf != nil {
		remaining, _ := carrierAuthSnapshotRemainingTTL(data)
		logf("WLT carrier auth event=snapshot_loaded source=%s remaining_ttl_seconds=%d", candidate.source, int64(remaining/time.Second))
	}
	return true, data, nil
}

func consumeCarrierAuthRecoverySource(options CarrierOptions, source string, logf func(string, ...any)) error {
	switch source {
	case "reserve", "reserve_refreshed", "reserve_previous", "reserve_previous_refreshed":
	default:
		return nil
	}
	reservePath := carrierAuthReservePath(options)
	if reservePath == "" {
		return nil
	}
	for _, path := range []string{
		reservePath,
		reservePath + authSnapshotPreviousSuffix,
	} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("consume promoted reserve auth snapshot: %w", err)
		}
	}
	if logf != nil {
		logf("WLT carrier auth ring event=reserve_consumed replacement=active replenishment=required")
	}
	return nil
}

func writeCarrierAuthReserve(options CarrierOptions, data []byte) error {
	path := carrierAuthReservePath(options)
	if path == "" {
		return nil
	}
	reserveOptions := options
	reserveOptions.AuthSnapshotFile = path
	reserveOptions.AuthSnapshotOutputFile = path
	return writeCarrierAuthSnapshot(reserveOptions, data, true)
}

func quarantineRejectedCarrierAuth(cfg *carrierconfig.ClientConfig, options CarrierOptions, recoverySource string, logf func(string, ...any)) error {
	if cfg == nil || recoverySource == "" || recoverySource == "current" || recoverySource == "previous" || recoverySource == "refreshed" {
		return nil
	}
	path := carrierAuthSnapshotPath(options)
	quarantinePath := carrierAuthQuarantinePath(options)
	if path == "" || quarantinePath == "" {
		return nil
	}
	rejected, err := os.ReadFile(path + authSnapshotPreviousSuffix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read rejected auth snapshot: %w", err)
	}
	active, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read replacement auth snapshot: %w", err)
	}
	independent, err := carrierAuthSnapshotsIndependent(*cfg, active, rejected)
	if err != nil {
		return fmt.Errorf("compare rejected auth snapshot: %w", err)
	}
	if !independent {
		return nil
	}
	if err := writeCarrierAuthSnapshotFile(quarantinePath, rejected); err != nil {
		return fmt.Errorf("quarantine rejected auth snapshot: %w", err)
	}
	if err := os.Remove(path + authSnapshotPreviousSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove rejected previous auth snapshot: %w", err)
	}
	if logf != nil {
		logf("WLT carrier auth ring event=active_quarantined replacement=%s", recoverySource)
	}
	return nil
}

func carrierAuthReserveIndependent(cfg *carrierconfig.ClientConfig, options CarrierOptions) (bool, error) {
	if cfg == nil {
		return false, errors.New("carrier config is required for auth ring")
	}
	activePath := carrierAuthSnapshotPath(options)
	reservePath := carrierAuthReservePath(options)
	if activePath == "" || reservePath == "" {
		return false, nil
	}
	active, err := os.ReadFile(activePath)
	if err != nil {
		return false, err
	}
	reserve, err := os.ReadFile(reservePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return carrierAuthSnapshotsIndependent(*cfg, active, reserve)
}

func retireCarrierAuthQuarantine(options CarrierOptions) error {
	path := carrierAuthQuarantinePath(options)
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("retire quarantined auth identity: %w", err)
	}
	return nil
}

func recoverCarrierAuthQuarantine(cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) error {
	path := carrierAuthSnapshotPath(options)
	if cfg == nil || path == "" {
		return nil
	}
	active, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	previous, err := os.ReadFile(path + authSnapshotPreviousSuffix)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	independent, err := carrierAuthSnapshotsIndependent(*cfg, active, previous)
	if err != nil || !independent {
		return nil
	}
	if err := writeCarrierAuthSnapshotFile(carrierAuthQuarantinePath(options), previous); err != nil {
		return err
	}
	if err := os.Remove(path + authSnapshotPreviousSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if logf != nil {
		logf("WLT carrier auth ring event=quarantine_recovered")
	}
	return nil
}

// validateCarrierAuthReserveCandidate proves a relay connection using this
// identity. Stopping the validation connection can end its signaling session;
// this proves identity recovery, not that an idle signaling socket stays live.
func validateCarrierAuthReserveCandidate(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, raw []byte, logf func(string, ...any)) ([]byte, error) {
	if err := carrierengine.ImportAuthSnapshotJSON(raw); err != nil {
		return nil, fmt.Errorf("import reserve identity: %w", err)
	}
	client, err := connectCarrierClientForStart(ctx, cfg, options.ConnectTimeout, logf)
	if err != nil && (errors.Is(err, carriercommon.ErrSignalingSessionEnded) || errors.Is(err, carriercommon.ErrAuthSnapshotReauthorizationRequired)) {
		if options.startup != nil && options.startup.providerRateLimited() {
			return nil, err
		}
		if refreshErr := refreshExistingCarrierAuthCandidate(ctx, cfg, raw, "reserve_validation", logf); refreshErr != nil {
			return nil, refreshErr
		}
		client, err = connectCarrierClientForStart(ctx, cfg, options.ConnectTimeout, logf)
	}
	if err != nil {
		return nil, fmt.Errorf("validate reserve carrier: %w", err)
	}
	defer func() {
		if ctx.Err() != nil {
			_ = client.Abort()
		} else {
			_ = client.Stop()
		}
	}()
	if err := promoteCarrierAuthSnapshot(*cfg); err != nil {
		return nil, fmt.Errorf("promote reserve identity: %w", err)
	}
	return carrierengine.ExportAuthSnapshotJSON(*cfg)
}

func replenishCarrierAuthReserve(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) (ready bool, resultErr error) {
	if cfg == nil || carrierAuthSnapshotPath(options) == "" {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	active, err := os.ReadFile(carrierAuthSnapshotPath(options))
	if err != nil {
		return false, fmt.Errorf("read active auth snapshot: %w", err)
	}
	serialized, err := carrierengine.AuthSnapshotRequiresSerializedRecovery(*cfg, active)
	if err != nil {
		return false, fmt.Errorf("inspect reserve concurrency policy: %w", err)
	}
	if serialized {
		// A bearer reserve is a saved fallback, not a concurrently usable
		// identity. Even prewarm may join the call and evict the active client.
		// Recovery validates it only after the old carrier has stopped.
		if logf != nil {
			logf("WLT carrier auth ring event=replenish_deferred reason=serialized_bearer_identity")
		}
		return false, nil
	}
	independent, err := carrierAuthReserveIndependent(cfg, options)
	if err != nil {
		return false, fmt.Errorf("inspect reserve identity: %w", err)
	}
	// Restore active after the validation client is fully stopped, on all exits.
	defer func() {
		if err := carrierengine.ImportAuthSnapshotJSON(active); err != nil {
			ready = false
			resultErr = errors.Join(resultErr, fmt.Errorf("restore active auth identity: %w", err))
		}
	}()
	var validated []byte
	if independent {
		raw, err := os.ReadFile(carrierAuthReservePath(options))
		if err != nil {
			return false, err
		}
		validated, err = validateCarrierAuthReserveCandidate(ctx, cfg, options, raw, logf)
		if err != nil {
			if isProviderRateLimitError(err) {
				recordProviderRateLimit(options, logf)
			}
			// Expiry, closed signaling, and transport failure do not prove
			// identity revocation. Preserve the reserve for later retry.
			if !errors.Is(err, carriercommon.ErrAuthSnapshotReauthorizationRequired) {
				return false, err
			}
		}
	}
	if validated == nil {
		if options.startup != nil && options.startup.providerRateLimited() {
			if logf != nil {
				logf("WLT carrier auth ring event=replenish_deferred reason=provider_rate_limited")
			}
			return false, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if err := recoverCarrierAuthQuarantine(cfg, options, logf); err != nil {
			return false, fmt.Errorf("recover auth ring quarantine: %w", err)
		}
		if err := carrierengine.ImportAuthSnapshotJSON(active); err != nil {
			return false, err
		}
		if logf != nil {
			logf("WLT carrier auth ring event=replenish_started")
		}
		fresh, err := prewarmFreshCarrierAuthSnapshot(ctx, *cfg)
		if err != nil {
			if isProviderRateLimitError(err) {
				recordProviderRateLimit(options, logf)
			}
			return false, fmt.Errorf("create reserve auth identity: %w", err)
		}
		independent, err = carrierAuthSnapshotsIndependent(*cfg, active, fresh)
		if err != nil || !independent {
			return false, errors.New("fresh reserve identity is not independently verified")
		}
		validated, err = validateCarrierAuthReserveCandidate(ctx, cfg, options, fresh, logf)
		if err != nil {
			if isProviderRateLimitError(err) {
				recordProviderRateLimit(options, logf)
			}
			return false, err
		}
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	independent, err = carrierAuthSnapshotsIndependent(*cfg, active, validated)
	if err != nil || !independent {
		return false, errors.New("validated reserve identity is not independent")
	}
	if err := carrierengine.ImportAuthSnapshotJSON(active); err != nil {
		return false, fmt.Errorf("restore active before reserve promotion: %w", err)
	}
	if err := writeCarrierAuthReserve(options, validated); err != nil {
		return false, err
	}
	if err := retireCarrierAuthQuarantine(options); err != nil {
		return false, err
	}
	if logf != nil {
		logf("WLT carrier auth ring event=reserve_validated scope=identity_recovery")
	}
	return true, nil
}

func scheduleCarrierAuthReserve(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) <-chan struct{} {
	done := make(chan struct{})
	if cfg == nil || options.startup == nil || carrierAuthSnapshotPath(options) == "" {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		timer := time.NewTimer(authReserveReadyWait)
		defer timer.Stop()
		select {
		case <-options.startup.trafficReadySignal:
		case <-timer.C:
			if logf != nil {
				logf("WLT carrier auth ring event=replenish_deferred reason=traffic_not_ready")
			}
			return
		case <-ctx.Done():
			return
		}
		retryDelay := authReserveRetryInitial
		for {
			ready, err := replenishCarrierAuthReserve(ctx, cfg, options, logf)
			if ready && err == nil {
				// Recheck during long sessions as well as on service restart.
				retryDelay = authReserveRetryInitial
			}
			if err != nil && logf != nil {
				// The active carrier remains authoritative. Reserve replenishment is
				// background maintenance and must never tear it down.
				logf("WLT carrier auth ring event=replenish_failed error=%v", err)
			}

			wait := retryDelay
			if remaining := options.startup.providerRateLimitRemaining(); remaining > wait {
				wait = remaining
			}
			if logf != nil {
				logf("WLT carrier auth ring event=replenish_deferred retry_seconds=%d validated=%t", int64(wait/time.Second), ready)
			}
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
			if !ready && retryDelay < authReserveRetryMaximum {
				retryDelay *= 2
				if retryDelay > authReserveRetryMaximum {
					retryDelay = authReserveRetryMaximum
				}
			}
		}
	}()
	return done
}
