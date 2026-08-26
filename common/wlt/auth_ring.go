//go:build with_wlt

package wlt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

const (
	authSnapshotReserveSuffix    = ".reserve"
	authSnapshotQuarantineSuffix = ".quarantine"
	authReserveReadyWait         = 30 * time.Second
	authReserveRetryInitial      = 15 * time.Minute
	authReserveRetryMaximum      = 6 * time.Hour
)

type carrierAuthCandidate struct {
	source string
	path   string
}

func carrierAuthReservePath(options CarrierOptions) string {
	path := carrierAuthSnapshotPath(options)
	if path == "" {
		return ""
	}
	return path + authSnapshotReserveSuffix
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
		{source: "previous", path: path + authSnapshotPreviousSuffix},
		{source: "reserve", path: path + authSnapshotReserveSuffix},
		{source: "reserve_previous", path: path + authSnapshotReserveSuffix + authSnapshotPreviousSuffix},
	}
}

func importCarrierAuthCandidate(cfg *carrierconfig.ClientConfig, candidate carrierAuthCandidate, logf func(string, ...any)) (bool, error) {
	if cfg == nil {
		return false, errors.New("carrier config is required for auth ring")
	}
	data, err := os.ReadFile(candidate.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read %s auth snapshot: %w", candidate.source, err)
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return false, nil
	}
	if err := carrierengine.ImportAuthSnapshotJSON(data); err != nil {
		return false, fmt.Errorf("import %s auth snapshot: %w", candidate.source, err)
	}
	if logf != nil {
		remaining, _ := carrierAuthSnapshotRemainingTTL(data)
		logf("WLT carrier auth event=snapshot_loaded source=%s remaining_ttl_seconds=%d", candidate.source, int64(remaining/time.Second))
	}
	return true, nil
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

func replenishCarrierAuthReserve(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) error {
	if cfg == nil || carrierAuthSnapshotPath(options) == "" {
		return nil
	}
	independent, err := carrierAuthReserveIndependent(cfg, options)
	if err == nil && independent {
		if err := retireCarrierAuthQuarantine(options); err != nil {
			return err
		}
		if logf != nil {
			logf("WLT carrier auth ring event=reserve_ready")
		}
		return nil
	}
	if err := recoverCarrierAuthQuarantine(cfg, options, logf); err != nil {
		return fmt.Errorf("recover auth ring quarantine: %w", err)
	}
	if options.startup != nil && options.startup.providerRateLimited() {
		if logf != nil {
			logf("WLT carrier auth ring event=replenish_deferred reason=provider_rate_limited")
		}
		return nil
	}
	active, err := os.ReadFile(carrierAuthSnapshotPath(options))
	if err != nil {
		return fmt.Errorf("read active auth snapshot: %w", err)
	}
	if logf != nil {
		logf("WLT carrier auth ring event=replenish_started")
	}
	fresh, err := prewarmFreshCarrierAuthSnapshot(ctx, *cfg)
	if err != nil {
		_ = carrierengine.ImportAuthSnapshotJSON(active)
		if errors.Is(err, carriercommon.ErrHumanChallengeErrorLimit) {
			recordProviderRateLimit(options, logf)
		}
		return fmt.Errorf("create reserve auth identity: %w", err)
	}
	independent, err = carrierAuthSnapshotsIndependent(*cfg, active, fresh)
	if err != nil || !independent {
		_ = carrierengine.ImportAuthSnapshotJSON(active)
		if err != nil {
			return errors.New("validate reserve auth identity failed")
		}
		return errors.New("reserve auth identity is not independent")
	}
	if err := carrierengine.ImportAuthSnapshotJSON(fresh); err != nil {
		_ = carrierengine.ImportAuthSnapshotJSON(active)
		return fmt.Errorf("import reserve auth identity: %w", err)
	}
	validationClient, err := connectCarrierClientForStart(ctx, cfg, options.ConnectTimeout, logf)
	if err != nil {
		_ = carrierengine.ImportAuthSnapshotJSON(active)
		return fmt.Errorf("validate reserve carrier: %w", err)
	}
	defer validationClient.Stop()
	if err := promoteCarrierAuthSnapshot(*cfg); err != nil {
		_ = carrierengine.ImportAuthSnapshotJSON(active)
		return fmt.Errorf("promote reserve auth identity: %w", err)
	}
	validated, err := carrierengine.ExportAuthSnapshotJSON(*cfg)
	if err != nil {
		_ = carrierengine.ImportAuthSnapshotJSON(active)
		return fmt.Errorf("export reserve auth identity: %w", err)
	}
	if err := writeCarrierAuthReserve(options, validated); err != nil {
		_ = carrierengine.ImportAuthSnapshotJSON(active)
		return err
	}
	if err := carrierengine.ImportAuthSnapshotJSON(active); err != nil {
		return fmt.Errorf("restore active auth identity: %w", err)
	}
	if err := retireCarrierAuthQuarantine(options); err != nil {
		return err
	}
	if logf != nil {
		logf("WLT carrier auth ring event=reserve_validated")
	}
	return nil
}

func scheduleCarrierAuthReserve(ctx context.Context, cfg *carrierconfig.ClientConfig, options CarrierOptions, logf func(string, ...any)) {
	if cfg == nil || options.startup == nil || carrierAuthSnapshotPath(options) == "" {
		return
	}
	go func() {
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
			err := replenishCarrierAuthReserve(ctx, cfg, options, logf)
			if err == nil {
				ready, readyErr := carrierAuthReserveIndependent(cfg, options)
				if readyErr == nil && ready {
					return
				}
				if readyErr != nil {
					err = readyErr
				}
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
				logf("WLT carrier auth ring event=replenish_deferred retry_seconds=%d", int64(wait/time.Second))
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
			if retryDelay < authReserveRetryMaximum {
				retryDelay *= 2
				if retryDelay > authReserveRetryMaximum {
					retryDelay = authReserveRetryMaximum
				}
			}
		}
	}()
}
