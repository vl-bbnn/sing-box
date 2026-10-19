//go:build with_wlt

package wlt

import (
	"errors"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
)

// Apply the explicit per-run option only to this in-memory client config.
// Saved profiles/auth are untouched; absent options retain legacy negotiation.
func applyDataHalfCloseOption(cfg *carrierconfig.ClientConfig, enabled bool) error {
	if cfg == nil {
		return errors.New("missing carrier config")
	}
	if enabled || cfg.DataHalfClose {
		if cfg.LowLatencyControl {
			return errors.New("bilateral DATA close requires sequenced control")
		}
		cfg.DataHalfClose = true
	}
	return nil
}

// Apply an explicit hold only to this runtime config. Default zero leaves
// the saved/embedded negotiation unchanged; no profile/auth is rewritten.
func applySessionRetentionOption(cfg *carrierconfig.ClientConfig, milliseconds int) error {
	if cfg == nil {
		return errors.New("missing carrier config")
	}
	if milliseconds == 0 {
		return nil
	}
	if cfg.Type != "relay" {
		return errors.New("session retention requires relay transport")
	}
	candidate := *cfg
	candidate.SessionRetentionMillis = milliseconds
	if err := candidate.ValidateSessionRetention(); err != nil {
		return err
	}
	cfg.SessionRetentionMillis = milliseconds
	return nil
}
