package rule

import (
	"context"
	"testing"
)

func TestRemoteRuleSetUpdaterStopsWhenTickerWasNotInitialized(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ruleSet := &RemoteRuleSet{ctx: ctx}

	// A reload can publish PostStart for a partially initialized rule-set. The
	// updater must fail closed rather than panic while dereferencing a nil
	// ticker.
	ruleSet.loopUpdate()
}
