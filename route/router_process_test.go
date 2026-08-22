package route

import "testing"

func TestProcessLookupRequired(t *testing.T) {
	if processLookupRequired(false, nil) {
		t.Fatal("a profile without process rules must not enable connection-owner lookup")
	}
	if !processLookupRequired(true, nil) {
		t.Fatal("an explicit find_process option must enable connection-owner lookup")
	}
}
