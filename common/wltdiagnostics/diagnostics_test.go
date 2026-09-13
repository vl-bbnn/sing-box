package wltdiagnostics

import "testing"

func TestDomainCategoryBoundaries(t *testing.T) {
	tests := map[string]string{
		"TIKTOKV.EU.":            "tiktok-family",
		"a.tiktokcdn-eu.com":     "tiktok-family",
		"a.byteoversea.net":      "tiktok-family",
		"www.youtube.com":        "youtube-family",
		"api.github.com":         "github-family",
		"example.com":            "neutral-example",
		"sub.example.com.":       "neutral-example",
		"eviltiktokv.eu.example": "",
		"notgithub.com.example":  "",
		"":                       "",
	}
	for input, want := range tests {
		if got := DomainCategory(input); got != want {
			t.Errorf("DomainCategory(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestOutboundClassIsBounded(t *testing.T) {
	for input, want := range map[string]string{
		"private-WLT-EU": "wlt-eu",
		"private-wlt-ru": "wlt-ru",
		"direct":         "direct",
		"secret.example": "other",
	} {
		if got := OutboundClass(input); got != want {
			t.Errorf("OutboundClass(%q) = %q, want %q", input, got, want)
		}
	}
}
