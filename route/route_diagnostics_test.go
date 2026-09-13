package route

import (
	"testing"

	"github.com/sagernet/sing-box/common/wltdiagnostics"
)

func TestWLTRouteDomainCategoryIsBounded(t *testing.T) {
	tests := map[string]string{
		"instagram.com":                    "instagram-family",
		"i.instagram.com.":                 "instagram-family",
		"scontent-ams4-1.cdninstagram.com": "instagram-family",
		"graph.facebook.com":               "meta-family",
		"video-edge.fbcdn.net.":            "meta-family",
		"api.tiktokv.com":                  "tiktok-family",
		"API.TIKTOKV.EU.":                  "tiktok-family",
		"v16.tiktokcdn-eu.com":             "tiktok-family",
		"v16.byteoversea.net":              "tiktok-family",
		"v16.byteoversea.com.":             "tiktok-family",
		"p16-sign-sg.tiktokcdn.com":        "tiktok-family",
		"www.youtube.com":                  "youtube-family",
		"raw.githubusercontent.com":        "github-family",
		"example.com":                      "neutral-example",
		"eviltiktokv.eu.example":           "",
		"":                                 "",
	}
	for input, want := range tests {
		if got := wltdiagnostics.DomainCategory(input); got != want {
			t.Fatalf("category(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestWLTRouteOutboundClassNeverReturnsRawTag(t *testing.T) {
	tests := map[string]string{
		"vless-wlt-eu": "wlt-eu",
		"vless-wlt-ru": "wlt-ru",
		"direct":       "direct",
		"private-node": "other",
	}
	for input, want := range tests {
		if got := wltdiagnostics.OutboundClass(input); got != want {
			t.Fatalf("outboundClass(%q) = %q, want %q", input, got, want)
		}
	}
}
