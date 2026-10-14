package wltdiagnostics

import "testing"

func TestDomainCategoryBoundaries(t *testing.T) {
	tests := map[string]string{
		"TIKTOKV.EU.":            "tiktok-family",
		"a.tiktokcdn-eu.com":     "tiktok-family",
		"a.byteoversea.net":      "tiktok-family",
		"www.youtube.com":        "youtube-family",
		"twitch.tv":              "twitch-family",
		"WWW.TWITCH.TV.":         "twitch-family",
		"ttvnw.net":              "twitch-family",
		"video.ttvnw.net":        "twitch-family",
		"jtvnw.net":              "twitch-family",
		"static.jtvnw.net":       "twitch-family",
		"twitchcdn.net":          "twitch-family",
		"assets.twitchcdn.net":   "twitch-family",
		"api.github.com":         "github-family",
		"example.com":            "neutral-example",
		"sub.example.com.":       "neutral-example",
		"eviltiktokv.eu.example": "",
		"notgithub.com.example":  "",
		"faketwitch.tv":          "",
		"video.ttvnw.net.evil":   "",
		"notjtvnw.net":           "",
		"twitchcdn.net.evil":     "",
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
