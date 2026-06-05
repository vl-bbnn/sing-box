package common

import "math/rand"

// BrowserProfile holds browser identity headers
type BrowserProfile struct {
	UserAgent       string
	SecChUa         string
	SecChUaMobile   string
	SecChUaPlatform string
}

// browserProfiles represents a pool of realistic browser profiles
var browserProfiles = []BrowserProfile{
	{
		UserAgent:       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
		SecChUa:         `"Not:A-Brand";v="99", "Google Chrome";v="147", "Chromium";v="147"`,
		SecChUaMobile:   "?0",
		SecChUaPlatform: `"Windows"`,
	},
	{
		UserAgent:       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
		SecChUa:         `"Not:A-Brand";v="99", "Google Chrome";v="147", "Chromium";v="147"`,
		SecChUaMobile:   "?0",
		SecChUaPlatform: `"macOS"`,
	},
	{
		UserAgent:       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
		SecChUa:         `"Not:A-Brand";v="99", "Google Chrome";v="147", "Chromium";v="147"`,
		SecChUaMobile:   "?0",
		SecChUaPlatform: `"Linux"`,
	},
}

// RandomBrowserProfile returns a random browser identity profile
func RandomBrowserProfile() BrowserProfile {
	return browserProfiles[rand.Intn(len(browserProfiles))]
}
