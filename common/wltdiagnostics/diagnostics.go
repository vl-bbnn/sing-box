package wltdiagnostics

import "strings"

func DomainCategory(domain string) string {
	domain = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
	for category, suffixes := range map[string][]string{
		"instagram-family": {"instagram.com", "cdninstagram.com"},
		"tiktok-family":    {"tiktok.com", "tiktokv.com", "tiktokv.eu", "tiktokcdn.com", "tiktokcdn-us.com", "tiktokcdn-eu.com", "byteoversea.com", "byteoversea.net", "ibytedtos.com", "ibyteimg.com", "byteimg.com", "bytefcdn-oversea.com", "muscdn.com", "musical.ly", "bytedance.com", "isnssdk.com", "snssdk.com", "pstatp.com"},
		"meta-family":      {"facebook.com", "facebook.net", "fbcdn.net", "fbsbx.com", "threads.net"},
		"youtube-family":   {"youtube.com", "youtu.be", "googlevideo.com", "ytimg.com", "youtube-nocookie.com"},
		"github-family":    {"github.com", "githubusercontent.com", "githubassets.com"},
		"neutral-example":  {"example.com"},
	} {
		for _, suffix := range suffixes {
			if domain == suffix || strings.HasSuffix(domain, "."+suffix) {
				return category
			}
		}
	}
	return ""
}

func OutboundClass(tag string) string {
	tag = strings.ToLower(tag)
	switch {
	case strings.Contains(tag, "wlt-eu"):
		return "wlt-eu"
	case strings.Contains(tag, "wlt-ru"):
		return "wlt-ru"
	case strings.Contains(tag, "direct"):
		return "direct"
	default:
		return "other"
	}
}
