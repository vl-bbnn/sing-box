//go:build with_wlt

package option

// WLTURLTestOptions is explicit opt-in; ordinary URL tests retain HEAD semantics.
type WLTURLTestOptions struct {
	WLTBodyProbe *WLTBodyProbeOptions `json:"wlt_body_probe,omitempty"`
}

type WLTBodyProbeOptions struct {
	URL                string `json:"url"`
	Bytes              int    `json:"bytes"`
	UnverifiedFallback string `json:"unverified_fallback"`
}
