//go:build with_wlt

package libbox

import "testing"

func TestCheckConfigAcceptsWLTServiceAfterOutbound(t *testing.T) {
	err := CheckConfig(`{
		"services": [
			{
				"type": "wlt",
				"tag": "wlt-carrier",
				"transport": "wlt",
				"carrier_config": "{}"
			}
		],
		"outbounds": [
			{
				"type": "wlt",
				"tag": "wlt-eu",
				"service": "wlt-carrier",
				"route": "eu",
				"network": "tcp"
			}
		],
		"route": {
			"final": "wlt-eu"
		}
	}`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPrewarmWLTAuthRejectsMissingCarrierConfig(t *testing.T) {
	err := PrewarmWLTAuth("", "", "")
	if err == nil {
		t.Fatal("expected missing carrier config error")
	}
}
