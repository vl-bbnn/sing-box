package libbox

import "testing"

func TestCheckConfigAcceptsWLTServiceAfterOutbound(t *testing.T) {
	err := CheckConfig(`{
		"services": [
			{
				"type": "wlt",
				"tag": "wlt-turnable",
				"transport": "turnable",
				"turnable_config": "{}"
			}
		],
		"outbounds": [
			{
				"type": "wlt",
				"tag": "wlt-eu",
				"service": "wlt-turnable",
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
