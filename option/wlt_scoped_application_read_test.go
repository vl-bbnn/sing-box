//go:build with_wlt

package option_test

import (
	"context"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"strings"
	"testing"
)

func TestWLTScopedApplicationReadOptionIsExplicitAndValidated(t *testing.T) {
	for _, tc := range []struct {
		name, extra     string
		selected, valid bool
	}{
		{"legacy-absent", "", false, true}, {"explicit-false", `,"scoped_application_read_retirement":false`, false, true}, {"selected-with-half-close", `,"data_half_close":true,"scoped_application_read_retirement":true`, true, true}, {"selected-without-half-close", `,"scoped_application_read_retirement":true`, true, false}, {"wrong-type", `,"data_half_close":true,"scoped_application_read_retirement":"true"`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts option.WLTServiceOptions
			e := opts.UnmarshalJSONContext(context.Background(), []byte(`{"carrier_config":"{}"`+tc.extra+`}`))
			if (e == nil) != tc.valid {
				t.Fatal("option validation", e)
			}
			if !tc.valid {
				return
			}
			if opts.ScopedApplicationReadRetirement != tc.selected {
				t.Fatal("selection changed")
			}
			raw, e := json.Marshal(opts)
			if e != nil {
				t.Fatal(e)
			}
			if strings.Contains(string(raw), "scoped_application_read_retirement") != tc.selected {
				t.Fatal("legacy profile output changed", string(raw))
			}
		})
	}
}
