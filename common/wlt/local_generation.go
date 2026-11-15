//go:build with_wlt

package wlt

import carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"

// LocalGenerationRetirement is a diagnostics-only cancellation cause shared
// with the carrier. It never grants retries without the service's exact lease
// identity and never changes raw transport failure counters.
type LocalGenerationRetirement = carriercommon.LocalGenerationRetirement
