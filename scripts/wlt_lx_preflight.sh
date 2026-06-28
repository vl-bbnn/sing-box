#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

carrier_module="github.com/vl-bbnn/wlt-carrier"
case ",${GOPRIVATE:-}," in
*,"$carrier_module",*) ;;
*) export GOPRIVATE="${GOPRIVATE:+$GOPRIVATE,}$carrier_module" ;;
esac

run() {
	printf '==> %s\n' "$*"
	"$@"
}

lx_tags=""
lx_ldflags=""
if [[ -f Makefile.lx ]]; then
	lx_tags="$(make -f Makefile.lx lx-print-tags)"
	lx_ldflags="-checklinkname=0"
fi

printf '==> checking default build and tests\n'
run go test ./...

printf '==> checking WLT build and tests\n'
run go test -tags with_wlt ./...

if [[ -n "$lx_tags" ]]; then
	printf '==> checking LX build and tests\n'
	run go test -tags "$lx_tags" -ldflags "$lx_ldflags" ./...

	printf '==> checking LX+WLT build and tests\n'
	run go test -tags "$lx_tags,with_wlt" -ldflags "$lx_ldflags" ./...
fi

printf '==> checking default dependency graph stays WLT-free\n'
if go list -deps ./cmd/sing-box | rg -q 'github.com/(vl-bbnn/wlt-carrier|2b2n/wlt-carrier|theairblow/turnable|pion/)'; then
	echo "default sing-box dependency graph includes WLT carrier dependencies" >&2
	exit 1
fi

if [[ -n "$lx_tags" ]]; then
	printf '==> checking LX dependency graph stays WLT-free\n'
	if go list -tags "$lx_tags" -deps ./cmd/sing-box | rg -q 'github.com/(vl-bbnn/wlt-carrier|2b2n/wlt-carrier|theairblow/turnable|pion/)'; then
		echo "LX sing-box dependency graph includes WLT carrier dependencies without with_wlt" >&2
		exit 1
	fi
fi

printf '==> checking WLT dependency graph includes carrier runtime\n'
if ! go list -tags with_wlt -deps ./cmd/sing-box | rg -q "$carrier_module/pkg/config"; then
	echo "with_wlt dependency graph does not include carrier runtime config package" >&2
	exit 1
fi

if [[ -n "$lx_tags" ]]; then
	printf '==> checking LX+WLT dependency graph includes carrier runtime\n'
	if ! go list -tags "$lx_tags,with_wlt" -deps ./cmd/sing-box | rg -q "$carrier_module/pkg/config"; then
		echo "LX+with_wlt dependency graph does not include carrier runtime config package" >&2
		exit 1
	fi
fi

printf '==> checking carrier runtime source mode\n'
if rg -q '^replace github\.com/vl-bbnn/wlt-carrier => \.\./wlt-carrier$' go.mod; then
	if [[ "${WLT_REQUIRE_EXTERNAL_CARRIER:-${WLT_REQUIRE_EXTERNAL_TURNABLE:-0}}" == "1" ]]; then
		echo "local sibling wlt-carrier replace is still present" >&2
		exit 1
	fi
	if ! rg -q '^module github\.com/vl-bbnn/wlt-carrier$' ../wlt-carrier/go.mod; then
		echo "../wlt-carrier/go.mod does not expose $carrier_module" >&2
		exit 1
	fi
else
	if ! go list -m "$carrier_module" | rg -q "^$carrier_module v"; then
		echo "carrier runtime is not resolved as a versioned module" >&2
		exit 1
	fi
fi

printf '==> WLT/LX preflight ok\n'
