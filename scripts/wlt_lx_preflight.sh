#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

run() {
	printf '==> %s\n' "$*"
	"$@"
}

printf '==> checking default build and tests\n'
run go test ./...

printf '==> checking WLT build and tests\n'
run go test -tags with_wlt ./...

printf '==> checking default dependency graph stays WLT-free\n'
if go list -deps ./cmd/sing-box | rg -q 'github.com/(2b2n/wlt-carrier|theairblow/turnable|pion/)'; then
	echo "default sing-box dependency graph includes WLT carrier dependencies" >&2
	exit 1
fi

printf '==> checking WLT dependency graph includes carrier runtime\n'
if ! go list -tags with_wlt -deps ./cmd/sing-box | rg -q 'github.com/2b2n/wlt-carrier/pkg/config'; then
	echo "with_wlt dependency graph does not include carrier runtime config package" >&2
	exit 1
fi

printf '==> checking carrier runtime source mode\n'
if [[ "${WLT_REQUIRE_EXTERNAL_CARRIER:-${WLT_REQUIRE_EXTERNAL_TURNABLE:-0}}" == "1" ]]; then
	if rg -q '^replace github\.com/2b2n/wlt-carrier => \.\./wlt-carrier$' go.mod; then
		echo "local sibling wlt-carrier replace is still present" >&2
		exit 1
	fi
else
	if ! rg -q '^replace github\.com/2b2n/wlt-carrier => \.\./wlt-carrier$' go.mod; then
		echo "local carrier runtime replace is missing; set WLT_REQUIRE_EXTERNAL_CARRIER=1 for an external module" >&2
		exit 1
	fi
	if ! rg -q '^module github\.com/2b2n/wlt-carrier$' ../wlt-carrier/go.mod; then
		echo "../wlt-carrier/go.mod does not expose github.com/2b2n/wlt-carrier" >&2
		exit 1
	fi
fi

printf '==> WLT/LX preflight ok\n'
