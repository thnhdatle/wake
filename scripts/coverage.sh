#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Merged unit + integration coverage with thresholds.
# Whole repo >= 80%, internal/domain + internal/app >= 90%.
# Excluded: cmd/, bench/, acceptance/, examples/, *_gen.go, *.pb.go.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
. scripts/lib.sh

MODULE=$(go list -m)
MIN_ALL=${MIN_ALL:-80}
MIN_CORE=${MIN_CORE:-90}
PG=${PG:-17}
export WAKE_TEST_PG=$PG

if ! has_pkgs; then
	echo "coverage: no Go packages, nothing to measure"
	exit 0
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# The race detector needs cgo outside macOS; release builds stay CGO-free.
CGO_ENABLED=1 go test -race -count=1 -covermode=atomic -coverpkg=./... -coverprofile="$tmp/unit.out" ./...

profiles=("$tmp/unit.out")
if grep -rlE '^//go:build .*integration' --include='*_test.go' . >/dev/null 2>&1; then
	go test -tags integration -count=1 -covermode=atomic -coverpkg=./... \
		-coverprofile="$tmp/integration.out" ./...
	profiles+=("$tmp/integration.out")
else
	echo "coverage: no integration tests, unit profile only"
fi

# Merge: a block counts as covered when any profile covers it. Drop excluded files.
awk -v mod="$MODULE" '
	FNR == 1 { next }
	{
		key = $1 " " $2
		if (!(key in cnt) || $3 > cnt[key]) cnt[key] = $3
	}
	END {
		print "mode: set"
		for (k in cnt) print k " " cnt[k]
	}
' "${profiles[@]}" |
	awk -v mod="$MODULE" '
		NR == 1 { print; next }
		{
			f = $1
			sub(/:.*/, "", f)
			rel = f
			sub("^" mod "/", "", rel)
			if (rel ~ /^(cmd|bench|acceptance|examples|research)\//) next
			if (rel ~ /(_gen\.go|\.pb\.go)$/) next
			print
		}
	' >"$tmp/merged.out"

pct() { # pct <awk-regex on relative path or empty>
	awk -v mod="$MODULE" -v re="$1" '
		NR == 1 { next }
		{
			f = $1
			sub(/:.*/, "", f)
			sub("^" mod "/", "", f)
			if (re != "" && f !~ re) next
			total += $2
			if ($3 > 0) covered += $2
		}
		END {
			if (total == 0) { print "none"; exit }
			printf "%.1f", 100 * covered / total
		}
	' "$tmp/merged.out"
}

fail=0
check() { # check <label> <value> <min>
	if [ "$2" = none ]; then
		echo "coverage: $1: no statements to measure"
	elif awk -v v="$2" -v m="$3" 'BEGIN { exit !(v + 0 >= m + 0) }'; then
		echo "coverage: $1: $2% (>= $3%)"
	else
		echo "coverage: $1: $2% is below $3%" >&2
		fail=1
	fi
}

check "whole repo" "$(pct '')" "$MIN_ALL"
check "internal/domain + internal/app" "$(pct '^internal/(domain|app)/')" "$MIN_CORE"

cp "$tmp/merged.out" coverage-merged.out
exit "$fail"
