#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs every Fuzz target for FUZZTIME (default 30s).
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
. scripts/lib.sh

FUZZTIME=${FUZZTIME:-30s}

if ! has_pkgs; then
	echo "fuzz: no Go packages, nothing to fuzz"
	exit 0
fi

count=0
while read -r pkg; do
	targets=$(go test -list '^Fuzz' "$pkg" 2>/dev/null | grep '^Fuzz' || true)
	for t in $targets; do
		count=$((count + 1))
		echo "fuzz: $pkg $t ($FUZZTIME)"
		go test -run='^$' -fuzz="^${t}\$" -fuzztime="$FUZZTIME" "$pkg"
	done
done < <(go list ./...)

if [ "$count" -eq 0 ]; then
	echo "fuzz: no Fuzz targets found"
fi
