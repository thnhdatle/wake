#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs every examples/<name>/run.sh and compares stdout with
# examples/<name>/expected_output.txt when that file exists.
set -euo pipefail
cd "$(dirname "$0")/.."

shopt -s nullglob
dirs=(examples/*/)
if [ "${#dirs[@]}" -eq 0 ]; then
	echo "examples: none found"
	exit 0
fi

fail=0
for d in "${dirs[@]}"; do
	d=${d%/}
	if [ ! -x "$d/run.sh" ]; then
		echo "examples: $d has no executable run.sh" >&2
		fail=1
		continue
	fi
	echo "examples: $d"
	out=$("$d/run.sh") || {
		echo "examples: $d failed" >&2
		fail=1
		continue
	}
	if [ -f "$d/expected_output.txt" ] && ! diff <(echo "$out") "$d/expected_output.txt" >/dev/null; then
		echo "examples: $d output differs from expected_output.txt" >&2
		fail=1
	fi
done
exit "$fail"
