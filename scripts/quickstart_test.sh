#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Clones the repository (HEAD) into a temp dir and runs the README quickstart
# verbatim: the first ```bash block under "## Quickstart". The quickstart uses
# the compose project "wake", so a running stack of that project is removed
# first and the quickstart's stack is removed at the end.
# Prints "quickstart-test: duration_s=<s>" for the README commands and
# "quickstart-test: PASS" when the consumer printed the inserted row as a
# CDC create event. QUICKSTART_TIMEOUT (default 300) bounds the README
# commands in seconds.
set -euo pipefail
cd "$(dirname "$0")/.."
repo=$(pwd)
limit=${QUICKSTART_TIMEOUT:-300}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wake-quickstart.XXXXXX")
clone="$tmp/wake"
cleanup() {
	(cd "$clone" 2>/dev/null && docker compose -f deploy/docker-compose.yml down -v >/dev/null 2>&1) || true
	rm -rf "$tmp"
}
trap cleanup EXIT

git clone --quiet "$repo" "$clone"
cd "$clone"

# The bash block that follows the "## Quickstart" heading.
awk '/^## Quickstart/ { q = 1; next }
	q && /^## / { exit }
	q && /^```bash$/ { b = 1; next }
	b && /^```$/ { exit }
	b { print }' README.md >"$tmp/quickstart.sh"
if [ ! -s "$tmp/quickstart.sh" ]; then
	echo "quickstart-test: no bash block under '## Quickstart' in README.md" >&2
	exit 1
fi
echo "quickstart-test: README commands:"
sed 's/^/  /' "$tmp/quickstart.sh"

docker compose -f deploy/docker-compose.yml down -v >/dev/null 2>&1 || true

start=$(date +%s)
bash -euo pipefail "$tmp/quickstart.sh" >"$tmp/out.txt" 2>&1 &
pid=$!
# The watchdog's output is detached so that its sleep, killed below, never
# holds the caller's output pipe open.
(
	sleep "$limit"
	kill "$pid" 2>/dev/null
) >/dev/null 2>&1 &
watchdog=$!
status=0
wait "$pid" || status=$?
pkill -P "$watchdog" 2>/dev/null || true
kill "$watchdog" 2>/dev/null || true
wait "$watchdog" 2>/dev/null || true
duration=$(($(date +%s) - start))

echo "quickstart-test: output:"
sed 's/^/  /' "$tmp/out.txt"
echo "quickstart-test: duration_s=$duration exit=$status"
if [ "$status" -ne 0 ]; then
	echo "quickstart-test: FAIL: README commands exited $status (limit ${limit}s)" >&2
	exit 1
fi
if ! grep -q '"source":{"db":"app","schema":"public","table":"users"}' "$tmp/out.txt" ||
	! grep -q '"op":"c"' "$tmp/out.txt" ||
	! grep -q '"after":{"id":1,"email":"a@example.com"}' "$tmp/out.txt"; then
	echo "quickstart-test: FAIL: the consumer did not print the inserted row as a create event" >&2
	exit 1
fi
echo "quickstart-test: PASS"
