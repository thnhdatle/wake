#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Mutation testing with gremlins on the core packages listed below.
#
# The gate is computed here from gremlins' JSON report, not from gremlins'
# own --threshold-efficacy: gremlins v0.6.0 reads that float flag through
# viper, which returns float flags as strings, so the threshold silently
# becomes 0 and a package with 0 killed / 0 lived still exits 0. A package
# fails when it yields no valid measurement (killed + lived == 0), when its
# efficacy killed / (killed + lived) is below MUTATION_THRESHOLD, or when so
# many mutants time out that the measurement is starved (see below).
#
# Timeouts. gremlins kills a mutant's `go test` after coefficient x the
# wall time of its baseline coverage run. A cached baseline (0.1-1 s) would
# be far too short, and each mutant run recompiles the package in a
# per-worker copy of the module while NumCPU workers compete for the CPU.
# GOFLAGS=-count=1 makes the baseline a real run, and the coefficient (30)
# keeps the per-mutant limit above the longest guard the unit tests use
# themselves (10 s, e.g. the stream fixture's "reader did not stop") plus a
# cold compile, so a TIMED OUT mutant is one that really hangs. --test-cpu is
# not used: gremlins v0.6.0 passes it as the single argument "-cpu N", which
# go test rejects.
#
# Build tags. Mutants run the unit suites only (no -tags integration). The
# redisl2 unit tests cover the Lua wrapper and reply decoding without a
# server, and cdcsource's template mapping and invalidation are covered by
# unit tests with in-process fakes (pkg/cache/internal/l2fake, l2test); the
# integration suites start Redis, Kafka and NATS with Testcontainers, which
# per mutant would take hours. Their coverage is measured by `make coverage`.
#
# Scope. Running gremlins on ./pkg/cache walks ./pkg/cache/... so its
# run excludes redisl2/ and cdcsource/ (measured on their own below) and the
# test doubles internal/l2fake/ and internal/l2/l2test/ (test helpers, not
# shipped behaviour: mutating them measures nothing about the cache).
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
. scripts/lib.sh
need jq

THRESHOLD=${MUTATION_THRESHOLD:-60}
COEFFICIENT=${MUTATION_TIMEOUT_COEFFICIENT:-30}
# A run where more than this share of covered mutants time out is starved
# (overloaded machine, coefficient too small), not a measurement.
MAX_TIMEOUT_PCT=${MUTATION_MAX_TIMEOUT_PCT:-25}
OUT=${MUTATION_OUT:-acceptance/logs/mutation}

# <package dir>|<exclude-files regexp, relative to the package dir; no commas>
packages=(
	'internal/domain/pgoutput|'
	'internal/domain/watermark|'
	'internal/app/stream|'
	'pkg/cache|^(redisl2|cdcsource|internal/l2fake|internal/l2/l2test)/'
	'pkg/cache/redisl2|'
	'pkg/cache/cdcsource|'
)

export GOFLAGS="${GOFLAGS:+$GOFLAGS }-count=1"
mkdir -p "$OUT"

ran=0
status=0
summary=()
for entry in "${packages[@]}"; do
	p=${entry%%|*}
	exclude=${entry#*|}
	if [ -z "$(find "$p" -maxdepth 1 -name '*.go' -print -quit 2>/dev/null)" ]; then
		echo "mutation: $p not found, skipping"
		continue
	fi
	ran=$((ran + 1))
	json="$OUT/$(echo "$p" | tr / _).json"
	rm -f "$json"
	args=(unleash --timeout-coefficient "$COEFFICIENT" --threshold-efficacy "$THRESHOLD" -o "$json")
	if [ -n "$exclude" ]; then
		args+=(--exclude-files "$exclude")
	fi
	echo "mutation: $p (gremlins ${args[*]} ./$p)"
	start=$(date +%s)
	if ! go tool gremlins "${args[@]}" "./$p"; then
		echo "mutation: $p: gremlins failed" >&2
		status=1
	fi
	secs=$(($(date +%s) - start))
	if [ ! -s "$json" ]; then
		echo "mutation: $p: no report written" >&2
		summary+=("$p: no report")
		status=1
		continue
	fi
	read -r killed lived notcov timedout notviable mcov < <(jq -r '
		def n(x): [.files[].mutations[] | select(.status == x)] | length;
		"\(n("KILLED")) \(n("LIVED")) \(n("NOT COVERED")) \(n("TIMED OUT")) \(n("NOT VIABLE")) \(.mutations_coverage // 0)"' "$json")
	verdict=$(awk -v k="$killed" -v l="$lived" -v to="$timedout" -v mcov="$mcov" \
		-v min="$THRESHOLD" -v maxto="$MAX_TIMEOUT_PCT" 'BEGIN {
		run = k + l + to
		if (k + l == 0) { printf "FAIL no valid measurement (killed + lived = 0)"; exit }
		eff = 100 * k / (k + l)
		line = sprintf("efficacy %.2f%%, mutator coverage %.2f%%", eff, mcov)
		if (run > 0 && 100 * to / run > maxto) {
			printf "FAIL %s; %d of %d run mutants timed out (> %s%%)", line, to, run, maxto; exit
		}
		if (eff < min) { printf "FAIL %s is below %s%%", line, min; exit }
		printf "ok %s (>= %s%%)", line, min
	}')
	line="$p: killed $killed, lived $lived, not covered $notcov, timed out $timedout, not viable $notviable; $verdict [${secs}s]"
	summary+=("$line")
	case $verdict in
	ok*) echo "mutation: $line" ;;
	*)
		echo "mutation: $line" >&2
		status=1
		;;
	esac
done

if [ "$ran" -eq 0 ]; then
	echo "mutation: no listed package found, nothing to mutate"
else
	echo "mutation: summary (threshold ${THRESHOLD}%, timeout coefficient ${COEFFICIENT}, reports in $OUT)"
	for s in "${summary[@]}"; do
		echo "  $s"
	done
fi
exit "$status"
