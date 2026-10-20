#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs every benchmark and writes bench/RESULTS.md.
#   1. unit benchmarks: go test -bench . -benchmem -count 3 ./...  -> out/bench/unit.txt
#   2. performance acceptance benchmarks WAK-05, WAK-10, WAK-13 (Wake native against the compose
#      dependencies, which the tests start themselves) -> acceptance/logs/<ID>.txt and .json
#   3. go run ./bench/cmd/results -> bench/RESULTS.md
# A failing acceptance benchmark does not stop the others; the script exits non-zero at the end.
# The compose stack is left running: `make down` follows.
# Environment: BENCH_ONLY=unit|acceptance runs one part; BENCH_SKIP_RUN=1 only regenerates
# bench/RESULTS.md from the existing outputs; BENCH_LOGS=<dir> reads the acceptance JSONs from
# <dir> instead of acceptance/logs (for example acceptance/logs/mac after a CI=1 run);
# RELEASE=<version> labels the results (the Makefile passes its RELEASE).
set -euo pipefail
cd "$(dirname "$0")/.."

only=${BENCH_ONLY:-}
case "$only" in
'' | unit | acceptance) ;;
*)
	echo "bench: BENCH_ONLY must be unit or acceptance, got '$only'" >&2
	exit 2
	;;
esac

mkdir -p bench out/bench acceptance/logs
fail=0

run_unit() {
	echo "bench: unit benchmarks -> out/bench/unit.txt"
	go test -run '^$' -bench . -benchmem -count 3 ./... >out/bench/unit.txt
}

# run_acceptance <ID> <benchmark name>
run_acceptance() {
	local id=$1 name=$2
	echo "bench: $id $name -> acceptance/logs/$id.txt"
	if ! go test -tags acceptance -count=1 -timeout 120m -run '^$' -bench "^${name}\$" -benchtime 1x \
		./acceptance/ >"acceptance/logs/$id.txt" 2>&1; then
		echo "bench: $id FAILED (see acceptance/logs/$id.txt)" >&2
		fail=1
	fi
}

if [ "${BENCH_SKIP_RUN:-}" != 1 ]; then
	if [ "$only" != acceptance ]; then
		run_unit
	fi
	if [ "$only" != unit ]; then
		run_acceptance WAK-05 BenchmarkAcceptance_WAK05_Throughput
		run_acceptance WAK-10 BenchmarkAcceptance_WAK10_InvalidationLatency
		run_acceptance WAK-13 BenchmarkAcceptance_WAK13_L1Get
	fi
fi

go run ./bench/cmd/results -version "${RELEASE:-dev}" -out bench/RESULTS.md -logs "${BENCH_LOGS:-acceptance/logs}"
exit "$fail"
