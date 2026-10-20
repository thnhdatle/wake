# Benchmark results

Reproduce with `make bench` (then `make down` to remove the compose dependencies). For the
performance criteria Wake runs natively on the host against the compose dependencies; the unit
benchmarks run with `go test -bench . -benchmem -count 3 ./...`. The WAK-05, WAK-10 and WAK-13
measurements are defined in `acceptance/criteria.yaml` (M6 and M7 for the load shape and the warm-up);
thresholds below are the Mac ones.

## WAK-05 Throughput and latency Postgres to Kafka, single slot, Wake native — 2026-10-19
Machine: Mac15,6, Apple M3 Pro, 36 GB, macOS 26.1, Docker VM 8 CPU / 9.7 GB, Go 1.27.2, Wake v0.1.0 pre-release build

| Metric | Median | Min | Max | Threshold | Pass |
|---|---|---|---|---|---|
| events/s | 4999.70 | 4999.45 | 5000.05 | >= 5000 | FAIL |
| latency p99 (ms) | 12 | 12 | 12 | < 200 | PASS |
| latency p50 (ms) | 6 | - | - | - | - |

Median, min and max over 3 runs.

Result: FAIL

Not met: In this run the offered load was 5,000 tx/s (one event each), equal to the throughput threshold, so a Wake that keeps up acks about 600,000 events in the 120 s window, give or take the events whose latency carries them across the window edges. Measured in 3 runs with Wake native: 4999.70, 5000.05 and 4999.45 events/s (599,964, 600,006 and 599,934 events acked in the window), median 4999.70 against ">= 5000"; latency p99 12 ms in every run (threshold 200 ms), p50 6 ms; Wake confirmed the final LSN about 0.2 s after the load ended (208 ms in run 1), so no backlog built up. The result shows that Wake kept up with 5,000 events/s; headroom above that rate was not measured.

Details: Wake native, compose Postgres and Kafka; 4 tables, 200-byte rows, 32 loadgen connections, 6-partition topics (LogAppendTime), default sink batching; offered 5000 tx/s (single-row INSERT), warm-up 20s, measured 2m0s; medians over 3 runs: 4999.7 events/s, p99 12.0 ms; run 1: offset 880µs, acked in window 599964, 4999.7 events/s, latency p50 6.0 ms p99 12.0 ms max 37.0 ms, records 700000, drain 208ms, load seed=6 offered=700000 committed=700000 errors=0 dropped=0 achieved=4999.8 tx/s max_lag=23ms; run 2: offset 100µs, acked in window 600006, 5000.1 events/s, latency p50 6.0 ms p99 12.0 ms max 45....

## WAK-10 CDC invalidation latency from commit to key gone from L1 of all 3 instances — 2026-10-19
Machine: Mac15,6, Apple M3 Pro, 36 GB, macOS 26.1, Docker VM 8 CPU / 9.7 GB, Go 1.27.2, Wake v0.1.0 pre-release build

| Metric | Median | Min | Max | Threshold | Pass |
|---|---|---|---|---|---|
| latency p99 (ms) | 18.48 | 18.48 | 18.48 | < 500 | PASS |
| latency p50 (ms) | 12.84 | 12.84 | 12.84 | - | - |
| latency max (ms) | 56.61 | 56.61 | 56.61 | - | - |

One run of 120 s, as the criterion fixes.

Result: PASS

Details: Wake native; Postgres -> Wake -> Kafka (3 partitions) -> cdcsource invalidator (own cache instance) -> Redis + bus -> L1 of 3 instances (TTL 10m0s, own Redis client each); 1000 cached keys; M7: the probe update was observed by every instance 22ms after its commit, measurement started there; writers seed=10 offered=12000 committed=12000 errors=0 dropped=0 achieved=100.0 tx/s max_lag=20ms (UPDATE ver = ver + 1, keys round-robin so each key changes every 10s, 8 conns); latency = M1 (host clock when COMMIT returned) to the last instance's first Get miss (L1 and L2 gone, polled every 1ms per instan...

## WAK-13 L1 hit path Cache[V].Get under 200 ns/op with zero allocations — 2026-10-19
Machine: Mac15,6, Apple M3 Pro, 36 GB, macOS 26.1, Docker VM 8 CPU / 9.7 GB, Go 1.27.2, Wake v0.1.0 pre-release build

| Metric | Median | Min | Max | Threshold | Pass |
|---|---|---|---|---|---|
| ns/op | 55.84 | 55.65 | 57.50 | < 200 | PASS |
| allocs/op | 0 | 0 | 0 | <= 0 | PASS |

Median, min and max over 3 runs.

Result: PASS

Details: Cache[V].Get on an L1 hit, one goroutine, metrics on a registry, msgpack codec, 64-byte value, string key, no L2; 3 timed runs of at least 1s each, median 55.84 ns/op (min 55.65, max 57.50), allocs/op max over runs 0; runs: [25600000 calls 55.65 ns/op 0 allocs/op 25600000 calls 57.50 ns/op 0 allocs/op 25600000 calls 55.84 ns/op 0 allocs/op]

## Unit benchmarks — 2026-10-19
Machine: Mac15,6, Apple M3 Pro, 36 GB, macOS 26.1, Docker VM 8 CPU / 9.7 GB, Go 1.27.2, Wake v0.1.0 pre-release build

| Benchmark | Package | Median ns/op | Min | Max | B/op | allocs/op |
|---|---|---|---|---|---|---|
| BenchmarkRecords_Build/insert | github.com/thnhdatle/wake/internal/adapter/kafkasink | 181.40 | 181 | 191.40 | 648 | 9 |
| BenchmarkRecords_Build/delete_tombstone | github.com/thnhdatle/wake/internal/adapter/kafkasink | 221.90 | 221.30 | 227.10 | 856 | 12 |
| BenchmarkReader_Tx | github.com/thnhdatle/wake/internal/app/stream | 753.50 | 749.50 | 770.80 | 1616 | 11 |
| BenchmarkEvent_AppendJSON/narrow_insert | github.com/thnhdatle/wake/internal/domain/change | 122 | 120.10 | 124.20 | 0 | 0 |
| BenchmarkEvent_AppendJSON/wide_update_toast | github.com/thnhdatle/wake/internal/domain/change | 207.30 | 206.40 | 213 | 0 | 0 |
| BenchmarkAppendString_Payload | github.com/thnhdatle/wake/internal/domain/change | 34.68 | 34.02 | 34.73 | 0 | 0 |
| BenchmarkDecoder_Decode/narrow_insert | github.com/thnhdatle/wake/internal/domain/pgoutput | 37.33 | 36.44 | 37.51 | 96 | 2 |
| BenchmarkDecoder_Decode/wide_insert | github.com/thnhdatle/wake/internal/domain/pgoutput | 118.10 | 117.80 | 119.20 | 352 | 2 |
| BenchmarkDecoder_Decode/wide_update_toast | github.com/thnhdatle/wake/internal/domain/pgoutput | 128.40 | 128.30 | 128.50 | 384 | 2 |
| BenchmarkDecoder_RowImages/narrow_insert | github.com/thnhdatle/wake/internal/domain/pgoutput | 136.20 | 135.80 | 139.30 | 368 | 2 |
| BenchmarkDecoder_RowImages/wide_insert | github.com/thnhdatle/wake/internal/domain/pgoutput | 509.70 | 508.20 | 522.30 | 1152 | 2 |
| BenchmarkDecoder_RowImages/wide_update_toast | github.com/thnhdatle/wake/internal/domain/pgoutput | 453.90 | 453.40 | 454.90 | 816 | 3 |
| BenchmarkRecord_NewEventRecord/narrow_insert | github.com/thnhdatle/wake/internal/domain/pipeline | 276.20 | 271.70 | 281.60 | 776 | 3 |
| BenchmarkRecord_NewEventRecord/wide_update_toast | github.com/thnhdatle/wake/internal/domain/pipeline | 433.80 | 431.80 | 436.60 | 1680 | 4 |
| BenchmarkCache_GetL1Hit | github.com/thnhdatle/wake/pkg/cache | 55.51 | 54.93 | 55.75 | 0 | 0 |
| BenchmarkCache_Get | github.com/thnhdatle/wake/pkg/cache/l1 | 41.14 | 40.24 | 41.16 | 0 | 0 |
