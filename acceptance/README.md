# Acceptance suite

Run the end-to-end tests that check Wake's guarantees against real Postgres, Kafka and Redis.
The numbers in the README and [`bench/RESULTS.md`](../bench/RESULTS.md) come from these tests.

## Running

```sh
make acceptance                    # every criterion, reference-machine thresholds
make acceptance CI=1               # CI thresholds (smaller loads)
make acceptance ONLY=WAK-01,WAK-07 # a subset
```

The runner starts [`deploy/docker-compose.yml`](../deploy/docker-compose.yml) on 127.0.0.1
ports 5432, 9092, 4222, 6379 and 9093. Each criterion gets a fresh stack. Results go to
`acceptance/report.md`. Do not run other compose stacks on those ports at the same time.

You need:

- Docker with the Compose plugin;
- Go (the version in `go.mod`) and GNU make;
- 45 to 60 minutes for a full run.

Reference-machine thresholds assume a Docker VM with 8 CPUs and 10 GB on a MacBook Pro M3 Pro
with 36 GB (`machine` in [`criteria.yaml`](criteria.yaml)). On any other host, run WAK-05 and
WAK-10 with `CI=1`, or they are Blocked.

| Variable | Effect |
|---|---|
| `ONLY` | criteria to run, like the flag |
| `WAKE_ACCEPTANCE_RECEIVER_BIND` | set `0.0.0.0` on Linux |
| `WAKE_ACCEPTANCE_CRITERIA` | other criteria file |
| `WAKE_ACCEPTANCE_LOGS` | other log directory |

## Criteria

Thresholds are in [`criteria.yaml`](criteria.yaml): `pass.mac` for the reference machine,
`pass.ci` for CI. Each criterion has one test named after it (`TestAcceptance_WAK01_…` or
`BenchmarkAcceptance_…`).

| ID | Kind | What it checks |
|---|---|---|
| WAK-01 | functional | 1M-row snapshot under 1,000 tx/s; sink equals Postgres |
| WAK-02 | functional | 20 × `kill -9`, large and small transactions: no loss, duplicates only after the last confirmed LSN |
| WAK-03 | functional | ADD, DROP, RENAME COLUMN and ALTER TYPE mid-stream; one schema event per DDL |
| WAK-04 | functional | Kafka down 3 minutes at 500 tx/s: lag rises, alert within 60 s, no loss |
| WAK-05 | performance | Postgres to Kafka throughput and commit-to-ack latency |
| WAK-06 | functional | Per-key order, 1M updates, 10k keys, 16 writers |
| WAK-07 | functional | Every type in [type-mapping.md](../docs/guides/type-mapping.md), including unchanged TOAST |
| WAK-08 | functional | Webhook with 30% retryable failures and 5% slow: all delivered, signed, in order |
| WAK-09 | functional | 1,000 concurrent `GetOrLoad` on an expired key, 3 instances: loader called once |
| WAK-10 | performance | CDC invalidation latency to L1 of 3 instances |
| WAK-11 | functional | Redis down 2 minutes: stale values, no caller errors, recovery |
| WAK-12 | functional | 3-minute race: no read older than commit + 2 s |
| WAK-13 | performance | `Cache[V].Get` L1 hit under 200 ns/op, zero allocations |
| WAK-14 | release | README quickstart: insert appears in the topic within 5 minutes |
| WAK-15 | functional | 500k-row table added under 500 tx/s, `kill -9` mid-snapshot, resumes from last acknowledged key |

## The report

`report.md` has the version, date and environment (`mac` or `ci`), then one row per criterion:
result, measured values, thresholds. Logs go to `acceptance/logs/<ID>.txt` and `<ID>.json`
and are not committed.

| Result | Meaning |
|---|---|
| **PASS** | every value meets its threshold |
| **FAIL** | a value misses its threshold; listed under "Not met" with the note from [`not-met.yaml`](not-met.yaml) |
| **Skipped (environment)** | `criteria.yaml` sets this environment's thresholds to `skip` |
| **Blocked (environment)** | a requirement is missing (Docker, reference machine, clock offset to Postgres above the WAK-05 limit) |

The run fails if a criterion does not have exactly one test, or if a test skips itself.
