# Acceptance suite

End-to-end tests that check Wake's guarantees against real Postgres, Kafka and
Redis: snapshots under write load, no loss across `kill -9`, schema changes
mid-stream, sink outages, per-key order, type fidelity, the webhook sink,
cache stampede protection, CDC invalidation latency, Redis outages, the stale
read window, the L1 hit path, the README quickstart and adding a table to a
running pipeline. The numbers in the README and in
[`bench/RESULTS.md`](../bench/RESULTS.md) come from these tests.

## Running

```sh
make acceptance                    # every criterion, reference-machine thresholds
make acceptance CI=1               # CI thresholds (smaller loads)
make acceptance ONLY=WAK-01,WAK-07 # a subset
```

The runner (`acceptance/cmd/runner`) builds the project and the image
(`make docker`), starts the stack in
[`deploy/docker-compose.yml`](../deploy/docker-compose.yml) (Postgres, Kafka,
NATS, Redis and Wake on 127.0.0.1 ports 5432, 9092, 4222, 6379 and 9093),
runs each criterion with no test cache and a fresh stack, writes
`acceptance/report.md`, and stops the stack with `docker compose down -v`.
Do not run other compose stacks on the same ports at the same time.

Requirements:

- Docker with the Compose plugin (Docker Desktop, OrbStack, Colima or a Linux
  engine). The reference-machine thresholds assume a Docker VM with 8 CPUs and
  10 GB of memory on a MacBook Pro M3 Pro with 36 GB (`machine` in
  [`criteria.yaml`](criteria.yaml));
- Go (the version in `go.mod`) and GNU make;
- about 45 to 60 minutes for a full run on the reference machine.

WAK-05 and WAK-10 require the reference machine for their `mac` thresholds: on
any other host run them with `CI=1`, otherwise they are reported as Blocked. On Linux, test receivers that Wake calls back through
`host.docker.internal` must listen on all interfaces:
`WAKE_ACCEPTANCE_RECEIVER_BIND=0.0.0.0`.

Other environment variables (optional): `ONLY` selects criteria like the flag;
`WAKE_ACCEPTANCE_CRITERIA` and `WAKE_ACCEPTANCE_LOGS` override the criteria
file and the log directory.

## Criteria

Each criterion has an id, one test named after it (`TestAcceptance_WAK01_…` or
`BenchmarkAcceptance_…`), a kind, and thresholds for two environments in
[`criteria.yaml`](criteria.yaml): `pass.mac` for the reference machine and
`pass.ci` for CI runners. Tests read the thresholds at run time; the
measurement definitions (M1 to M7) are at the top of the file.

| ID | Kind | What it checks |
|---|---|---|
| WAK-01 | functional | Snapshot of a 1M-row table under 1,000 tx/s of mixed writes; the final sink state equals Postgres |
| WAK-02 | functional | `kill -9` of Wake 20 times with large (spilled) and small transactions: nothing lost, duplicates only after the last confirmed LSN |
| WAK-03 | functional | ADD, DROP and RENAME COLUMN and ALTER TYPE mid-stream under writes, with a schema-change event per DDL |
| WAK-04 | functional | Kafka down for 3 minutes under 500 tx/s: the lag metric rises, the lag alert fires within 60 s, nothing is lost |
| WAK-05 | performance | Postgres to Kafka throughput and commit-to-ack latency, one slot, Wake native |
| WAK-06 | functional | Per-key order of 1M updates over 10k keys from 16 concurrent writers |
| WAK-07 | functional | Type fidelity of every type in [type-mapping.md](../docs/guides/type-mapping.md), including unchanged TOAST values |
| WAK-08 | functional | Webhook sink with 30% retryable failures and 5% slow responses: everything delivered, signed and in order |
| WAK-09 | functional | 1,000 concurrent `GetOrLoad` calls on a just-expired key across 3 cache instances call the loader once |
| WAK-10 | performance | CDC invalidation latency from commit until the key is gone from the L1 of 3 instances |
| WAK-11 | functional | Redis killed for 2 minutes under read load: stale values served, no caller errors, automatic recovery |
| WAK-12 | functional | 3-minute read/write race through CDC invalidation: no read older than commit + 2 s |
| WAK-13 | performance | `Cache[V].Get` on an L1 hit under 200 ns/op with zero allocations |
| WAK-14 | release | The README quickstart brings up Postgres, Kafka and Wake, and an insert appears in the topic within 5 minutes |
| WAK-15 | functional | A 500k-row table added to a running pipeline under 500 tx/s, `kill -9` mid-snapshot, resumes from the last acknowledged key |

WAK-02 takes `LOCK TABLE … IN EXCLUSIVE MODE` on the small-transaction tables
inside each large transaction, before it reads the LSN that marks it, so no
small transaction commits between that read and the large transaction's
COMMIT.

## The report

`report.md` starts with the version and the date of the run and the
environment (`mac` or `ci`). It has one row per criterion with the result, the
measured values and the thresholds. Per-criterion logs
(`acceptance/logs/<ID>.txt` and `<ID>.json`) are written locally during a run
and are not committed. Results are:

- **PASS**: every measured value meets its threshold;
- **FAIL**: a measured value misses its threshold; listed again under
  "Not met", with the explanation from [`not-met.yaml`](not-met.yaml) when
  there is one;
- **Skipped (environment)**: [`criteria.yaml`](criteria.yaml) sets the
  criterion's thresholds for this environment to `skip`. The runner, not the
  test, decides when a criterion is skipped;
- **Blocked (environment)**: a capability the criterion requires is missing
  (for example Docker), or the criterion cannot run as defined (for example
  reference-machine thresholds on another host, or a clock offset between the
  host and Postgres above the WAK-05 limit).

Before running, the runner also checks that every criterion has exactly one
test whose name carries its id, and that no acceptance test skips itself; any
mismatch fails the run.
