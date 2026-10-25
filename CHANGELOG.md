# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-10-25

### Added

- Interactive demo page in `docs/demo`.

### Removed

- Acceptance, benchmark and mutation-testing code and their Makefile targets; benchmark
  results stay in the README.

## [0.1.0] - 2026-10-21

First public release.

### Added

- **Capture**: `wake run` streams changes from Postgres 14 to 17 through one logical
  replication slot (`pgoutput`), with managed or existing publications, partitioned tables
  through their root, opt-in keyless tables, table name mapping, and tables added or removed
  at runtime (SIGHUP or periodic refresh). Large transactions spill to disk.
- **Delivery**: at-least-once delivery in commit order per key; Wake confirms WAL to Postgres
  only after every sink acknowledged it, retries with capped backoff instead of dropping, and
  keeps standby status updates going while reading is paused.
- **Sinks**: Kafka (idempotent producer, tombstones, TLS, SASL/SCRAM), NATS JetStream
  (deduplication by event id), HMAC-signed webhooks with per-key lanes and an optional
  dead-letter directory (verifier examples in Go, Python and Node), and stdout.
- **Snapshots**: a consistent initial snapshot from the slot's exported snapshot with parallel
  table workers and crash resume, and snapshots of added tables or on request
  (`wake snapshot table`) through watermarks, without pausing the stream.
- **Events**: CDC event format v1 with deterministic ids, a total order (`seq`), unchanged
  TOAST columns, exact large integers, per-table schema versions and schema-change events;
  JSON Schemas in `api/schema` and Go types with a strict decoder in `pkg/event`.
- **Operations**: `wake check`, `wake status`, `wake config validate` and
  `wake config print --redact`, `wake slot drop`, `wake publication drop`, `wake healthcheck`
  and `wake version`; an admin HTTP API (`/healthz`, `/readyz`, `/metrics`, `/v1/status`,
  `/v1/version`, described in `api/openapi.yaml`); slot lag, WAL status and ack-age alerts
  with an optional alert webhook; idle-source heartbeats; stable error codes; Prometheus
  alert rules and a Grafana dashboard.
- **Cache library** `pkg/cache`: a two-tier read-through cache (in-process L1, Redis L2 for
  single node, Sentinel or Cluster) with per-key generations, stampede protection,
  stale-if-error, degraded mode while Redis is down, and CDC-driven invalidation from Kafka
  or NATS (`pkg/cache/cdcsource`, also available as `wake cache-invalidator`).
- **Packaging**: static binaries for Linux and macOS (amd64, arm64) with checksums and SBOMs,
  a multi-platform distroless image `ghcr.io/thnhdatle/wake`, a Docker Compose quickstart and
  Kubernetes manifests (single-replica StatefulSet).
- Guides in `docs/guides`.

### Known limitations

- Postgres to Kafka throughput: the target of at least 5,000 events/s was not met in the
  v0.1.0 measurement (4,999.7 events/s with a p99 latency of 12 ms at an offered
  5,000 tx/s, no backlog); headroom above that rate was not measured. See the README.
- One Wake instance per source, no automatic failover; one stuck sink holds back all sinks.
- Features not in v0.1 (for example outbox mode, Avro and Protobuf encodings, multiple slots
  and high availability) are listed in [ROADMAP.md](https://github.com/thnhdatle/wake/blob/main/ROADMAP.md).

[Unreleased]: https://github.com/thnhdatle/wake/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/thnhdatle/wake/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/thnhdatle/wake/releases/tag/v0.1.0
