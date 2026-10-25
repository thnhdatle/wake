# Wake

Wake streams Postgres changes to Kafka, NATS or webhooks in commit order, and keeps a two-level cache in sync with them.

[![CI](https://github.com/thnhdatle/wake/actions/workflows/ci.yml/badge.svg)](https://github.com/thnhdatle/wake/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/thnhdatle/wake)](https://github.com/thnhdatle/wake/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

## Architecture

```mermaid
flowchart TB
  APP["Your app"]

  subgraph PG["PostgreSQL"]
    direction LR
    TBL[("Tables")]
    WAL[("WAL slot")]
    TBL ~~~ WAL
  end

  subgraph WK["Wake service"]
    direction LR
    SN["Snapshotter"]
    RD["Reader"]
    TX["Assembler"]
    RT["Router"]
    ACK["Ack tracker"]
    RD --> TX --> RT
    SN --> RT
    RT -.->|"track"| ACK
  end

  subgraph SK["Sinks"]
    direction LR
    KF["Kafka"]
    NT["NATS"]
    WH["Webhooks"]
    KF ~~~ NT ~~~ WH
  end

  subgraph CA["Cache"]
    direction LR
    INV["Invalidator"]
    RDS[("Redis")]
    L1["L1 cache"]
    INV -->|"delete"| RDS -.->|"notify"| L1
  end

  APP ==>|"commit"| PG
  PG <==>|"stream · confirm"| WK
  WK <==>|"deliver · ack"| SK
  SK -.->|"events"| CA

  classDef app fill:#eef4ff,stroke:#3b6fd8,color:#0b1f44,stroke-width:1.5px
  classDef db fill:#e9f7ef,stroke:#2f9e5c,color:#0d2e1a,stroke-width:1.5px
  classDef core fill:#f3eefe,stroke:#7a52d1,color:#22134a,stroke-width:1.5px
  classDef extra fill:#fff5e6,stroke:#d98a1c,color:#3d2504,stroke-width:1.5px
  class APP,L1 app
  class TBL,WAL,RDS db
  class RD,TX,SN,RT,ACK,INV core
  class KF,NT,WH extra
  style PG fill:#f6fcf8,stroke:#2f9e5c,stroke-dasharray:4 3
  style WK fill:#faf8ff,stroke:#7a52d1,stroke-dasharray:4 3
  style SK fill:#fffaf2,stroke:#d98a1c,stroke-dasharray:4 3
  style CA fill:#f7faff,stroke:#3b6fd8,stroke-dasharray:4 3
```

Wake reads one replication slot, assembles each transaction and routes it to the sinks at commit.
It confirms WAL to Postgres only after every sink holds the change, and the invalidator removes
changed keys from Redis and every L1 cache.

## Why

Polling misses changes, triggers slow down writes, and dual writes drift. Debezium needs the JVM
and usually Kafka Connect, while home-grown WAL readers tend to lose events on restart or during
the first snapshot. Wake is one static Go binary that replays after a crash instead of losing,
and its cache library stays correct when data changes outside your application.

## Install

```sh
docker run --rm ghcr.io/thnhdatle/wake:v0.1.1 version         # service in Docker
go install github.com/thnhdatle/wake/cmd/wake@v0.1.1          # service binary
go get github.com/thnhdatle/wake@v0.1.1                        # cache library and event types
```

Prebuilt binaries for Linux and macOS are on the [releases page](https://github.com/thnhdatle/wake/releases).

## Quickstart

Needs Docker with Compose v2. From a clone of this repository:

```bash
docker compose -f deploy/docker-compose.yml up -d --wait
docker compose -f deploy/docker-compose.yml exec postgres psql -U postgres -d app -c "INSERT INTO users VALUES (1, 'a@example.com')"
docker compose -f deploy/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic cdc.public.users --from-beginning --max-messages 1
```

The first run builds the Wake image from source. The last command prints the inserted row as a
JSON change event.

## Usage

```yaml
# wake.yaml
source:
  name: app
  dsn_env: WAKE_SOURCE_DSN
tables:
  include: ["public.*"]
sinks:
  - name: kafka-main
    type: kafka
    tables: ["*"]
    kafka:
      brokers: ["localhost:9092"]
      topic_prefix: cdc
```

```sh
export WAKE_SOURCE_DSN='postgres://wake:change-me@localhost:5432/app'
wake check --config wake.yaml
wake run --config wake.yaml
```

Delivery is at least once, so consumers deduplicate by event id or upsert by key. Preparing
Postgres, sinks, snapshots and the cache library are covered in the [guides](docs/guides).

## Performance

MacBook Pro M3 Pro 36 GB, macOS 26.1, Go 1.27.2, Wake v0.1.0. Wake runs on the host against the
compose Postgres, Kafka and Redis (Docker VM 8 CPU / 10 GB).

| Scenario | Result |
|---|---|
| Postgres → Kafka, single-row inserts offered at 5,000 tx/s, median of 3 runs | 4,999.7 events/s, p99 12 ms |
| CDC invalidation latency, commit → key gone from L1 on 3 instances | p99 18.5 ms |
| `Cache.Get` L1 hit, one goroutine, 64-byte value, no L2 | 55.8 ns/op, 0 allocs/op |

## Documentation

- [Guides](docs/guides)
- [Roadmap](ROADMAP.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [License: Apache-2.0](LICENSE)
