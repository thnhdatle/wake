# Quickstart

This guide runs Wake against a throwaway Postgres and Kafka on your machine and
shows one insert arriving as a change event. It takes a few minutes. For a real
deployment read [getting-started.md](getting-started.md) next.

## Prerequisites

- Docker with Compose v2.
- A clone of this repository. The compose file builds the Wake image from the
  `Dockerfile` in the repository root on first use.

The compose stack starts Postgres 17 with `wal_level=logical`, a single-node
Kafka (KRaft), NATS JetStream, Redis and Wake. Wake runs with the configuration
in `deploy/config.compose.yaml`: source `app`, every table of the `public`
schema, one Kafka sink with topic prefix `cdc` and topic auto-creation. The
compose Postgres creates the table `users (id bigint PRIMARY KEY, email text)` on
first start, and the quickstart connects as the Postgres superuser so that Wake
can create its slot and publication without further setup.

## Run it

```bash
docker compose -f deploy/docker-compose.yml up -d --wait
docker compose -f deploy/docker-compose.yml exec postgres psql -U postgres -d app -c "INSERT INTO users VALUES (1, 'a@example.com')"
docker compose -f deploy/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic cdc.public.users --from-beginning --max-messages 1
```

The last command prints the insert as a CDC event:

```json
{"id":"0/1A2B3C8-0","source":{"db":"app","schema":"public","table":"users"},"op":"c","ts_ms":1760600000000,"lsn":"0/1A2B3C8","tx_id":750,"key":{"id":1},"before":null,"after":{"id":1,"email":"a@example.com"},"schema_version":1}
```

The LSN, transaction id and timestamp differ on your machine. The fields are
explained in [consuming-events.md](consuming-events.md).

## Look around

Wake serves its admin endpoints on `127.0.0.1:9093`:

```bash
curl -s http://127.0.0.1:9093/readyz
curl -s http://127.0.0.1:9093/v1/status
```

`/readyz` answers 200 once Wake streams from Postgres and no sink is blocked;
`/v1/status` shows the slot, the confirmed LSN, the lag and the schema version of
`public.users`. The same document is available from the command line:

```bash
docker compose -f deploy/docker-compose.yml exec wake /wake status --config /etc/wake/config.yaml --addr 127.0.0.1:9093
```

Try an update and a delete, then read all events of the topic:

```bash
docker compose -f deploy/docker-compose.yml exec postgres psql -U postgres -d app -c "UPDATE users SET email = 'b@example.com' WHERE id = 1"
docker compose -f deploy/docker-compose.yml exec postgres psql -U postgres -d app -c "DELETE FROM users WHERE id = 1"
docker compose -f deploy/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic cdc.public.users --from-beginning --max-messages 4
```

You see the create, the update (`before` holds only the key columns because the
table uses the default replica identity), the delete and the Kafka tombstone that
follows it (an empty value, printed as `null`).

## Clean up

```bash
docker compose -f deploy/docker-compose.yml down -v
```

This removes the containers and the volumes, including Wake's state file and the
Postgres replication slot (it lives in the Postgres volume).

## Next steps

- [getting-started.md](getting-started.md): install, a least-privilege Postgres
  role, first run and verification.
- [configuration.md](configuration.md): every configuration key.
- [sinks.md](sinks.md): Kafka, NATS, webhook and stdout options.
- [cache.md](cache.md): invalidate a Redis-backed cache from the same events.
