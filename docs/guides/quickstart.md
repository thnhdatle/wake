# Quickstart

Run Wake against a throwaway Postgres and Kafka and see one insert arrive as a change event.

## Prerequisites

- Docker with Compose v2.
- A clone of this repository. The compose file builds the Wake image on first use.

The stack runs Postgres 17, Kafka, NATS, Redis and Wake. Wake captures every table of the
`public` schema to Kafka topics prefixed `cdc`. Postgres starts with a `users` table.

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

LSN, transaction id and timestamp differ on your machine. Fields are explained in
[consuming-events.md](consuming-events.md).

## Look around

```bash
curl -s http://127.0.0.1:9093/readyz      # 200 once Wake streams and no sink is blocked
curl -s http://127.0.0.1:9093/v1/status   # slot, confirmed LSN, lag, schema versions
docker compose -f deploy/docker-compose.yml exec wake /wake status --config /etc/wake/config.yaml --addr 127.0.0.1:9093
```

Try an update and a delete, then read all events:

```bash
docker compose -f deploy/docker-compose.yml exec postgres psql -U postgres -d app -c "UPDATE users SET email = 'b@example.com' WHERE id = 1"
docker compose -f deploy/docker-compose.yml exec postgres psql -U postgres -d app -c "DELETE FROM users WHERE id = 1"
docker compose -f deploy/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic cdc.public.users --from-beginning --max-messages 4
```

You see the create, the update, the delete and a tombstone (printed as `null`). The update's
`before` holds only the key, because the table uses the default replica identity.

## Clean up

```bash
docker compose -f deploy/docker-compose.yml down -v   # removes containers, volumes, state file and slot
```

## Next steps

- [getting-started.md](getting-started.md): a real deployment.
- [configuration.md](configuration.md): every configuration key.
- [sinks.md](sinks.md): Kafka, NATS, webhook and stdout.
- [cache.md](cache.md): invalidate a Redis-backed cache from the same events.
