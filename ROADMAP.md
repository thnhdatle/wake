# Roadmap

Items marked `[gfi]` are suitable as good first issues. Everything below is out
of scope for v0.1 and is tracked here; each `TODO(roadmap)` in the code has an
entry in this file.

## Deferred from v0.1

- Parallel incremental snapshot of many tables at once (v0.1 snapshots added tables one at a time)
- Outbox mode
- Column transforms and PII masking
- Avro and Protobuf encodings with Schema Registry
- Web console
- Multiple replication slots in parallel
- MySQL and other sources
- Exactly-once delivery
- RESP3 client-side caching for the cache library
- Tag-based cache invalidation
- HTTP cache middleware
- Dead-letter replay tool [gfi]
- Generated-column handling [gfi]
- Multi-instance high availability
- Helm chart for Kubernetes
