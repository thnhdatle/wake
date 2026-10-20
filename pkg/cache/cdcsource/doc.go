// SPDX-License-Identifier: Apache-2.0

// Package cdcsource invalidates pkg/cache entries from Wake's change events
// (the CDC event contract): it consumes the events from Kafka or NATS
// JetStream, renders cache keys from each changed row with key templates and
// deletes them through a Deleter such as cache.Cache[V].
//
// Main types: Template (ParseTemplate), Mapping (NewMapping: tables to
// templates), Invalidator (New, Run, Ready), the Source interface with its
// implementations KafkaSource (NewKafkaSource) and NATSSource
// (NewNATSSource), and Deleter.
//
// # Templates
//
// A template is literal text with "{column}" placeholders, for example
// "user:{id}" or "user:email:{email}"; "{{" and "}}" stand for literal braces.
// Each template of a table is rendered against each available row image
// (before and after), so a changed unique attribute invalidates both its old
// and its new key. A placeholder resolves from the event key, then the image
// (for the before image, the image first, so a primary-key change also
// invalidates the old key's entries). Integers render as exact decimals and
// strings verbatim; a null or absent value skips that rendering.
//
// # Events
//
// Kafka tombstones, invalid payloads, unknown ops, truncates (t), snapshot
// reads (r, unless WithInvalidateOnSnapshot) and events of unmapped tables
// are skipped and counted. Templates that reference non-key columns need
// REPLICA IDENTITY FULL: the first update or delete whose before image lacks
// such a column raises template_requires_full_identity (an error log per
// table, a metric, and Ready reports it until restart), and processing
// continues with the images available. Mapping.CheckReplicaIdentity performs
// the same check up front from table metadata.
//
// # Delivery
//
// A batch of events is acknowledged (Kafka offset commit, NATS ack) only
// after Delete returned nil for all its keys. cache.ErrDeferred, like any
// other error, means the invalidation has not reached every instance: the
// Delete is retried with a capped, jittered exponential backoff (WithRetry,
// WithClock, WithRand) and nothing is acknowledged meanwhile. Invalidations
// are idempotent and commutative, so redelivered events are harmless.
//
//	m, err := cdcsource.NewMapping(map[string][]string{
//		"public.users": {"user:{id}", "user:email:{email}"},
//	})
//	if err != nil { ... }
//	src, err := cdcsource.NewKafkaSource(cdcsource.KafkaConfig{
//		Brokers: []string{"localhost:9092"}, TopicPrefix: "cdc", Group: "wake-cache-invalidator",
//	}, m)
//	if err != nil { ... }
//	inv, err := cdcsource.New(src, m, users) // users is a cache.Cache[User]
//	if err != nil { ... }
//	err = inv.Run(ctx)
package cdcsource
