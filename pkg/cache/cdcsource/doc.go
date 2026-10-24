// SPDX-License-Identifier: Apache-2.0

// Package cdcsource invalidates pkg/cache entries from Wake's change events. It consumes the events
// from Kafka or NATS JetStream and deletes the cache keys rendered from each changed row.
package cdcsource
