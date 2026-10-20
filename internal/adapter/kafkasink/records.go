// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Record headers (CDC event transport).
const (
	HeaderEventID       = "wake-event-id"
	HeaderSchemaVersion = "wake-schema-version"
)

// batchOverhead bounds the bytes a record batch holding one record adds to
// the record's key, value and headers: the 61-byte batch header plus the
// record's varint framing. It turns an event size into the size the broker
// compares with max.message.bytes.
const batchOverhead = 128

// maxReasonKey caps the key quoted in an event_too_large reason.
const maxReasonKey = 128

// unit is one pipeline.Record from Send to acknowledgement. Its writes are
// the Kafka records it becomes: one, two for a `d` event with its tombstone,
// one per partition for a `t` event.
type unit struct {
	rec      pipeline.Record
	seq      uint64 // dispatch order, assigned by the dispatcher
	size     int64  // event size: key, value and headers
	reserved int64  // bytes held in the buffer semaphore
	topic    string
	writes   []*write

	// Guarded by Sink.mu.
	pending int       // writes not yet acknowledged
	sentAt  time.Time // last produce attempt
}

// write is one Kafka record of a unit.
type write struct {
	u     *unit
	idx   int
	rec   *kgo.Record
	retry bool // re-produced after a failure; guarded by Sink.mu
}

func newUnit(rec pipeline.Record, prefix string) *unit {
	return &unit{rec: rec, size: eventSize(&rec), topic: topicName(prefix, rec.Destination)}
}

// topicName returns `<prefix>.<destination>`.
func topicName(prefix, destination string) string { return prefix + "." + destination }

func isTruncate(rec *pipeline.Record) bool {
	return rec.Kind == pipeline.RecordKindEvent && rec.Op == change.OpTruncate
}

func hasTombstone(rec *pipeline.Record, enabled bool) bool {
	return enabled && rec.Kind == pipeline.RecordKindEvent && rec.Op == change.OpDelete && rec.Key != nil
}

// eventSize is the size max_event_bytes limits: key, value and headers.
func eventSize(rec *pipeline.Record) int64 {
	var buf [64]byte
	id := rec.ID.AppendText(buf[:0])
	n := len(rec.Key) + len(rec.Payload) + len(HeaderEventID) + len(id) + len(HeaderSchemaVersion)
	n += len(strconv.AppendUint(buf[:0], rec.SchemaVersion, 10))
	return int64(n)
}

func headers(rec *pipeline.Record) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: HeaderEventID, Value: rec.ID.AppendText(nil)},
		{Key: HeaderSchemaVersion, Value: strconv.AppendUint(nil, rec.SchemaVersion, 10)},
	}
}

// buildWrites turns u into its Kafka records. A `t` event goes to each of
// the topic's partitions with the same id; a `d` event is followed by a
// tombstone (same key, null value, no headers) that hashes to the same
// partition, so it lands right after it.
func buildWrites(u *unit, partitions int, tombstones bool) {
	rec := &u.rec
	hdrs := headers(rec)
	if isTruncate(rec) {
		u.writes = make([]*write, max(partitions, 1))
		for p := range u.writes {
			part := int32(p) //nolint:gosec // G115: partition counts fit in int32
			u.writes[p] = &write{u: u, idx: p, rec: &kgo.Record{
				Topic: u.topic, Value: rec.Payload, Headers: hdrs, Partition: part,
			}}
		}
		u.pending = len(u.writes)
		return
	}
	u.writes = []*write{{u: u, rec: &kgo.Record{
		Topic: u.topic, Key: rec.Key, Value: rec.Payload, Headers: hdrs, Partition: hashPartition,
	}}}
	if hasTombstone(rec, tombstones) {
		u.writes = append(u.writes, &write{u: u, idx: 1, rec: &kgo.Record{
			Topic: u.topic, Key: rec.Key, Partition: hashPartition,
		}})
	}
	u.pending = len(u.writes)
}

// retryRecord returns a fresh copy of a failed record for re-production; it
// keeps the partition the first attempt chose.
func retryRecord(r *kgo.Record) *kgo.Record {
	return &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: r.Headers, Partition: r.Partition}
}

// sortWrites orders failed writes by dispatch order, so re-production keeps
// the per-key order.
func sortWrites(ws []*write) {
	slices.SortFunc(ws, func(a, b *write) int {
		if c := cmp.Compare(a.u.seq, b.u.seq); c != 0 {
			return c
		}
		return cmp.Compare(a.idx, b.idx)
	})
}

// tooLargeReason describes an event above max_event_bytes with its table,
// key and lsn; it never contains row values.
func tooLargeReason(rec *pipeline.Record, size, limit int64) string {
	table := rec.Table()
	if table == "" {
		table = rec.Destination
	}
	key := rec.Key
	if len(key) > maxReasonKey {
		key = key[:maxReasonKey]
	}
	return fmt.Sprintf("event of %d bytes exceeds max_event_bytes %d: table %s, key %s, lsn %s, id %s",
		size, limit, table, key, rec.ID.LSN, rec.ID)
}
