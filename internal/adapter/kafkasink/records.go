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

const (
	HeaderEventID       = "wake-event-id"
	HeaderSchemaVersion = "wake-schema-version"
)

const batchOverhead = 128

const maxReasonKey = 128

type unit struct {
	rec      pipeline.Record
	seq      uint64
	size     int64
	reserved int64
	topic    string
	writes   []*write

	pending int
	sentAt  time.Time
}

type write struct {
	u     *unit
	idx   int
	rec   *kgo.Record
	retry bool
}

func newUnit(rec pipeline.Record, prefix string) *unit {
	return &unit{rec: rec, size: eventSize(&rec), topic: topicName(prefix, rec.Destination)}
}

func topicName(prefix, destination string) string { return prefix + "." + destination }

func isTruncate(rec *pipeline.Record) bool {
	return rec.Kind == pipeline.RecordKindEvent && rec.Op == change.OpTruncate
}

func hasTombstone(rec *pipeline.Record, enabled bool) bool {
	return enabled && rec.Kind == pipeline.RecordKindEvent && rec.Op == change.OpDelete && rec.Key != nil
}

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

func retryRecord(r *kgo.Record) *kgo.Record {
	return &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: r.Headers, Partition: r.Partition}
}

func sortWrites(ws []*write) {
	slices.SortFunc(ws, func(a, b *write) int {
		if c := cmp.Compare(a.u.seq, b.u.seq); c != 0 {
			return c
		}
		return cmp.Compare(a.idx, b.idx)
	})
}

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
