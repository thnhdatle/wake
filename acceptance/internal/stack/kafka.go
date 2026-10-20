// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/pkg/event"
)

// Topic returns the Kafka topic of a table: <prefix>.<schema>.<table>.
func Topic(prefix, schema, table string) string { return prefix + "." + schema + "." + table }

// SchemaTopic returns the schema-change topic <prefix>._schema.
func SchemaTopic(prefix string) string { return prefix + "._schema" }

// Record is one consumed Kafka record.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	// Timestamp is the record timestamp: the broker's append time for
	// topics with message.timestamp.type=LogAppendTime.
	Timestamp time.Time
}

// ReadTopics consumes the topics from the beginning up to the end offsets
// listed when it is called and returns the records ordered by topic,
// partition and offset (per-key delivery order, a key maps to one
// partition). Topics that do not exist yield no records. Call it after
// WaitConfirmed: every event Wake confirmed is acknowledged by Kafka, so it
// is below the listed end offsets.
func ReadTopics(ctx context.Context, topics ...string) ([]Record, error) {
	var out []Record
	if err := ScanTopics(ctx, func(r Record) { out = append(out, r) }, topics...); err != nil {
		return out, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		if a.Partition != b.Partition {
			return a.Partition < b.Partition
		}
		return a.Offset < b.Offset
	})
	return out, nil
}

// ScanTopics is ReadTopics without keeping the records: fn sees each record
// once, in offset order within a partition (partitions interleave).
func ScanTopics(ctx context.Context, fn func(Record), topics ...string) error {
	ends, err := EndOffsets(ctx, topics...)
	if err != nil {
		return err
	}
	want := map[string]map[int32]kgo.Offset{}
	remaining := map[string]map[int32]int64{}
	for topic, ps := range ends {
		for p, off := range ps {
			if off <= 0 {
				continue
			}
			if want[topic] == nil {
				want[topic], remaining[topic] = map[int32]kgo.Offset{}, map[int32]int64{}
			}
			want[topic][p] = kgo.NewOffset().AtStart()
			remaining[topic][p] = off
		}
	}
	if len(want) == 0 {
		return nil
	}
	return consume(ctx, want, remaining, fn)
}

// Offsets are end offsets by topic and partition.
type Offsets map[string]map[int32]int64

// EndOffsets lists the end offsets (the offset the next record gets) of the
// topics' partitions; topics that do not exist are absent.
func EndOffsets(ctx context.Context, topics ...string) (Offsets, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(KafkaBroker))
	if err != nil {
		return nil, fmt.Errorf("stack: kafka client: %w", err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("stack: list end offsets: %w", err)
	}
	out := Offsets{}
	var listErr error
	ends.Each(func(o kadm.ListedOffset) {
		switch {
		case errors.Is(o.Err, kerr.UnknownTopicOrPartition):
		case o.Err != nil:
			listErr = errors.Join(listErr, fmt.Errorf("%s[%d]: %w", o.Topic, o.Partition, o.Err))
		default:
			if out[o.Topic] == nil {
				out[o.Topic] = map[int32]int64{}
			}
			out[o.Topic][o.Partition] = o.Offset
		}
	})
	if listErr != nil {
		return nil, fmt.Errorf("stack: list end offsets: %w", listErr)
	}
	return out, nil
}

// CreateTopics creates the topics with the given partition count, replication
// factor 1 and configs (for example message.timestamp.type).
func CreateTopics(ctx context.Context, partitions int32, configs map[string]string, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(KafkaBroker))
	if err != nil {
		return fmt.Errorf("stack: kafka client: %w", err)
	}
	defer cl.Close()
	cfg := make(map[string]*string, len(configs))
	for k, v := range configs {
		cfg[k] = &v
	}
	resp, err := kadm.NewClient(cl).CreateTopics(ctx, partitions, 1, cfg, topics...)
	if err != nil {
		return fmt.Errorf("stack: create topics: %w", err)
	}
	var errs error
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			errs = errors.Join(errs, fmt.Errorf("stack: create topic %s: %w", r.Topic, r.Err))
		}
	}
	return errs
}

// DeleteTopics deletes the topics; topics that do not exist are ignored.
// Tests call it at cleanup so large runs do not fill the broker's disk.
func DeleteTopics(ctx context.Context, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(KafkaBroker))
	if err != nil {
		return fmt.Errorf("stack: kafka client: %w", err)
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).DeleteTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("stack: delete topics: %w", err)
	}
	var errs error
	for _, r := range resp.Sorted() {
		if r.Err != nil && !errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			errs = errors.Join(errs, fmt.Errorf("stack: delete topic %s: %w", r.Topic, r.Err))
		}
	}
	return errs
}

func consume(ctx context.Context, want map[string]map[int32]kgo.Offset, end map[string]map[int32]int64,
	fn func(Record),
) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(KafkaBroker), kgo.ConsumePartitions(want),
		kgo.FetchMaxBytes(64<<20))
	if err != nil {
		return fmt.Errorf("stack: kafka consumer: %w", err)
	}
	defer cl.Close()
	open := 0
	for _, ps := range end {
		open += len(ps)
	}
	for open > 0 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return fmt.Errorf("stack: consume (%d partitions not at end): %w", open, ctx.Err())
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("stack: fetch %s[%d]: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			last, ok := end[r.Topic][r.Partition]
			if !ok || r.Offset >= last {
				return
			}
			fn(toRecord(r))
			if r.Offset == last-1 {
				delete(end[r.Topic], r.Partition)
				open--
			}
		})
	}
	return nil
}

func toRecord(r *kgo.Record) Record {
	h := make(map[string]string, len(r.Headers))
	for _, kv := range r.Headers {
		h[kv.Key] = string(kv.Value)
	}
	return Record{
		Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value, Headers: h,
		Timestamp: r.Timestamp,
	}
}

// Decoded holds classified payloads in input order.
type Decoded struct {
	Events        []*event.Event
	SchemaChanges []*event.SchemaChange
	Messages      int
	Tombstones    int
}

// DecodePayloads classifies and decodes CDC event payloads (Kafka record values or
// webhook array elements) with pkg/event; a nil or empty payload is a Kafka
// tombstone.
func DecodePayloads(payloads [][]byte) (Decoded, error) {
	var d Decoded
	for i, p := range payloads {
		if event.IsKafkaTombstone(p) {
			d.Tombstones++
			continue
		}
		kind, err := event.KindOf(p)
		if err != nil {
			return d, fmt.Errorf("stack: payload %d: %w", i, err)
		}
		switch kind {
		case event.KindEvent:
			e, err := event.Decode(p)
			if err != nil {
				return d, fmt.Errorf("stack: payload %d: %w", i, err)
			}
			d.Events = append(d.Events, e)
		case event.KindSchemaChange:
			s, err := event.DecodeSchemaChange(p)
			if err != nil {
				return d, fmt.Errorf("stack: payload %d: %w", i, err)
			}
			d.SchemaChanges = append(d.SchemaChanges, s)
		case event.KindMessage:
			d.Messages++
		}
	}
	return d, nil
}

// Values returns the record values.
func Values(recs []Record) [][]byte {
	out := make([][]byte, len(recs))
	for i, r := range recs {
		out[i] = r.Value
	}
	return out
}
