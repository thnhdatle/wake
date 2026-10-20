// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"strings"
	"testing"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// BenchmarkRecords_Build turns one accepted event into its Kafka record
// without a broker: the unit Send queues, the records prepare builds and the
// partition the partitioner picks (6 partitions, as in the throughput acceptance test).
func BenchmarkRecords_Build(b *testing.B) {
	payload := `"` + strings.Repeat("x", 168) + `"`
	cases := []struct {
		name string
		op   change.Op
	}{{"insert", change.OpCreate}, {"delete_tombstone", change.OpDelete}}
	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			ev := &change.Event{
				ID: change.StreamID(lsn.LSN(0x16B3748), 0), Source: change.Source{DB: "app", Schema: "wak05", Table: "t1"},
				Op: bc.op, TsMs: 1_792_308_604_187, TxID: 900, SchemaVersion: 1,
				Key: change.Row{{Name: "id", Value: []byte("1234567")}},
			}
			if bc.op == change.OpCreate {
				ev.After = change.Row{{Name: "id", Value: []byte("1234567")}, {Name: "payload", Value: []byte(payload)}}
			}
			rec := pipeline.NewEventRecord(ev)
			b.ReportAllocs()
			for b.Loop() {
				u := newUnit(rec, testTopicPrefix)
				buildWrites(u, 6, true)
				if partitionFor(u.writes[0].rec, 6) < 0 {
					b.Fatal("negative partition")
				}
			}
		})
	}
}
