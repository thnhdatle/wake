// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

type ackSink struct{}

func (ackSink) Name() string                                      { return "ack" }
func (ackSink) Health() pipeline.SinkHealth                       { return pipeline.SinkHealth{} }
func (ackSink) Run(ctx context.Context) error                     { <-ctx.Done(); return nil }
func (ackSink) Send(_ context.Context, rec pipeline.Record) error { return rec.Ack.Ack() }

func BenchmarkReader_Tx(b *testing.B) {
	f := newFixture(b, fixtureOpts{route: ackSink{}})
	ctx := context.Background()
	var s synth
	s.begin(0x1000, 900)
	s.relation(16400, "wak05", "t1", 'd', "payload")
	s.insert(16400, "1234567", strings.Repeat("x", 168))
	s.commit(0x1000, 0x1030)
	for _, m := range s.msgs {
		require.NoError(b, f.reader.step(ctx, &received{msg: xlog(m)}))
	}
	begin, insert, commit := s.msgs[0], s.msgs[2], s.msgs[3]
	at := lsn.LSN(0x1000)
	b.ReportAllocs()
	for b.Loop() {
		at += 0x100
		binary.BigEndian.PutUint64(begin.data[1:], uint64(at))
		binary.BigEndian.PutUint64(commit.data[2:], uint64(at))
		binary.BigEndian.PutUint64(commit.data[10:], uint64(at+0x30))
		for _, m := range []walMsg{begin, insert, commit} {
			if err := f.reader.step(ctx, &received{msg: xlog(m)}); err != nil {
				b.Fatal(err)
			}
		}
	}
	require.Equal(b, at+0x30, f.tracker.Confirmed(), "every transaction confirmed")
}
