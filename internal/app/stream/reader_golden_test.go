// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
)

var update = flag.Bool("update", false, "rewrite testdata/golden/reader")

const readerGoldenDir = "testdata/golden/reader"

var goldenVariants = []struct {
	name   string
	cfg    ReaderConfig
	policy pipeline.TablePolicyConfig
}{
	{name: "default"},
	{
		name:   "emit_all",
		cfg:    ReaderConfig{EmitTruncate: true, EmitMessages: true},
		policy: pipeline.TablePolicyConfig{EmitUnkeyed: true},
	},
}

func TestReader_Run_Golden(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob(filepath.Join(recordedDir, "*.hex"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, p := range paths {
		name := filepath.Base(p)
		msgs := readRecorded(t, name)
		for _, v := range goldenVariants {
			t.Run(name+"/"+v.name, func(t *testing.T) {
				t.Parallel()
				keys := keyColumns{keys: map[string][]string{"rec.docs_full": {"id"}}}
				recs, _ := runAll(t, fixtureOpts{
					cfg: v.cfg, policy: v.policy, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()},
					readerOpts: []ReaderOption{WithKeyColumns(keys)},
				}, msgs)
				got := render(recs)
				path := filepath.Join(readerGoldenDir, name+"."+v.name+".golden")
				if *update {
					require.NoError(t, os.MkdirAll(readerGoldenDir, 0o750))
					require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
					return
				}
				want, err := os.ReadFile(path) //nolint:gosec // G304: golden path from fixture names
				require.NoError(t, err, "missing golden file; run go test ./internal/app/stream -update")
				assert.Equal(t, string(want), got)
			})
		}
	}
}

func TestReader_Run_RecordedIDs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		file string
		cfg  ReaderConfig
		want []string
	}{
		{
			name: "multi default drops messages and truncate", file: "multi.hex",
			want: []string{"0/197D6F0-0", "0/197D6F0-1", "0/197D6F0-2"},
		},
		{
			name: "multi forwards messages and truncate", file: "multi.hex",
			cfg: ReaderConfig{EmitTruncate: true, EmitMessages: true},
			want: []string{
				"0/197D6F0-0", "0/197D6F0-1", "0/197D6F0-2", "0/197D6F0-m0", "0/197D768-m0", "0/197DDB0-0",
			},
		},
		{
			name: "schema changes precede their data event", file: "schema.hex",
			want: []string{
				"0/197DF98-0", "0/1981E98-s0", "0/1981E98-0", "0/1982280-s0", "0/1982280-0",
				"0/1988688-s0", "0/1988688-0", "0/1988950-s0", "0/1988950-0",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recs, _ := runAll(t, fixtureOpts{cfg: tc.cfg, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}},
				readRecorded(t, tc.file))
			assert.Equal(t, tc.want, ids(recs))
		})
	}
}

func TestReader_Run_RecordedMessageFields(t *testing.T) {
	t.Parallel()
	recs, _ := runAll(t, fixtureOpts{
		cfg: ReaderConfig{EmitTruncate: true, EmitMessages: true}, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()},
	}, readRecorded(t, "multi.hex"))
	require.Len(t, recs, 6)
	inTx, outside, trunc := recs[3], recs[4], recs[5]
	assert.Equal(t, pipeline.RecordKindMessage, inTx.Kind)
	assert.Equal(t, pipeline.DestinationMessages, inTx.Destination)
	assert.Contains(t, string(inTx.Payload), `"transactional":true`)
	assert.Contains(t, string(inTx.Payload), `"ts_ms":1792308605580`)
	assert.Contains(t, string(outside.Payload), `"transactional":false`)
	assert.Contains(t, string(outside.Payload), `"ts_ms":1792308700000`, "server time of the XLogData")
	assert.Equal(t, change.OpTruncate, trunc.Op)
	assert.True(t, trunc.Barrier)
	assert.Nil(t, trunc.Key)
	assert.Contains(t, string(trunc.Payload), `"key":null`)
	for _, r := range recs {
		assert.NotContains(t, string(r.Payload), "wake.heartbeat")
	}
}

func TestReader_Run_RecordedKeylessAndFullIdentity(t *testing.T) {
	t.Parallel()
	msgs := readRecorded(t, "identity.hex")
	recs, _ := runAll(t, fixtureOpts{sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}}, msgs)
	for _, r := range recs {
		assert.NotEqual(t, "rec.keyless", r.Destination, "keyless table emitted without emit_unkeyed")
	}
	recs, _ = runAll(t, fixtureOpts{
		policy: pipeline.TablePolicyConfig{EmitUnkeyed: true}, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()},
	}, msgs)
	last := recs[len(recs)-1]
	assert.Equal(t, "rec.keyless", last.Destination)
	assert.Equal(t, last.ID.String(), string(last.Key), "unkeyed events are partitioned by id")
	assert.Contains(t, string(last.Payload), `"key":null`)
}
