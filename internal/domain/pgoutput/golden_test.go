// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

var update = flag.Bool("update", false,
	"rewrite testdata/golden, the synthesized testdata/pgoutput/*.bin inputs and testdata/fuzz/FuzzDecode")

const (
	recordedDir  = "../../../testdata/pgoutput"
	synthDir     = "testdata/pgoutput"
	goldenDir    = "testdata/golden"
	fuzzSeedsDir = "testdata/fuzz/FuzzDecode"
)

func appKeyColumns() map[string][]string {
	return map[string][]string{"rec.docs_full": {"id"}, "syn.full_pk": {"id"}}
}

func readRecorded(tb testing.TB, path string) [][]byte {
	tb.Helper()
	f, err := os.Open(path)
	require.NoError(tb, err)
	defer f.Close()
	var msgs [][]byte
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		require.Len(tb, fields, 3, "line %q", sc.Text())
		b, err := hex.DecodeString(fields[2])
		require.NoError(tb, err)
		msgs = append(msgs, b)
	}
	require.NoError(tb, sc.Err())
	return msgs
}

func frame(msgs [][]byte) []byte {
	var out []byte
	for _, m := range msgs {
		out = binary.BigEndian.AppendUint32(out, uint32(len(m))) //nolint:gosec // G115: small test messages
		out = append(out, m...)
	}
	return out
}

func unframe(tb testing.TB, data []byte) [][]byte {
	tb.Helper()
	var msgs [][]byte
	for len(data) > 0 {
		require.GreaterOrEqual(tb, len(data), 4, "truncated frame header")
		n := int(binary.BigEndian.Uint32(data))
		require.GreaterOrEqual(tb, len(data)-4, n, "truncated frame")
		msgs = append(msgs, data[4:4+n])
		data = data[4+n:]
	}
	return msgs
}

type replay struct {
	dec     *pgoutput.Decoder
	reg     *schema.Registry
	begin   *pgoutput.Begin
	index   uint64
	msgN    uint64
	pending map[uint32]schema.Observation
	out     bytes.Buffer
}

func newReplay() *replay {
	return &replay{dec: pgoutput.NewDecoder(), reg: schema.NewRegistry(), pending: map[uint32]schema.Observation{}}
}

func (r *replay) line(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	r.out.Write(b)
	r.out.WriteByte('\n')
}

func (r *replay) raw(b []byte) {
	r.out.Write(b)
	r.out.WriteByte('\n')
}

func (r *replay) run(msgs [][]byte) string {
	for _, b := range msgs {
		m, err := r.dec.Decode(b)
		if err != nil {
			r.line(map[string]string{"error": err.Error()})
			continue
		}
		r.apply(m)
	}
	return r.out.String()
}

func (r *replay) apply(m pgoutput.Message) {
	switch m := m.(type) {
	case *pgoutput.Begin:
		r.begin, r.index, r.msgN = m, 0, 0
		r.line(map[string]any{"begin": map[string]any{"final_lsn": m.FinalLSN.String(), "ts_ms": m.TsMs, "xid": m.XID}})
	case *pgoutput.Commit:
		r.begin = nil
		r.line(map[string]any{"commit": map[string]any{
			"flags": m.Flags, "commit_lsn": m.CommitLSN.String(), "end_lsn": m.EndLSN.String(), "ts_ms": m.TsMs,
		}})
	case *pgoutput.Origin:
		r.line(map[string]any{"origin": map[string]any{"commit_lsn": m.CommitLSN.String(), "name": m.Name}})
	case *pgoutput.TypeMessage:
		r.line(map[string]any{"type": map[string]any{"oid": m.OID, "namespace": m.Namespace, "name": m.Name}})
	case *pgoutput.Relation:
		r.relation(m)
	case *pgoutput.Insert, *pgoutput.Update, *pgoutput.Delete:
		r.row(m)
	case *pgoutput.Truncate:
		r.truncate(m)
	case *pgoutput.LogicalMessage:
		r.message(m)
	}
}

func (r *replay) relation(m *pgoutput.Relation) {
	obs := r.reg.Observe(&m.Relation)
	if obs.Changed {
		r.pending[m.OID] = obs
	}
	if keys, ok := appKeyColumns()[m.QualifiedName()]; ok {
		if err := r.dec.SetKeyColumns(m.OID, keys); err != nil {
			panic(err)
		}
	}
	cols := make([]map[string]any, len(m.Columns))
	for i, c := range m.Columns {
		cols[i] = map[string]any{"name": c.Name, "type": c.TypeName, "oid": c.TypeOID, "typmod": c.TypeMod, "key": c.Key}
	}
	keys, _ := r.dec.KeyColumns(m.OID)
	r.line(map[string]any{"relation": map[string]any{
		"oid": m.OID, "table": m.QualifiedName(), "identity": string(rune(m.Identity)), "columns": cols,
		"key_columns": keys, "keyless": m.IsKeyless(), "unknown_types": m.UnknownTypes(), "schema_version": obs.Version,
	}})
}

func (r *replay) commitLSNAndTime() (lsn.LSN, int64, uint32) {
	if r.begin == nil {
		return lsn.InvalidLSN, 0, 0
	}
	return r.begin.FinalLSN, r.begin.TsMs, r.begin.XID
}

func (r *replay) source(oid uint32) (change.Source, uint64) {
	rel, ok := r.dec.Relation(oid)
	if !ok {
		return change.Source{DB: "app"}, 0
	}
	v, _ := r.reg.Version(rel.QualifiedName())
	return change.Source{DB: "app", Schema: rel.Namespace, Table: rel.Name}, v
}

func (r *replay) row(m pgoutput.Message) {
	var oid uint32
	switch m := m.(type) {
	case *pgoutput.Insert:
		oid = m.RelationOID
	case *pgoutput.Update:
		oid = m.RelationOID
	case *pgoutput.Delete:
		oid = m.RelationOID
	}
	img, err := r.dec.RowImages(m)
	if err != nil {
		r.line(map[string]string{"error": err.Error()})
		return
	}
	at, ts, xid := r.commitLSNAndTime()
	src, version := r.source(oid)
	if obs, ok := r.pending[oid]; ok {
		delete(r.pending, oid)
		sc := change.SchemaChange{
			ID: change.SchemaID(at, r.index), Source: src, TsMs: ts, SchemaVersion: obs.Version,
			ColumnsBefore: obs.Before, ColumnsAfter: obs.After,
		}
		r.raw(sc.AppendJSON(nil))
	}
	ev := change.Event{
		ID: change.StreamID(at, r.index), Source: src, Op: img.Op, TsMs: ts, TxID: xid,
		Key: img.Key, Before: img.Before, After: img.After, Unchanged: img.Unchanged, SchemaVersion: version,
	}
	r.index++
	r.raw(ev.AppendJSON(nil))
}

func (r *replay) truncate(m *pgoutput.Truncate) {
	at, ts, xid := r.commitLSNAndTime()
	r.line(map[string]any{"truncate": map[string]any{
		"cascade": m.Cascade(), "restart_identity": m.RestartIdentity(), "relations": m.RelationOIDs,
	}})
	for _, oid := range m.RelationOIDs {
		src, version := r.source(oid)
		ev := change.Event{
			ID: change.StreamID(at, r.index), Source: src, Op: change.OpTruncate, TsMs: ts, TxID: xid,
			SchemaVersion: version,
		}
		r.index++
		r.raw(ev.AppendJSON(nil))
	}
}

func (r *replay) message(m *pgoutput.LogicalMessage) {
	if m.IsInternal() {
		r.line(map[string]any{"internal_message": map[string]any{
			"prefix": m.Prefix, "lsn": m.LSN.String(), "transactional": m.Transactional(), "content": string(m.Content),
		}})
		return
	}
	at, ts, _ := r.commitLSNAndTime()
	msg := change.Message{
		ID: change.MessageID(m.LSN, 0), Transactional: m.Transactional(), Prefix: m.Prefix, Content: m.Content,
	}
	if m.Transactional() {
		msg.ID = change.MessageID(at, r.msgN)
		msg.TsMs = ts
		r.msgN++
	}
	r.raw(msg.AppendJSON(nil))
}

type goldenCase struct {
	name   string
	golden string
	msgs   [][]byte
}

func recordedCases(tb testing.TB) []goldenCase {
	tb.Helper()
	paths, err := filepath.Glob(filepath.Join(recordedDir, "*.hex"))
	require.NoError(tb, err)
	require.NotEmpty(tb, paths, "recorded traffic missing in %s", recordedDir)
	cases := make([]goldenCase, 0, len(paths))
	for _, p := range paths {
		base := filepath.Base(p)
		cases = append(cases, goldenCase{name: base, golden: base + ".golden", msgs: readRecorded(tb, p)})
	}
	return cases
}

func TestMain(m *testing.M) {
	flag.Parse()
	if *update {
		if err := writeSynthesized(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func writeSynthesized() error {
	if err := os.MkdirAll(synthDir, 0o750); err != nil {
		return err
	}
	for name, msgs := range synthesizedScenarios() {
		if err := os.WriteFile(filepath.Join(synthDir, name+".bin"), frame(msgs), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func synthCases(tb testing.TB) []goldenCase {
	tb.Helper()
	paths, err := filepath.Glob(filepath.Join(synthDir, "*.bin"))
	require.NoError(tb, err)
	require.NotEmpty(tb, paths, "run go test ./internal/domain/pgoutput -update")
	cases := make([]goldenCase, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p) //nolint:gosec // G304: test fixture path from Glob
		require.NoError(tb, err)
		base := filepath.Base(p)
		cases = append(cases, goldenCase{name: base, golden: base + ".golden", msgs: unframe(tb, data)})
	}
	return cases
}

func TestGolden_Replay(t *testing.T) {
	t.Parallel()
	cases := append(recordedCases(t), synthCases(t)...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := newReplay().run(tc.msgs)
			for i, line := range strings.Split(strings.TrimSpace(got), "\n") {
				require.True(t, json.Valid([]byte(line)), "line %d is not JSON: %s", i+1, line)
			}
			path := filepath.Join(goldenDir, tc.golden)
			if *update {
				require.NoError(t, os.MkdirAll(goldenDir, 0o750))
				require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
				return
			}
			want, err := os.ReadFile(path) //nolint:gosec // G304: golden path built from fixture names
			require.NoError(t, err, "missing golden file; run go test ./internal/domain/pgoutput -update")
			assert.Equal(t, string(want), got)
		})
	}
}

func TestGolden_SynthesizedInputsUpToDate(t *testing.T) {
	t.Parallel()
	for name, msgs := range synthesizedScenarios() {
		data, err := os.ReadFile(filepath.Join(synthDir, name+".bin"))
		require.NoError(t, err, "run go test ./internal/domain/pgoutput -update")
		assert.Equal(t, frame(msgs), data, "%s.bin is stale; run go test ./internal/domain/pgoutput -update", name)
	}
}

func fuzzSeeds(tb testing.TB) map[string][]byte {
	tb.Helper()
	seeds := map[string][]byte{}
	for _, tc := range append(recordedCases(tb), synthCases(tb)...) {
		stem := strings.ReplaceAll(tc.name, ".", "_")
		for i, m := range tc.msgs {
			seeds[fmt.Sprintf("%s_%03d", stem, i)] = m
			if len(m) > 2 && i%3 == 0 {
				seeds[fmt.Sprintf("%s_%03d_truncated", stem, i)] = m[:len(m)/2]
			}
		}
	}
	return seeds
}

func corpusFile(b []byte) []byte {
	return []byte(fmt.Sprintf("go test fuzz v1\n[]byte(%q)\n", b))
}

func TestGolden_FuzzCorpusUpToDate(t *testing.T) {
	t.Parallel()
	seeds := fuzzSeeds(t)
	if *update {
		require.NoError(t, os.MkdirAll(fuzzSeedsDir, 0o750))
		removeStaleSeeds(t, seeds)
		for name, b := range seeds {
			require.NoError(t, os.WriteFile(filepath.Join(fuzzSeedsDir, name), corpusFile(b), 0o600))
		}
		return
	}
	names := make([]string, 0, len(seeds))
	for name := range seeds {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(fuzzSeedsDir, name)) //nolint:gosec // G304: corpus path
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fuzz seed %s missing; run go test ./internal/domain/pgoutput -update", name)
		}
		require.NoError(t, err)
		assert.Equal(t, string(corpusFile(seeds[name])), string(data), "fuzz seed %s is stale", name)
	}
}

func removeStaleSeeds(t *testing.T, seeds map[string][]byte) {
	t.Helper()
	entries, err := os.ReadDir(fuzzSeedsDir)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		generated := strings.Contains(name, "_hex_") || strings.Contains(name, "_bin_")
		if _, keep := seeds[name]; generated && !keep {
			require.NoError(t, os.Remove(filepath.Join(fuzzSeedsDir, name)))
		}
	}
}
