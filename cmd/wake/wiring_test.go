// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/adapter/kafkasink"
	"github.com/thnhdatle/wake/internal/adapter/pgrepl"
	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/app/stream"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
)

func TestErrorCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		code string
		exit int
	}{
		{"slot missing", fmt.Errorf("prepare: %w", snapshot.ErrSlotMissing), codeSlotMissing, exitFailure},
		{"state mismatch", fmt.Errorf("x: %w", pipeline.ErrStateMismatch), codeStateMismatch, exitFailure},
		{"slot in use (app)", snapshot.ErrSlotInUse, codeSlotInUse, exitFailure},
		{"slot invalidated (app)", snapshot.ErrSlotInvalidated, codeSlotInvalidated, exitFailure},
		{"adapter code wins", &pgrepl.Error{Code: pgrepl.CodeReplicationPrivilege}, "replication_privilege", 1},
		{
			"pgsql code", fmt.Errorf("sync: %w", &pgsql.Error{Code: pgsql.CodePublicationPrivilege}),
			"publication_privilege", exitFailure,
		},
		{"status interval", fmt.Errorf("x: %w", stream.ErrStatusInterval), codeStatusInterval, exitFailure},
		{"config", fmt.Errorf("x: %w", config.ErrInvalid), codeConfigInvalid, exitConfig},
		{"sink config", fmt.Errorf("x: %w", kafkasink.ErrInvalidConfig), codeConfigInvalid, exitConfig},
		{"webhook secret", webhooksink.ErrMissingSecret, codeConfigInvalid, exitConfig},
		{"explicit exit", &exitError{code: 7, err: errors.New("x")}, "", 7},
		{"plain", errors.New("boom"), "", exitFailure},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.code, errorCode(tc.err))
			assert.Equal(t, tc.exit, exitCode(tc.err))
			if tc.code != "" {
				assert.True(t, strings.HasPrefix(describe(tc.err), "["+tc.code+"] "), describe(tc.err))
			}
		})
	}
}

func TestTopicResolver(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("t", 240)
	cfg := config.Config{
		Source: config.Source{Name: "app"},
		Tables: config.Tables{
			Include: []string{"*"},
			NameMap: map[string]string{"public.bad name": "public.bad_name"},
		},
		Sinks: []config.Sink{
			{
				Name: "k", Type: config.SinkKafka, Tables: []string{"public.*"},
				Kafka: &config.KafkaSink{TopicPrefix: "cdc"},
			},
			{Name: "o", Type: config.SinkStdout, Tables: []string{"*"}},
		},
	}
	r, err := newResolver(&cfg)
	require.NoError(t, err)
	tests := []struct {
		name string
		info pipeline.TableInfo
		want pipeline.TableDecision
	}{
		{
			"captured",
			pipeline.TableInfo{Schema: "public", Table: "users", HasKey: true},
			pipeline.TableDecision{Capture: true, Source: change.Source{DB: "app", Schema: "public", Table: "users"}},
		},
		{
			"topic too long",
			pipeline.TableInfo{Schema: "public", Table: long, HasKey: true},
			pipeline.TableDecision{Skip: pipeline.SkipIdentifierInvalid},
		},
		{
			"long name outside the kafka sink's globs",
			pipeline.TableInfo{Schema: "other", Table: long, HasKey: true},
			pipeline.TableDecision{Capture: true, Source: change.Source{DB: "app", Schema: "other", Table: long}},
		},
		{
			"mapped",
			pipeline.TableInfo{Schema: "public", Table: "bad name", HasKey: true},
			pipeline.TableDecision{
				Capture: true,
				Source:  change.Source{DB: "app", Schema: "public", Table: "bad_name"},
			},
		},
		{
			"keyless",
			pipeline.TableInfo{Schema: "public", Table: "k"},
			pipeline.TableDecision{Skip: pipeline.SkipKeyless},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, r.Resolve(tc.info))
		})
	}
}

func TestLiveResolver_Swap(t *testing.T) {
	t.Parallel()
	mk := func(include string) *topicResolver {
		cfg := config.Config{Source: config.Source{Name: "app"}, Tables: config.Tables{Include: []string{include}}}
		r, err := newResolver(&cfg)
		require.NoError(t, err)
		return r
	}
	live := newLiveResolver(mk("public.a"))
	info := pipeline.TableInfo{Schema: "public", Table: "b", HasKey: true}
	require.False(t, live.Resolve(info).Capture)

	live.swap(mk("public.*"))

	assert.True(t, live.Resolve(info).Capture)
}

func TestCollisionFinding(t *testing.T) {
	t.Parallel()
	src := func(s, tb string) change.Source { return change.Source{DB: "app", Schema: s, Table: tb} }
	ok := collisionFinding([]snapshot.Table{
		{Name: "public.a", Source: src("public", "a")}, {Name: "public.b", Source: src("public", "b")},
	})
	assert.Equal(t, pgsql.StatusOK, ok.Status)

	bad := collisionFinding([]snapshot.Table{
		{Name: "public.a", Source: src("public", "a")}, {Name: "public.a b", Source: src("public", "a")},
	})
	assert.Equal(t, pgsql.StatusFail, bad.Status)
	assert.Equal(t, codeIdentifierInvalid, bad.Code)
	assert.Contains(t, bad.Message, "public.a and public.a b map to public.a")
}

func TestStatusIntervalFinding(t *testing.T) {
	t.Parallel()
	cfg := config.Default()
	cfg.Stream.StatusInterval = config.Duration(10 * time.Second)
	tests := []struct {
		name    string
		timeout time.Duration
		want    pgsql.Status
	}{
		{"below half", time.Minute, pgsql.StatusOK},
		{"not below half", 20 * time.Second, pgsql.StatusFail},
		{"timeout disabled", 0, pgsql.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := statusIntervalFinding(&cfg, pipeline.ServerSettings{WALSenderTimeout: tc.timeout})
			assert.Equal(t, tc.want, f.Status)
			if tc.want == pgsql.StatusFail {
				assert.Equal(t, codeStatusInterval, f.Code)
			}
		})
	}
}

func TestJudge(t *testing.T) {
	t.Parallel()
	report := pgsql.Report{Findings: []pgsql.Finding{
		{Check: "wal_level", Status: pgsql.StatusOK, Message: "logical"},
		{Check: "wal_retention", Status: pgsql.StatusWarn, Code: "wal_retention_unlimited", Message: "unlimited"},
		{
			Check: "keyless_tables", Status: pgsql.StatusFail, Code: "table_keyless", Message: "keyless",
			Tables: []string{"public.k"},
		},
		{Check: "identifiers", Status: pgsql.StatusFail, Code: "identifier_invalid", Message: "bad"},
	}}

	err := judge(t.Context(), testutil.DiscardLogger(), &report)

	require.Error(t, err)
	assert.Equal(t, "table_keyless", errorCode(err))
	assert.Contains(t, err.Error(), "keyless_tables [table_keyless]: keyless (public.k)")
	assert.Contains(t, err.Error(), "identifiers [identifier_invalid]")
	require.NoError(t, judge(t.Context(), testutil.DiscardLogger(), &pgsql.Report{Findings: report.Findings[:2]}))
}

func TestPrintReport(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	printReport(&buf, &pgsql.Report{Findings: []pgsql.Finding{
		{Check: "wal_level", Status: pgsql.StatusOK, Message: "wal_level is logical"},
		{
			Check: "keyless_tables", Status: pgsql.StatusFail, Code: "table_keyless", Message: "keyless",
			Tables: []string{"public.k"},
		},
	}})

	out := buf.String()
	assert.Contains(t, out, "OK    wal_level")
	assert.Contains(t, out, "FAIL  keyless_tables")
	assert.Contains(t, out, "[table_keyless]")
	assert.Contains(t, out, "- public.k")
	assert.Contains(t, out, "result: FAILED")
}

type fakeSource struct {
	fail error
}

func (f *fakeSource) StartReplication(context.Context, pipeline.StartOptions) error { return f.fail }

func (f *fakeSource) Receive(context.Context) (pipeline.ReplicationMessage, error) {
	return pipeline.ReplicationMessage{}, f.fail
}

func (f *fakeSource) SendStandbyStatus(context.Context, pipeline.StandbyStatus) error { return f.fail }

func TestReplicationCheck(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clk := clock.NewFake(start)
	cfg := config.Default()
	cfg.Stream.StatusInterval = config.Duration(time.Second)
	src := &fakeSource{}
	s := &service{cfg: &cfg, repl: newReplActivity(src, clk)}

	require.False(t, s.replicationCheck(clk.Now()).OK, "before START_REPLICATION")
	require.NoError(t, s.repl.StartReplication(t.Context(), pipeline.StartOptions{}))
	require.False(t, s.replicationCheck(clk.Now()).OK, "reader not running")
	s.readerRunning.Store(true)
	require.True(t, s.replicationCheck(clk.Now()).OK)

	clk.Advance(4 * time.Second)
	c := s.replicationCheck(clk.Now())
	require.False(t, c.OK, "idle beyond three status intervals")
	assert.Equal(t, codeReplicationNotStarted, c.Code)

	require.NoError(t, s.repl.SendStandbyStatus(t.Context(), pipeline.StandbyStatus{}))
	require.True(t, s.replicationCheck(clk.Now()).OK, "a status update the server accepted is activity")

	clk.Advance(4 * time.Second)
	src.fail = errors.New("connection reset")
	_, err := s.repl.Receive(t.Context())
	require.Error(t, err)
	assert.False(t, s.replicationCheck(clk.Now()).OK, "failed calls are no activity")
}

func TestRuntimeView_Starting(t *testing.T) {
	t.Parallel()
	v := newRuntimeView(config.Default(), clock.NewFake(time.Unix(0, 0)))

	checks := v.readiness(t.Context())
	_, err := v.status(t.Context())

	require.Len(t, checks, 2)
	assert.Equal(t, httpadmin.Check{Name: "config", OK: true}, checks[0])
	assert.False(t, checks[1].OK)
	assert.ErrorIs(t, err, errStarting)
}

type fakeLister struct {
	names   []string
	skipped map[string]string
	err     error
}

func (f *fakeLister) captured(context.Context, pipeline.TableResolver) ([]string, map[string]string, error) {
	return f.names, f.skipped, f.err
}

type fakeQueue struct {
	mu     sync.Mutex
	queued [][]string
}

func (f *fakeQueue) AddTables(_ context.Context, tables []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queued = append(f.queued, tables)
	return nil
}

func TestRefresher_Refresh(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		added  bool
		rounds []fakeLister
		want   [][]string
	}{
		{
			name:  "new tables are queued once",
			added: true,
			rounds: []fakeLister{
				{names: []string{"public.a", "public.c", "public.b"}},
				{names: []string{"public.a", "public.b", "public.c"}},
			},
			want: [][]string{{"public.b", "public.c"}},
		},
		{
			name:   "added_tables off queues nothing",
			rounds: []fakeLister{{names: []string{"public.a", "public.b"}}},
		},
		{
			name:  "a failed round changes nothing",
			added: true,
			rounds: []fakeLister{
				{err: errors.New("connection refused")},
				{names: []string{"public.a", "public.b"}},
			},
			want: [][]string{{"public.b"}},
		},
		{
			name:  "a removed and re-added table is new again",
			added: true,
			rounds: []fakeLister{
				{names: []string{}},
				{names: []string{"public.a"}},
			},
			want: [][]string{{"public.a"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q := &fakeQueue{}
			r := &refresher{
				queue: q, added: tc.added, log: testutil.DiscardLogger(),
				known: map[string]bool{"public.a": true}, skipped: map[string]bool{},
			}
			for i := range tc.rounds {
				r.lister = &tc.rounds[i]
				r.refresh(t.Context())
			}
			assert.Equal(t, tc.want, q.queued)
		})
	}
}

func TestRefresher_LogsSkippedOnce(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	r := &refresher{log: newTestLogger(&buf), skipped: map[string]bool{}}

	r.logSkipped(map[string]string{"public.k": string(pipeline.SkipKeyless)})
	r.logSkipped(map[string]string{"public.k": string(pipeline.SkipKeyless)})
	r.logSkipped(map[string]string{})
	r.logSkipped(map[string]string{"public.k": string(pipeline.SkipKeyless)})

	assert.Equal(t, 2, strings.Count(buf.String(), "table skipped"))
	assert.Contains(t, buf.String(), "emit_unkeyed")
}

func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}

func TestRefresher_ReloadSwapsTheResolver(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, minimalConfig)
	cfg, err := loadConfig(path, false)
	require.NoError(t, err)
	base, err := newResolver(&cfg)
	require.NoError(t, err)
	live := newLiveResolver(base)
	var buf bytes.Buffer
	r := &refresher{resolver: live, cfgPath: path, log: newTestLogger(&buf)}
	other := pipeline.TableInfo{Schema: "other", Table: "t", HasKey: true}
	require.False(t, live.Resolve(other).Capture)

	require.NoError(t, os.WriteFile(path, []byte(strings.Replace(minimalConfig, `include: ["public.*"]`,
		`include: ["public.*", "other.*"]`, 1)), 0o600))
	r.reload()
	assert.True(t, live.Resolve(other).Capture, "reloaded globs apply")

	require.NoError(t, os.WriteFile(path, []byte("source: {name: Bad}\n"), 0o600))
	r.reload()
	assert.True(t, live.Resolve(other).Capture, "an invalid file keeps the current resolver")
	assert.Contains(t, buf.String(), "configuration not reloaded")
}
