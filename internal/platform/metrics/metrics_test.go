// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/app/lag"
	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/app/stream"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/buildinfo"
	"github.com/thnhdatle/wake/internal/platform/metrics"
)

var (
	_ lag.Observer                      = (*metrics.Lag)(nil)
	_ lag.LatencyObserver               = (*metrics.Lag)(nil)
	_ stream.ReaderMetrics              = (*metrics.Reader)(nil)
	_ snapshot.Metrics                  = (*metrics.Snapshot)(nil)
	_ prometheus.Registerer             = (*prometheus.Registry)(nil)
	_ interface{ Label(string) string } = (*metrics.Capper)(nil)
)

// family returns the gathered metric family named name, failing the test if
// it is absent.
func family(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range fams {
		if f.GetName() == name {
			return f
		}
	}
	require.Failf(t, "metric family missing", "%s", name)
	return nil
}

func hasFamily(t *testing.T, reg *prometheus.Registry, name string) bool {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range fams {
		if f.GetName() == name {
			return true
		}
	}
	return false
}

func labelsOf(m *dto.Metric) map[string]string {
	out := map[string]string{}
	for _, lp := range m.GetLabel() {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

// find returns the metric of fam whose labels equal want.
func find(t *testing.T, fam *dto.MetricFamily, want map[string]string) *dto.Metric {
	t.Helper()
	for _, m := range fam.GetMetric() {
		got := labelsOf(m)
		if len(got) != len(want) {
			continue
		}
		match := true
		for k, v := range want {
			if got[k] != v {
				match = false
			}
		}
		if match {
			return m
		}
	}
	require.Failf(t, "series missing", "%s %v", fam.GetName(), want)
	return nil
}

func counter(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	return find(t, family(t, reg, name), labels).GetCounter().GetValue()
}

func gauge(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	return find(t, family(t, reg, name), labels).GetGauge().GetValue()
}

func histogram(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) *dto.Histogram {
	t.Helper()
	return find(t, family(t, reg, name), labels).GetHistogram()
}

func TestNewRegistry(t *testing.T) {
	t.Parallel()
	info := buildinfo.Info{Version: "1.2.3", Commit: "abc", GoVersion: "go1.27"}
	reg, err := metrics.NewRegistry(info)
	require.NoError(t, err)

	got := gauge(t, reg, "wake_build_info", map[string]string{
		"version": "1.2.3", "commit": "abc", "go_version": "go1.27",
	})
	assert.InDelta(t, 1, got, 0)
	assert.True(t, hasFamily(t, reg, "go_goroutines"), "Go collector")
	assert.True(t, hasFamily(t, reg, "go_info"), "Go collector")
}

func TestConstructors_NilRegisterer(t *testing.T) {
	t.Parallel()
	_, err := metrics.NewLag(nil)
	require.ErrorIs(t, err, metrics.ErrNilRegisterer)
	_, err = metrics.NewReader(nil)
	require.ErrorIs(t, err, metrics.ErrNilRegisterer)
	_, err = metrics.NewSnapshot(nil)
	require.ErrorIs(t, err, metrics.ErrNilRegisterer)
	_, err = metrics.NewSinkMetrics(nil)
	require.ErrorIs(t, err, metrics.ErrNilRegisterer)
}

func TestConstructors_DuplicateRegistration(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	_, err := metrics.NewLag(reg)
	require.NoError(t, err)
	_, err = metrics.NewLag(reg)
	require.Error(t, err)

	_, err = metrics.NewSnapshot(reg)
	require.NoError(t, err)
	_, err = metrics.NewSnapshot(reg)
	require.Error(t, err)
}

func TestLag(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	l, err := metrics.NewLag(reg)
	require.NoError(t, err)

	assert.InDelta(t, -1, gauge(t, reg, "wake_slot_wal_status", nil), 0, "unknown before the first poll")

	l.ObserveSlot(4096, "extended")
	assert.InDelta(t, 4096, gauge(t, reg, "wake_slot_lag_bytes", nil), 0)
	assert.InDelta(t, 1, gauge(t, reg, "wake_slot_wal_status", nil), 0)

	l.ObserveOldestUnackedAge(1500 * time.Millisecond)
	assert.InDelta(t, 1.5, gauge(t, reg, "wake_oldest_unacked_age_seconds", nil), 1e-9)

	l.ObservePollError(assert.AnError)
	l.ObservePollError(assert.AnError)
	assert.InDelta(t, 2, counter(t, reg, "wake_lag_poll_errors_total", nil), 0)

	l.SetAlertActive("slot_lag_high", true)
	l.SetAlertActive("slot_lost", false)
	assert.InDelta(t, 1, gauge(t, reg, "wake_alert_active", map[string]string{"rule": "slot_lag_high"}), 0)
	assert.InDelta(t, 0, gauge(t, reg, "wake_alert_active", map[string]string{"rule": "slot_lost"}), 0)
	l.SetAlertActive("slot_lag_high", false)
	assert.InDelta(t, 0, gauge(t, reg, "wake_alert_active", map[string]string{"rule": "slot_lag_high"}), 0)

	l.ObserveEndToEndLatency("kafka", 20*time.Millisecond)
	l.ObserveEndToEndLatency("kafka", 3*time.Second)
	h := histogram(t, reg, "wake_end_to_end_latency_seconds", map[string]string{"sink": "kafka"})
	assert.Equal(t, uint64(2), h.GetSampleCount())
	assert.InDelta(t, 3.02, h.GetSampleSum(), 1e-9)
	assert.Len(t, h.GetBucket(), len(metrics.LatencyBuckets()))
}

func TestLag_WALStatusMatchesApp(t *testing.T) {
	t.Parallel()
	for _, status := range []string{"reserved", "extended", "unreserved", "lost", "", "bogus"} {
		t.Run(status, func(t *testing.T) {
			t.Parallel()
			reg := prometheus.NewRegistry()
			l, err := metrics.NewLag(reg)
			require.NoError(t, err)
			l.ObserveSlot(0, status)
			assert.InDelta(t, float64(lag.WALStatusLevel(status)), gauge(t, reg, "wake_slot_wal_status", nil), 0)
		})
	}
}

func TestReader(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	r, err := metrics.NewReader(reg)
	require.NoError(t, err)

	r.UnknownType("hstore")
	r.UnknownType("hstore")
	r.UnknownType("bad\xffname")
	assert.InDelta(t, 2, counter(t, reg, "wake_unknown_types_total", map[string]string{"type": "hstore"}), 0)
	assert.InDelta(t, 1, counter(t, reg, "wake_unknown_types_total", map[string]string{"type": "bad?name"}), 0)

	r.UnknownMessage('X')
	r.UnknownMessage(0x01)
	assert.InDelta(t, 1, counter(t, reg, "wake_unknown_messages_total", map[string]string{"type": "X"}), 0)
	assert.InDelta(t, 1, counter(t, reg, "wake_unknown_messages_total", map[string]string{"type": "0x01"}), 0)

	r.TruncateSkipped("public.orders")
	assert.InDelta(t, 1, counter(t, reg, "wake_truncate_skipped_total", map[string]string{"table": "public.orders"}), 0)

	r.TableSkipped("public.audit", pipeline.SkipKeyless)
	r.TableSkipped("public.audit", pipeline.SkipNotCaptured)
	got := counter(t, reg, "wake_tables_skipped_total",
		map[string]string{"table": "public.audit", "reason": "table_keyless"})
	assert.InDelta(t, 1, got, 0)

	r.Spilled(1000)
	r.Spilled(3000)
	r.Spilled(0)
	assert.InDelta(t, 3, counter(t, reg, "wake_spilled_transactions_total", nil), 0)
	assert.InDelta(t, 4000, counter(t, reg, "wake_spilled_bytes_total", nil), 0)
	h := histogram(t, reg, "wake_spilled_transaction_size_bytes", nil)
	assert.Equal(t, uint64(2), h.GetSampleCount())
}

func TestReader_TableLabelCapped(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	r, err := metrics.NewReader(reg, metrics.WithTableCapper(metrics.NewCapper(2)))
	require.NoError(t, err)
	for _, tbl := range []string{"a", "b", "c", "d", "a"} {
		r.TruncateSkipped(tbl)
	}
	fam := family(t, reg, "wake_truncate_skipped_total")
	assert.Len(t, fam.GetMetric(), 3)
	assert.InDelta(t, 2, counter(t, reg, "wake_truncate_skipped_total", map[string]string{"table": "a"}), 0)
	assert.InDelta(t, 2, counter(t, reg, "wake_truncate_skipped_total", map[string]string{"table": "other"}), 0)
}

func TestSnapshot(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	capper := metrics.NewCapper(1)
	s, err := metrics.NewSnapshot(reg, metrics.WithTableCapper(capper))
	require.NoError(t, err)

	s.RowsAcked("public.a", 10)
	s.RowsAcked("public.a", 5)
	s.RowsAcked("public.b", 7)
	s.Request("public.a")
	s.Request(snapshot.OtherTable)
	s.Request(snapshot.OtherTable)
	s.ChunkRetry("public.a")

	assert.InDelta(t, 15, counter(t, reg, "wake_snapshot_rows_total", map[string]string{"table": "public.a"}), 0)
	assert.InDelta(t, 7, counter(t, reg, "wake_snapshot_rows_total", map[string]string{"table": "other"}), 0,
		"second table beyond the cap")
	assert.InDelta(t, 1, counter(t, reg, "wake_snapshot_requests_total", map[string]string{"table": "public.a"}), 0)
	assert.InDelta(t, 2, counter(t, reg, "wake_snapshot_requests_total", map[string]string{"table": "_other"}), 0,
		"uncaptured requests keep _other and do not use the cap")
	assert.InDelta(t, 1, counter(t, reg, "wake_snapshot_chunk_retries_total", map[string]string{"table": "public.a"}), 0)
	assert.Equal(t, 1, capper.Len())
}

func TestSinkMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m, err := metrics.NewSinkMetrics(reg)
	require.NoError(t, err)
	k := map[string]string{"sink": "kafka"}

	m.Sent("kafka", 10)
	m.Sent("kafka", 0)
	m.Sent("kafka", -3)
	m.Acked("kafka", 8)
	m.Retry("kafka", 2)
	m.DeadLettered("kafka", 1)
	m.Inflight("kafka", 5)
	m.Inflight("kafka", 2)
	m.SendDuration("kafka", 40*time.Millisecond)
	m.Sent("nats", 1)

	assert.InDelta(t, 10, counter(t, reg, "wake_sink_events_sent_total", k), 0)
	assert.InDelta(t, 1, counter(t, reg, "wake_sink_events_sent_total", map[string]string{"sink": "nats"}), 0)
	assert.InDelta(t, 8, counter(t, reg, "wake_sink_events_acked_total", k), 0)
	assert.InDelta(t, 2, counter(t, reg, "wake_sink_retries_total",
		map[string]string{"sink": "kafka", "reason": metrics.RetryReasonUnspecified}), 0)
	assert.InDelta(t, 1, counter(t, reg, "wake_sink_dead_lettered_total", k), 0)
	assert.InDelta(t, 2, gauge(t, reg, "wake_sink_inflight", k), 0)
	h := histogram(t, reg, "wake_sink_send_duration_seconds", k)
	assert.Equal(t, uint64(1), h.GetSampleCount())
}

// sinkObserver is the observer method set the sink adapters share (the
// webhook sink adds ObserveDeadLettered).
type sinkObserver interface {
	ObserveSent(sink string, events, bytes int)
	ObserveAcked(sink string, events int)
	ObserveRetry(sink, reason string)
	ObserveLatency(sink string, d time.Duration)
	SetInflight(sink string, events int)
	ObserveDeadLettered(sink string, events int)
}

var _ sinkObserver = (*metrics.SinkMetrics)(nil)

func TestSinkMetrics_ObserverMethods(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m, err := metrics.NewSinkMetrics(reg)
	require.NoError(t, err)
	var obs sinkObserver = m
	k := map[string]string{"sink": "hooks"}

	obs.ObserveSent("hooks", 3, 300)
	obs.ObserveSent("hooks", 2, 0)
	obs.ObserveAcked("hooks", 4)
	obs.ObserveRetry("hooks", "server_error")
	obs.ObserveRetry("hooks", "server_error")
	obs.ObserveRetry("hooks", "")
	obs.ObserveLatency("hooks", 20*time.Millisecond)
	obs.SetInflight("hooks", 7)
	obs.ObserveDeadLettered("hooks", 2)

	assert.InDelta(t, 5, counter(t, reg, "wake_sink_events_sent_total", k), 0)
	assert.InDelta(t, 300, counter(t, reg, "wake_sink_bytes_sent_total", k), 0)
	assert.InDelta(t, 4, counter(t, reg, "wake_sink_events_acked_total", k), 0)
	assert.InDelta(t, 2, counter(t, reg, "wake_sink_retries_total",
		map[string]string{"sink": "hooks", "reason": "server_error"}), 0)
	assert.InDelta(t, 1, counter(t, reg, "wake_sink_retries_total",
		map[string]string{"sink": "hooks", "reason": metrics.RetryReasonUnspecified}), 0)
	assert.InDelta(t, 7, gauge(t, reg, "wake_sink_inflight", k), 0)
	assert.InDelta(t, 2, counter(t, reg, "wake_sink_dead_lettered_total", k), 0)
	assert.Equal(t, uint64(1), histogram(t, reg, "wake_sink_send_duration_seconds", k).GetSampleCount())
}

func TestBuckets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		buckets  []float64
		min, max float64
	}{
		{"latency", metrics.LatencyBuckets(), 0.001, 60},
		{"size", metrics.SizeBuckets(), 64, 64 << 20},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NotEmpty(t, tc.buckets)
			assert.InDelta(t, tc.min, tc.buckets[0], 0)
			assert.InDelta(t, tc.max, tc.buckets[len(tc.buckets)-1], 0)
			assert.IsIncreasing(t, tc.buckets)
		})
	}
	b := metrics.LatencyBuckets()
	b[0] = 99
	assert.InDelta(t, 0.001, metrics.LatencyBuckets()[0], 0, "callers get a copy")
	s := metrics.SizeBuckets()
	s[0] = 99
	assert.InDelta(t, 64, metrics.SizeBuckets()[0], 0, "callers get a copy")
}
