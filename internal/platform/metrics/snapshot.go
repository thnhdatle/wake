// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// otherTable is the table label the snapshotter passes for uncaptured or
// unparsable tables (snapshot.OtherTable); it is never capped.
const otherTable = "_other"

// Snapshot exports the snapshot counters. It satisfies snapshot.Metrics. The
// table label is capped (see Capper); "_other" passes through.
type Snapshot struct {
	tables       *Capper
	rows         *prometheus.CounterVec
	requests     *prometheus.CounterVec
	chunkRetries *prometheus.CounterVec
}

// NewSnapshot creates the snapshot collectors and registers them on reg:
// wake_snapshot_rows_total{table}, wake_snapshot_requests_total{table} and
// wake_snapshot_chunk_retries_total{table}. WithTableCapper shares the
// capped table set with other types.
func NewSnapshot(reg prometheus.Registerer, opts ...Option) (*Snapshot, error) {
	o := newOptions(opts)
	s := &Snapshot{
		tables: o.tables,
		rows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_snapshot_rows_total",
			Help: "Snapshot rows whose events every sink acknowledged.",
		}, []string{labelTable}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_snapshot_requests_total",
			Help: "Table snapshot requests; table is _other for uncaptured tables.",
		}, []string{labelTable}),
		chunkRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_snapshot_chunk_retries_total",
			Help: "Snapshot chunks discarded and run again.",
		}, []string{labelTable}),
	}
	if err := register(reg, s.rows, s.requests, s.chunkRetries); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Snapshot) label(table string) string {
	if table == otherTable {
		return otherTable
	}
	return s.tables.Label(table)
}

// RowsAcked counts snapshot rows acknowledged by every sink.
func (s *Snapshot) RowsAcked(table string, n uint64) {
	s.rows.WithLabelValues(s.label(table)).Add(float64(n))
}

// Request counts a table snapshot request.
func (s *Snapshot) Request(table string) {
	s.requests.WithLabelValues(s.label(table)).Inc()
}

// ChunkRetry counts a chunk discarded and re-run.
func (s *Snapshot) ChunkRetry(table string) {
	s.chunkRetries.WithLabelValues(s.label(table)).Inc()
}
