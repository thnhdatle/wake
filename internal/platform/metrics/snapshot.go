// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

const otherTable = "_other"

// Snapshot exports the snapshot counters.
type Snapshot struct {
	tables       *Capper
	rows         *prometheus.CounterVec
	requests     *prometheus.CounterVec
	chunkRetries *prometheus.CounterVec
}

// NewSnapshot creates the snapshot collectors and registers them on reg.
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

func (s *Snapshot) RowsAcked(table string, n uint64) {
	s.rows.WithLabelValues(s.label(table)).Add(float64(n))
}

func (s *Snapshot) Request(table string) {
	s.requests.WithLabelValues(s.label(table)).Inc()
}

func (s *Snapshot) ChunkRetry(table string) {
	s.chunkRetries.WithLabelValues(s.label(table)).Inc()
}
