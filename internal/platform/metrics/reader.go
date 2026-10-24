// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Reader exports the stream reader's counters.
type Reader struct {
	tables           *Capper
	types            *Capper
	unknownTypes     *prometheus.CounterVec
	unknownMessages  *prometheus.CounterVec
	truncateSkipped  *prometheus.CounterVec
	tablesSkipped    *prometheus.CounterVec
	spilledTx        prometheus.Counter
	spilledBytes     prometheus.Counter
	spilledTxBytes   prometheus.Histogram
	unknownMsgLabels *Capper
}

// NewReader creates the reader collectors and registers them on reg.
func NewReader(reg prometheus.Registerer, opts ...Option) (*Reader, error) {
	o := newOptions(opts)
	r := &Reader{
		tables: o.tables,
		types:  NewCapper(DefaultLabelLimit),
		unknownTypes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_unknown_types_total",
			Help: "Column types outside the type mapping, counted once per type.",
		}, []string{"type"}),
		unknownMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_unknown_messages_total",
			Help: "pgoutput message types outside protocol version 1, counted once per type and skipped.",
		}, []string{"type"}),
		truncateSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_truncate_skipped_total",
			Help: "TRUNCATE messages not emitted because events.emit_truncate is off.",
		}, []string{labelTable}),
		tablesSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_tables_skipped_total",
			Help: "Published tables that are not captured, counted once per table and reason.",
		}, []string{"table", "reason"}),
		spilledTx: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wake_spilled_transactions_total",
			Help: "Transactions moved to a spill file.",
		}),
		spilledBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wake_spilled_bytes_total",
			Help: "Bytes of transactions moved to a spill file.",
		}),
		spilledTxBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wake_spilled_transaction_size_bytes",
			Help:    "Size of each transaction moved to a spill file.",
			Buckets: SizeBuckets(),
		}),
		unknownMsgLabels: NewCapper(DefaultLabelLimit),
	}
	err := register(reg, r.unknownTypes, r.unknownMessages, r.truncateSkipped, r.tablesSkipped,
		r.spilledTx, r.spilledBytes, r.spilledTxBytes)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) UnknownType(typeName string) {
	r.unknownTypes.WithLabelValues(r.types.Label(typeName)).Inc()
}

func (r *Reader) UnknownMessage(msgType byte) {
	r.unknownMessages.WithLabelValues(r.unknownMsgLabels.Label(messageLabel(msgType))).Inc()
}

func (r *Reader) TruncateSkipped(table string) {
	r.truncateSkipped.WithLabelValues(r.tables.Label(table)).Inc()
}

func (r *Reader) TableSkipped(table string, reason pipeline.SkipReason) {
	r.tablesSkipped.WithLabelValues(r.tables.Label(table), strings.ToValidUTF8(string(reason), "?")).Inc()
}

func (r *Reader) Spilled(bytes int64) {
	r.spilledTx.Inc()
	if bytes > 0 {
		r.spilledBytes.Add(float64(bytes))
		r.spilledTxBytes.Observe(float64(bytes))
	}
}

func messageLabel(b byte) string {
	if b > ' ' && b < 0x7f {
		return string(rune(b))
	}
	return fmt.Sprintf("0x%02x", b)
}
