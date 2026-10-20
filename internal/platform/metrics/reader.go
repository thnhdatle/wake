// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Reader exports the stream reader's counters. It satisfies
// stream.ReaderMetrics. The table and type labels come from user data and
// are capped (see Capper).
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

// NewReader creates the reader collectors and registers them on reg:
// wake_unknown_types_total{type}, wake_unknown_messages_total{type},
// wake_truncate_skipped_total{table}, wake_tables_skipped_total{table,reason},
// wake_spilled_transactions_total, wake_spilled_bytes_total and
// wake_spilled_transaction_size_bytes.
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

// UnknownType counts a column type outside the type mapping.
func (r *Reader) UnknownType(typeName string) {
	r.unknownTypes.WithLabelValues(r.types.Label(typeName)).Inc()
}

// UnknownMessage counts a pgoutput message type outside protocol version 1.
func (r *Reader) UnknownMessage(msgType byte) {
	r.unknownMessages.WithLabelValues(r.unknownMsgLabels.Label(messageLabel(msgType))).Inc()
}

// TruncateSkipped counts a TRUNCATE that was not emitted.
func (r *Reader) TruncateSkipped(table string) {
	r.truncateSkipped.WithLabelValues(r.tables.Label(table)).Inc()
}

// TableSkipped counts a published table that is not captured.
func (r *Reader) TableSkipped(table string, reason pipeline.SkipReason) {
	r.tablesSkipped.WithLabelValues(r.tables.Label(table), strings.ToValidUTF8(string(reason), "?")).Inc()
}

// Spilled counts a transaction moved to a spill file and its size.
func (r *Reader) Spilled(bytes int64) {
	r.spilledTx.Inc()
	if bytes > 0 {
		r.spilledBytes.Add(float64(bytes))
		r.spilledTxBytes.Observe(float64(bytes))
	}
}

// messageLabel renders a pgoutput message type byte: the letter when it is
// printable ASCII, else its hex value.
func messageLabel(b byte) string {
	if b > ' ' && b < 0x7f {
		return string(rune(b))
	}
	return fmt.Sprintf("0x%02x", b)
}
