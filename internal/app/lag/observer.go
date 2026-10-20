// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"fmt"
	"time"

	"github.com/thnhdatle/wake/internal/platform/clock"
)

// Observer receives the values the monitor exports as metrics. The
// metrics adapter implements it with Prometheus collectors; the app layer
// never imports a metrics library. Calls come from the monitor's goroutine
// only, one evaluation at a time.
type Observer interface {
	// ObserveSlot reports a successful slot poll: lagBytes is
	// pg_current_wal_lsn() - confirmed_flush_lsn (wake_slot_lag_bytes) and
	// walStatus the slot's wal_status (wake_slot_wal_status, see
	// WALStatusLevel).
	ObserveSlot(lagBytes int64, walStatus string)
	// ObserveOldestUnackedAge reports the age of the oldest delivery not yet
	// acknowledged, zero when none is pending
	// (wake_oldest_unacked_age_seconds).
	ObserveOldestUnackedAge(age time.Duration)
	// ObservePollError reports a failed slot poll, e.g. to count it; the
	// monitor keeps running and has already logged it.
	ObservePollError(err error)
	// SetAlertActive reports a rule's state after it changed, and the initial
	// inactive state of every rule when Run starts (wake_alert_active{rule}).
	SetAlertActive(rule string, active bool)
}

// LatencyObserver receives end-to-end latencies, commit to sink
// acknowledgement (wake_end_to_end_latency_seconds{sink}).
type LatencyObserver interface {
	// ObserveEndToEndLatency records one acknowledged event's latency.
	ObserveEndToEndLatency(sink string, latency time.Duration)
}

// WAL status levels exported as wake_slot_wal_status, ordered by severity so
// that an alert can compare against a level.
const (
	WALStatusLevelUnknown    = -1
	WALStatusLevelReserved   = 0
	WALStatusLevelExtended   = 1
	WALStatusLevelUnreserved = 2
	WALStatusLevelLost       = 3
)

// WALStatusLevel maps a pg_replication_slots wal_status to its
// wake_slot_wal_status value: reserved 0, extended 1, unreserved 2, lost 3,
// anything else (empty, unknown) -1.
func WALStatusLevel(walStatus string) int {
	switch walStatus {
	case "reserved":
		return WALStatusLevelReserved
	case "extended":
		return WALStatusLevelExtended
	case "unreserved":
		return WALStatusLevelUnreserved
	case "lost":
		return WALStatusLevelLost
	default:
		return WALStatusLevelUnknown
	}
}

// LatencyRecorder turns an acknowledged event's ts_ms into an end-to-end
// latency on the host clock. Sinks call Record from their ack path; it
// is safe for concurrent use if the LatencyObserver is.
type LatencyRecorder struct {
	observer LatencyObserver
	clock    clock.Clock
}

// NewLatencyRecorder validates the dependencies.
func NewLatencyRecorder(observer LatencyObserver, clk clock.Clock) (*LatencyRecorder, error) {
	if observer == nil || clk == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidOption)
	}
	return &LatencyRecorder{observer: observer, clock: clk}, nil
}

// Record observes host clock - tsMS (Unix milliseconds, the event's ts_ms)
// for one event acknowledged by sink. A negative difference, caused by clock
// skew between the host and Postgres, is recorded as zero.
func (r *LatencyRecorder) Record(sink string, tsMS int64) {
	latency := max(r.clock.Now().Sub(time.UnixMilli(tsMS)), 0)
	r.observer.ObserveEndToEndLatency(sink, latency)
}

type nopObserver struct{}

func (nopObserver) ObserveSlot(int64, string)             {}
func (nopObserver) ObserveOldestUnackedAge(time.Duration) {}
func (nopObserver) ObservePollError(error)                {}
func (nopObserver) SetAlertActive(string, bool)           {}
