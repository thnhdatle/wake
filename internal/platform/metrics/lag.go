// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// WAL status levels of wake_slot_wal_status; the same mapping as
// lag.WALStatusLevel (a test keeps them equal).
const (
	walStatusUnknown    = -1
	walStatusReserved   = 0
	walStatusExtended   = 1
	walStatusUnreserved = 2
	walStatusLost       = 3
)

// Lag exports the lag monitor's values and the end-to-end latency. It
// satisfies lag.Observer and lag.LatencyObserver.
type Lag struct {
	slotLag     prometheus.Gauge
	walStatus   prometheus.Gauge
	oldestAge   prometheus.Gauge
	pollErrors  prometheus.Counter
	alertActive *prometheus.GaugeVec
	latency     *prometheus.HistogramVec
}

// NewLag creates the lag collectors and registers them on reg:
// wake_slot_lag_bytes, wake_slot_wal_status (-1 until the first poll),
// wake_oldest_unacked_age_seconds, wake_lag_poll_errors_total,
// wake_alert_active{rule} and wake_end_to_end_latency_seconds{sink}.
func NewLag(reg prometheus.Registerer) (*Lag, error) {
	l := &Lag{
		slotLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wake_slot_lag_bytes",
			Help: "WAL bytes between pg_current_wal_lsn() and the slot's confirmed_flush_lsn.",
		}),
		walStatus: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wake_slot_wal_status",
			Help: "Slot wal_status: reserved 0, extended 1, unreserved 2, lost 3, unknown -1.",
		}),
		oldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wake_oldest_unacked_age_seconds",
			Help: "Age of the oldest delivery not yet acknowledged by every sink; 0 when none is pending.",
		}),
		pollErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wake_lag_poll_errors_total",
			Help: "Failed slot polls of the lag monitor.",
		}),
		alertActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wake_alert_active",
			Help: "1 while the alert rule is active, else 0.",
		}, []string{"rule"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wake_end_to_end_latency_seconds",
			Help:    "Commit to sink acknowledgement latency (host clock minus ts_ms).",
			Buckets: LatencyBuckets(),
		}, []string{labelSink}),
	}
	l.walStatus.Set(walStatusUnknown)
	if err := register(reg, l.slotLag, l.walStatus, l.oldestAge, l.pollErrors, l.alertActive, l.latency); err != nil {
		return nil, err
	}
	return l, nil
}

// ObserveSlot sets wake_slot_lag_bytes and wake_slot_wal_status.
func (l *Lag) ObserveSlot(lagBytes int64, walStatus string) {
	l.slotLag.Set(float64(lagBytes))
	l.walStatus.Set(walStatusLevel(walStatus))
}

// ObserveOldestUnackedAge sets wake_oldest_unacked_age_seconds.
func (l *Lag) ObserveOldestUnackedAge(age time.Duration) {
	l.oldestAge.Set(age.Seconds())
}

// ObservePollError counts a failed slot poll.
func (l *Lag) ObservePollError(error) {
	l.pollErrors.Inc()
}

// SetAlertActive sets wake_alert_active{rule} to 1 or 0.
func (l *Lag) SetAlertActive(rule string, active bool) {
	v := 0.0
	if active {
		v = 1
	}
	l.alertActive.WithLabelValues(rule).Set(v)
}

// ObserveEndToEndLatency records one acknowledged event's latency for sink.
func (l *Lag) ObserveEndToEndLatency(sink string, latency time.Duration) {
	l.latency.WithLabelValues(sink).Observe(latency.Seconds())
}

func walStatusLevel(s string) float64 {
	switch s {
	case "reserved":
		return walStatusReserved
	case "extended":
		return walStatusExtended
	case "unreserved":
		return walStatusUnreserved
	case "lost":
		return walStatusLost
	default:
		return walStatusUnknown
	}
}
