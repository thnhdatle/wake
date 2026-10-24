// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"fmt"
	"time"

	"github.com/thnhdatle/wake/internal/platform/clock"
)

type Observer interface {
	ObserveSlot(lagBytes int64, walStatus string)
	ObserveOldestUnackedAge(age time.Duration)
	ObservePollError(err error)
	SetAlertActive(rule string, active bool)
}

type LatencyObserver interface {
	ObserveEndToEndLatency(sink string, latency time.Duration)
}

const (
	WALStatusLevelUnknown    = -1
	WALStatusLevelReserved   = 0
	WALStatusLevelExtended   = 1
	WALStatusLevelUnreserved = 2
	WALStatusLevelLost       = 3
)

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

// LatencyRecorder turns an acknowledged event into an end-to-end latency.
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

func (r *LatencyRecorder) Record(sink string, tsMS int64) {
	latency := max(r.clock.Now().Sub(time.UnixMilli(tsMS)), 0)
	r.observer.ObserveEndToEndLatency(sink, latency)
}

type nopObserver struct{}

func (nopObserver) ObserveSlot(int64, string)             {}
func (nopObserver) ObserveOldestUnackedAge(time.Duration) {}
func (nopObserver) ObservePollError(error)                {}
func (nopObserver) SetAlertActive(string, bool)           {}
