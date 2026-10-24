// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

const (
	DefaultInterval      = 15 * time.Second
	DefaultWarnBytes     = 1 << 30
	DefaultCriticalBytes = 10 << 30
	DefaultMaxAckAge     = 5 * time.Minute
)

type SlotReader interface {
	Slot(ctx context.Context, name string) (pipeline.SlotStatus, error)
}

type AckAges interface {
	OldestUnacked() (readAt time.Time, ok bool)
}

type SinkHealthReporter interface {
	Name() string
	Health() pipeline.SinkHealth
}

type MonitorOption func(*Monitor)

func WithInterval(d time.Duration) MonitorOption { return func(m *Monitor) { m.interval = d } }

func WithWarnBytes(n int64) MonitorOption { return func(m *Monitor) { m.warnBytes = n } }

func WithCriticalBytes(n int64) MonitorOption { return func(m *Monitor) { m.criticalBytes = n } }

func WithMaxAckAge(d time.Duration) MonitorOption { return func(m *Monitor) { m.maxAckAge = d } }

func WithSinks(sinks ...SinkHealthReporter) MonitorOption {
	return func(m *Monitor) { m.sinks = append(m.sinks, sinks...) }
}

func WithNotifier(n pipeline.AlertNotifier) MonitorOption { return func(m *Monitor) { m.notifier = n } }

func WithObserver(o Observer) MonitorOption { return func(m *Monitor) { m.observer = o } }

func WithLogger(l *slog.Logger) MonitorOption { return func(m *Monitor) { m.logger = l } }

// Monitor polls the slot lag and evaluates the alert rules.
type Monitor struct {
	slots         SlotReader
	slot          string
	acks          AckAges
	clock         clock.Clock
	interval      time.Duration
	warnBytes     int64
	criticalBytes int64
	maxAckAge     time.Duration
	sinks         []SinkHealthReporter
	notifier      pipeline.AlertNotifier
	observer      Observer
	logger        *slog.Logger

	mu    sync.Mutex
	rules []*rule
}

// NewMonitor validates the dependencies and options.
func NewMonitor(
	slots SlotReader, slot string, acks AckAges, clk clock.Clock, opts ...MonitorOption,
) (*Monitor, error) {
	if slots == nil || acks == nil || clk == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidOption)
	}
	if slot == "" {
		return nil, fmt.Errorf("%w: empty slot name", ErrInvalidOption)
	}
	m := &Monitor{
		slots: slots, slot: slot, acks: acks, clock: clk,
		interval: DefaultInterval, warnBytes: DefaultWarnBytes, criticalBytes: DefaultCriticalBytes,
		maxAckAge: DefaultMaxAckAge, notifier: nopNotifier{}, observer: nopObserver{},
		logger: slog.New(slog.DiscardHandler),
	}
	for _, o := range opts {
		o(m)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	m.rules = []*rule{
		newRule(RuleSlotLagWarn, float64(m.warnBytes)),
		newRule(RuleSlotLagCritical, float64(m.criticalBytes)),
		newRule(RuleMaxAckAge, m.maxAckAge.Seconds()),
		newRule(RuleSinkFatal, 0),
	}
	return m, nil
}

func (m *Monitor) validate() error {
	switch {
	case m.notifier == nil || m.observer == nil || m.logger == nil:
		return fmt.Errorf("%w: nil notifier, observer or logger", ErrInvalidOption)
	case m.interval <= 0:
		return fmt.Errorf("%w: interval %s", ErrInvalidOption, m.interval)
	case m.warnBytes <= 0 || m.criticalBytes <= m.warnBytes:
		return fmt.Errorf("%w: warn bytes %d, critical bytes %d", ErrInvalidOption, m.warnBytes, m.criticalBytes)
	case m.maxAckAge <= 0:
		return fmt.Errorf("%w: max ack age %s", ErrInvalidOption, m.maxAckAge)
	}
	for _, s := range m.sinks {
		if s == nil {
			return fmt.Errorf("%w: nil sink", ErrInvalidOption)
		}
	}
	return nil
}

// Run polls at every interval until ctx ends.
func (m *Monitor) Run(ctx context.Context) error {
	m.mu.Lock()
	for _, r := range m.rules {
		m.observer.SetAlertActive(r.name, r.active)
	}
	m.mu.Unlock()
	m.Poll(ctx)
	ticker := m.clock.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			m.Poll(ctx)
		}
	}
}

// Poll runs one poll and evaluation.
func (m *Monitor) Poll(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Now()
	m.pollSlot(ctx, now)
	m.evaluateAckAge(ctx, now)
	m.evaluateSinks(ctx, now)
	m.deliver(ctx)
}

func (m *Monitor) pollSlot(ctx context.Context, now time.Time) {
	qctx, cancel := context.WithTimeout(ctx, m.interval)
	st, err := m.slots.Slot(qctx, m.slot)
	cancel()
	if err == nil && !st.Exists {
		err = fmt.Errorf("%w: %s", ErrSlotNotFound, m.slot)
	}
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		err = fmt.Errorf("lag: poll slot %s: %w", m.slot, err)
		m.observer.ObservePollError(err)
		m.logger.LogAttrs(ctx, slog.LevelWarn, "slot lag poll failed",
			slog.String("component", "lag"), slog.String("operation", "poll"), slog.Any("error", err))
		return
	}
	m.observer.ObserveSlot(st.LagBytes, st.WALStatus)
	for _, r := range m.rules[:2] {
		if r.evaluate(float64(st.LagBytes), now) {
			m.stateChanged(ctx, r, slog.Int64("bytes", st.LagBytes))
		}
	}
}

func (m *Monitor) evaluateAckAge(ctx context.Context, now time.Time) {
	var age time.Duration
	if readAt, ok := m.acks.OldestUnacked(); ok {
		age = max(now.Sub(readAt), 0)
	}
	m.observer.ObserveOldestUnackedAge(age)
	if r := m.rules[2]; r.evaluate(age.Seconds(), now) {
		m.stateChanged(ctx, r, slog.Int64("duration_ms", age.Milliseconds()))
	}
}

func (m *Monitor) evaluateSinks(ctx context.Context, now time.Time) {
	var fatal []string
	for _, s := range m.sinks {
		if h := s.Health(); h.IsFatal() {
			fatal = append(fatal, fmt.Sprintf("sink %s: %s: %s", s.Name(), h.Code, h.Reason))
		}
	}
	if r := m.rules[3]; r.evaluate(float64(len(fatal)), now) {
		attrs := []slog.Attr{slog.Int("count", len(fatal))}
		if len(fatal) > 0 {
			attrs = append(attrs, slog.String("error", strings.Join(fatal, "; ")))
		}
		m.stateChanged(ctx, r, attrs...)
	}
}

func (m *Monitor) stateChanged(ctx context.Context, r *rule, attrs ...slog.Attr) {
	m.observer.SetAlertActive(r.name, r.active)
	level := slog.LevelInfo
	if r.active {
		level = slog.LevelWarn
	}
	msg := fmt.Sprintf("alert %s %s (threshold %s)", r.name, r.state(), strconv.FormatFloat(r.threshold, 'f', -1, 64))
	all := append([]slog.Attr{
		slog.String("component", "lag"), slog.String("operation", "alert"), slog.String("status", string(r.state())),
	}, attrs...)
	m.logger.LogAttrs(ctx, level, msg, all...)
}

func (m *Monitor) deliver(ctx context.Context) {
	for _, r := range m.rules {
		nctx, cancel := context.WithTimeout(ctx, m.interval)
		err := r.deliver(nctx, m.notifier)
		cancel()
		if err == nil || ctx.Err() != nil {
			continue
		}
		m.logger.LogAttrs(ctx, slog.LevelWarn,
			fmt.Sprintf("alert %s %s notification failed, retrying on the next poll", r.name, r.pending.State),
			slog.String("component", "lag"), slog.String("operation", "notify"),
			slog.Int("attempt", r.attempts), slog.Any("error", err))
	}
}

type nopNotifier struct{}

func (nopNotifier) Notify(context.Context, pipeline.Alert) error { return nil }
