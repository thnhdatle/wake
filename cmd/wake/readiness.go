// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// checkReplication names the replication readiness check.
const checkReplication = "replication"

// errStarting reports a status request before the pipeline is wired.
var errStarting = errors.New("wake is starting: pre-flight checks, publication and slot setup")

// activityWindowFactor × stream.status_interval is how long the replication
// connection may go without activity before /readyz reports it.
const activityWindowFactor = 3

// replActivity wraps the replication connection and records when it last
// showed activity: START_REPLICATION, a received message (XLogData or
// keepalive) or a standby status update the server accepted. Status
// updates count because an idle server sends no keepalives while Wake
// replies every status_interval, and because reading pauses under
// backpressure while updates continue; a broken connection fails
// both.
type replActivity struct {
	src     pipeline.ReplicationSource
	clk     clock.Clock
	started atomic.Bool
	// last is the Unix nanosecond time of the last activity.
	last atomic.Int64
}

func newReplActivity(src pipeline.ReplicationSource, clk clock.Clock) *replActivity {
	return &replActivity{src: src, clk: clk}
}

func (a *replActivity) touch() { a.last.Store(a.clk.Now().UnixNano()) }

// StartReplication implements pipeline.ReplicationSource.
func (a *replActivity) StartReplication(ctx context.Context, opts pipeline.StartOptions) error {
	if err := a.src.StartReplication(ctx, opts); err != nil {
		return err
	}
	a.started.Store(true)
	a.touch()
	return nil
}

// Receive implements pipeline.ReplicationSource.
func (a *replActivity) Receive(ctx context.Context) (pipeline.ReplicationMessage, error) {
	msg, err := a.src.Receive(ctx)
	if err == nil {
		a.touch()
	}
	return msg, err
}

// SendStandbyStatus implements pipeline.StandbyStatusSender.
func (a *replActivity) SendStandbyStatus(ctx context.Context, st pipeline.StandbyStatus) error {
	err := a.src.SendStandbyStatus(ctx, st)
	if err == nil {
		a.touch()
	}
	return err
}

// lastActivity returns the time of the last activity, zero before any.
func (a *replActivity) lastActivity() time.Time {
	ns := a.last.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// runtimeView serves /readyz and /v1/status. The pipeline is published to it
// once wired; before that the service is starting.
type runtimeView struct {
	cfg config.Config
	clk clock.Clock
	svc atomic.Pointer[service]
}

func newRuntimeView(cfg config.Config, clk clock.Clock) *runtimeView {
	return &runtimeView{cfg: cfg, clk: clk}
}

func (v *runtimeView) set(s *service) { v.svc.Store(s) }

// readiness implements httpadmin.ReadinessFunc: configuration loaded,
// replication streaming, and no sink blocked by a non-retriable error. A
// sink retrying after retriable errors stays ready: backpressure is the
// designed behavior and lag.max_ack_age alerts on it.
func (v *runtimeView) readiness(context.Context) []httpadmin.Check {
	checks := []httpadmin.Check{{Name: "config", OK: true}}
	s := v.svc.Load()
	if s == nil {
		return append(checks, httpadmin.Check{
			Name: checkReplication, Code: codeReplicationNotStarted,
			Reason: "starting: pre-flight checks, publication and slot setup",
		})
	}
	checks = append(checks, s.replicationCheck(v.clk.Now()))
	for _, sk := range s.sinks {
		h := sk.Health()
		checks = append(checks, httpadmin.Check{
			Name: "sink", Sink: sk.Name(), OK: !h.IsFatal(), State: h.State.String(), Code: h.Code, Reason: h.Reason,
		})
	}
	return checks
}

// replicationCheck reports whether replication is streaming at now.
func (s *service) replicationCheck(now time.Time) httpadmin.Check {
	c := httpadmin.Check{Name: checkReplication, Code: codeReplicationNotStarted}
	window := activityWindowFactor * s.cfg.Stream.StatusInterval.Std()
	switch last := s.repl.lastActivity(); {
	case !s.repl.started.Load():
		c.Reason = "START_REPLICATION not issued yet"
	case !s.readerRunning.Load():
		c.Reason = "the stream reader stopped"
	case now.Sub(last) > window:
		c.Reason = fmt.Sprintf("no replication activity for %s (limit %s)", now.Sub(last).Round(time.Second), window)
	default:
		return httpadmin.Check{Name: checkReplication, OK: true}
	}
	return c
}

// status implements httpadmin.StatusFunc (GET /v1/status, `wake status`).
func (v *runtimeView) status(ctx context.Context) (any, error) {
	s := v.svc.Load()
	if s == nil {
		return nil, errStarting
	}
	slot, slotErr := s.src.catalog.Slot(ctx, slotName(s.cfg))
	doc := newStatusDoc(s.cfg, slot, slotErr, s.keeper.State())
	doc.ConfirmedLSN = s.tracker.Confirmed().String()
	doc.StatusSentLSN = s.status.LastSent().String()
	doc.Replication = &replicationDoc{Streaming: s.replicationCheck(v.clk.Now()).OK}
	if last := s.repl.lastActivity(); !last.IsZero() {
		doc.Replication.LastActivity = &last
	}
	doc.Sinks = make([]sinkDoc, 0, len(s.sinks))
	for _, sk := range s.sinks {
		h := sk.Health()
		d := sinkDoc{Name: sk.Name(), Type: string(sk.typ), State: h.State.String(), Code: h.Code, Reason: h.Reason}
		if !h.Since.IsZero() {
			d.Since = &h.Since
		}
		doc.Sinks = append(doc.Sinks, d)
	}
	return doc, nil
}
