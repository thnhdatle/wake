// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/platform/buildinfo"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/platform/metrics"
)

// runOptions are the flags of `wake run`.
type runOptions struct {
	cfgPath string
	reinit  bool
}

// runService is `wake run`: it loads the configuration, starts the admin
// server, wires the pipeline (wirePipeline) and runs it until ctx ends or a
// component fails.
//
// Failure policy (replication connection loss and any other component
// error): the pipeline stops reading, drains the sinks within a bound,
// sends a final standby status update and the process exits with code 1.
// Restarting is left to the supervisor (Kubernetes StatefulSet, compose
// `restart: unless-stopped`, systemd), which already bounds and backs off
// restarts and makes every restart visible. An in-process reconnect loop
// would duplicate that, hide flapping and complicate the single-consumer
// slot rule (the old walsender may still hold the slot). Restarting is safe
// because delivery is at-least-once: streaming resumes from the slot's
// confirmed_flush_lsn, which never passes an unacknowledged transaction,
// and an interrupted snapshot resumes from its persisted progress.
func runService(ctx context.Context, opts runOptions) error {
	log := newLogger()
	cfg, err := loadConfig(opts.cfgPath, false)
	if err != nil {
		return err
	}
	dsn, err := sourceDSN(&cfg)
	if err != nil {
		return err
	}
	// SIGHUP re-evaluates the table globs; it must be caught before
	// anything else so that it never terminates the process.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	reg, err := metrics.NewRegistry(buildinfo.Get())
	if err != nil {
		return err
	}
	view := newRuntimeView(cfg, clock.System{})
	admin, err := httpadmin.New(cfg.Admin.Listen, buildinfo.Get(), log,
		httpadmin.WithRegistry(reg, reg), httpadmin.WithReadiness(view.readiness), httpadmin.WithStatus(view.status))
	if err != nil {
		return err
	}

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The admin server outlives the pipeline so that /healthz answers during
	// startup and shutdown; it stops once the pipeline returned.
	adminCtx, stopAdmin := context.WithCancel(context.WithoutCancel(ctx))
	defer stopAdmin()
	var g errgroup.Group
	g.Go(func() error {
		err := admin.Run(adminCtx)
		if err != nil {
			cancel()
		}
		return err
	})
	g.Go(func() error {
		defer stopAdmin()
		w := &wiring{
			cfg: cfg, dsn: dsn, cfgPath: opts.cfgPath, reinit: opts.reinit, log: log, clk: clock.System{}, reg: reg, hup: hup,
		}
		svc, err := wirePipeline(pctx, w)
		if err != nil {
			return err
		}
		defer svc.close(pctx)
		view.set(svc)
		log.Info("wake started", "component", "main", "lsn", svc.plan.StartLSN.String())
		err = svc.run(pctx)
		log.Info("wake stopped", "component", "main")
		return err
	})
	return g.Wait()
}

func newLogger() *slog.Logger {
	if os.Getenv("WAKE_LOG_FORMAT") == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, nil))
}

// newCLILogger is the logger of the short-lived commands: warnings and
// errors only, as text on stderr.
func newCLILogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}
