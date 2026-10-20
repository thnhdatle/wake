// SPDX-License-Identifier: Apache-2.0

// Package httpadmin implements the admin HTTP server and the clients used by
// "wake healthcheck" and "wake status".
//
// Routes (api/openapi.yaml is the source of truth; a test keeps them equal):
// GET /healthz (always 200 while serving), GET /readyz (200 when every check
// of the ReadinessFunc passes, else 503 with the error envelope
// code not_ready plus the checks), GET /metrics (Prometheus exposition of the
// configured gatherer), GET /v1/version and, when WithStatus is given,
// GET /v1/status. Every response carries X-Request-Id (accepted when valid,
// else a new ULID); a handler panic is logged and answered with 500 internal.
//
//	srv, err := httpadmin.New(cfg.Admin.Listen, buildinfo.Get(), logger,
//		httpadmin.WithRegistry(reg, reg), httpadmin.WithReadiness(ready), httpadmin.WithStatus(status))
//	g.Go(func() error { return srv.Run(ctx) })
package httpadmin
