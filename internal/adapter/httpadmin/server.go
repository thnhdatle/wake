// SPDX-License-Identifier: Apache-2.0

package httpadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/thnhdatle/wake/internal/platform/buildinfo"
)

const (
	readHeaderTimeout      = 5 * time.Second
	readTimeout            = 10 * time.Second
	writeTimeout           = 15 * time.Second
	idleTimeout            = 60 * time.Second
	maxHeaderBytes         = 16 << 10
	DefaultShutdownTimeout = 30 * time.Second
	DefaultCheckTimeout    = 5 * time.Second
)

// Server is the admin HTTP server.
type Server struct {
	addr            string
	info            buildinfo.Info
	log             *slog.Logger
	reg             prometheus.Registerer
	gather          prometheus.Gatherer
	shutdownTimeout time.Duration
	readiness       ReadinessFunc
	status          StatusFunc
	routes          []string
	srv             *http.Server
}

type Option func(*Server)

func WithRegistry(r prometheus.Registerer, g prometheus.Gatherer) Option {
	return func(s *Server) { s.reg, s.gather = r, g }
}

func WithShutdownTimeout(d time.Duration) Option {
	return func(s *Server) { s.shutdownTimeout = d }
}

func WithReadiness(f ReadinessFunc) Option {
	return func(s *Server) { s.readiness = f }
}

func WithStatus(f StatusFunc) Option {
	return func(s *Server) { s.status = f }
}

// New returns an admin Server listening on addr.
func New(addr string, info buildinfo.Info, log *slog.Logger, opts ...Option) (*Server, error) {
	if addr == "" {
		return nil, errors.New("httpadmin: empty listen address")
	}
	s := &Server{
		addr:            addr,
		info:            info,
		log:             log.With("component", "httpadmin"),
		reg:             prometheus.DefaultRegisterer,
		gather:          prometheus.DefaultGatherer,
		shutdownTimeout: DefaultShutdownTimeout,
	}
	for _, o := range opts {
		o(s)
	}
	if err := registerBuildInfo(s.reg, info); err != nil {
		return nil, err
	}
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	return s, nil
}

func registerBuildInfo(reg prometheus.Registerer, info buildinfo.Info) error {
	buildGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wake_build_info",
		Help: "Build information; the value is always 1.",
		ConstLabels: prometheus.Labels{
			"version":    info.Version,
			"commit":     info.Commit,
			"go_version": info.GoVersion,
		},
	})
	buildGauge.Set(1)
	err := reg.Register(buildGauge)
	if are := (prometheus.AlreadyRegisteredError{}); errors.As(err, &are) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("register wake_build_info: %w", err)
	}
	return nil
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern string, h http.Handler) {
		mux.Handle(pattern, h)
		s.routes = append(s.routes, pattern)
	}
	handle("GET /healthz", http.HandlerFunc(s.healthz))
	handle("GET /readyz", http.HandlerFunc(s.readyz))
	handle("GET /metrics", promhttp.HandlerFor(s.gather, promhttp.HandlerOpts{}))
	handle("GET /v1/version", http.HandlerFunc(s.version))
	if s.status != nil {
		handle("GET /v1/status", http.HandlerFunc(s.statusDoc))
	}
	return s.recoverer(requestID(mux))
}

func (s *Server) Routes() []string { return slices.Clone(s.routes) }

// Run listens on the configured address and serves until ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("admin listen %s: %w", s.addr, err)
	}
	return s.Serve(ctx, ln)
}

// Serve serves on ln until ctx is canceled.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(ln) }()
	s.log.Info("admin server listening", "addr", ln.Addr().String())
	select {
	case err := <-errCh:
		return fmt.Errorf("admin serve: %w", err)
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.shutdownTimeout)
	defer cancel()
	if err := s.srv.Shutdown(shutCtx); err != nil {
		return fmt.Errorf("admin shutdown: %w", err)
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("admin serve: %w", err)
	}
	return nil
}
