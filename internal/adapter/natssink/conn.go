// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/thnhdatle/wake/internal/config"
)

type conn interface {
	publish(msg *nats.Msg, id string) (ackFuture, error)
	epoch() uint64
	maxPayload() int64
	state() (connected bool, lastErr error)
	ensureStream(ctx context.Context, spec streamSpec) error
	streamExists(ctx context.Context, name string) error
	close()
}

type ackFuture interface {
	Ok() <-chan *jetstream.PubAck
	Err() <-chan error
}

type streamSpec struct {
	name    string
	subject string
	window  time.Duration
}

type dialer func(s *Sink) (conn, error)

type natsConn struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream string
}

func dialNATS(s *Sink) (conn, error) {
	opts := []nats.Option{
		nats.Name("wake-" + s.name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(reconnectWait),
		nats.ReconnectJitter(reconnectWait/2, reconnectWait/2),
		nats.ReconnectBufSize(-1),
		nats.RetryOnFailedConnect(true),
		nats.IgnoreAuthErrorAbort(),
		nats.Timeout(s.requestTimeout),
		nats.PingInterval(pingInterval),
		nats.MaxPingsOutstanding(maxPingsOutstanding),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { s.noteConnErr(err) }),
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) { s.noteConnErr(err) }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { s.noteConnErr(err) }),
	}
	if s.credsFile != "" {
		opts = append(opts, nats.UserCredentials(s.credsFile))
	}
	if s.tls != nil {
		opts = append(opts, nats.Secure(s.tls.Clone()))
	}
	nc, err := nats.Connect(s.url, opts...)
	if err != nil {
		return nil, fmt.Errorf("natssink: connect %s: %w", s.url, err)
	}
	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(s.maxRecords+1),
		jetstream.WithPublishAsyncTimeout(s.ackTimeout),
		jetstream.WithDefaultTimeout(s.requestTimeout),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natssink: jetstream: %w", err)
	}
	return &natsConn{nc: nc, js: js, stream: s.stream}, nil
}

func (c *natsConn) publish(msg *nats.Msg, id string) (ackFuture, error) {
	f, err := c.js.PublishMsgAsync(msg,
		jetstream.WithMsgID(id), jetstream.WithExpectStream(c.stream), jetstream.WithRetryAttempts(0))
	if err != nil {
		return nil, fmt.Errorf("natssink: publish to %s: %w", msg.Subject, err)
	}
	return f, nil
}

func (c *natsConn) epoch() uint64 { return c.nc.Stats().Reconnects }

func (c *natsConn) maxPayload() int64 { return c.nc.MaxPayload() }

func (c *natsConn) state() (bool, error) {
	connected := c.nc.Status() == nats.CONNECTED
	if err := c.nc.LastError(); err != nil {
		return connected, fmt.Errorf("natssink: connection: %w", err)
	}
	return connected, nil
}

func (c *natsConn) ensureStream(ctx context.Context, spec streamSpec) error {
	st, err := c.js.Stream(ctx, spec.name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		_, err = c.js.CreateStream(ctx, jetstream.StreamConfig{
			Name:       spec.name,
			Subjects:   []string{spec.subject},
			Duplicates: spec.window,
			Storage:    jetstream.FileStorage,
		})
		if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("natssink: create stream %s: %w", spec.name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("natssink: stream %s: %w", spec.name, err)
	}
	cfg := st.CachedInfo().Config
	if slices.Contains(cfg.Subjects, spec.subject) && cfg.Duplicates == spec.window {
		return nil
	}
	if !slices.Contains(cfg.Subjects, spec.subject) {
		cfg.Subjects = append(cfg.Subjects, spec.subject)
	}
	cfg.Duplicates = spec.window
	if _, err := c.js.UpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("natssink: update stream %s: %w", spec.name, err)
	}
	return nil
}

func (c *natsConn) streamExists(ctx context.Context, name string) error {
	_, err := c.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("%w: stream %s does not exist and nats.create_stream is false", ErrUnknownStream, name)
	}
	if err != nil {
		return fmt.Errorf("natssink: stream %s: %w", name, err)
	}
	return nil
}

func (c *natsConn) close() { c.nc.Close() }

func loadTLS(c config.TLS) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		data, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%w: nats.tls.ca_file: %w", ErrInvalidConfig, err)
		}
		roots := x509.NewCertPool()
		if ok := roots.AppendCertsFromPEM(data); !ok {
			return nil, fmt.Errorf("%w: nats.tls.ca_file %s has no PEM certificate", ErrInvalidConfig, c.CAFile)
		}
		cfg.RootCAs = roots
	}
	if c.CertFile == "" && c.KeyFile == "" {
		return cfg, nil
	}
	pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("%w: nats.tls.cert_file and key_file: %w", ErrInvalidConfig, err)
	}
	cfg.Certificates = append(cfg.Certificates, pair)
	return cfg, nil
}
