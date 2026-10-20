// SPDX-License-Identifier: Apache-2.0

package alerthook

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Timeouts of one alert POST.
const (
	DefaultTimeout = 5 * time.Second
	// MaxTimeout caps WithTimeout so a slow receiver cannot stall the lag
	// monitor for a large part of lag.interval.
	MaxTimeout = 5 * time.Second
)

const (
	userAgent = "wake-alert/1"
	maxDrain  = 64 << 10
)

// Notifier posts alert state changes. It is safe for concurrent use.
type Notifier struct {
	url    string
	client *http.Client
}

var _ pipeline.AlertNotifier = (*Notifier)(nil)

// Option configures a Notifier.
type Option func(*options)

type options struct {
	timeout   time.Duration
	transport http.RoundTripper
}

// WithTimeout bounds one POST (default DefaultTimeout); it must lie in
// (0, MaxTimeout].
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithTransport replaces the HTTP transport (default: a dedicated
// http.Transport with TLS 1.2 or later and system roots).
func WithTransport(rt http.RoundTripper) Option { return func(o *options) { o.transport = rt } }

// New validates the http or https URL of lag.alert_webhook. It starts
// nothing.
func New(rawURL string, opts ...Option) (*Notifier, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("%w: lag.alert_webhook must be an http or https URL", ErrInvalidConfig)
	}
	o := options{timeout: DefaultTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	if o.timeout <= 0 || o.timeout > MaxTimeout {
		return nil, fmt.Errorf("%w: timeout %s must lie in (0, %s]", ErrInvalidConfig, o.timeout, MaxTimeout)
	}
	if o.transport == nil {
		o.transport = &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        1,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: o.timeout,
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
		}
	}
	return &Notifier{
		url: rawURL,
		client: &http.Client{
			Transport: o.transport,
			Timeout:   o.timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// payload is the alert body.
type payload struct {
	Rule      string  `json:"rule"`
	State     string  `json:"state"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Ts        string  `json:"ts"`
}

// Notify implements pipeline.AlertNotifier: one POST of
// {"rule","state","value","threshold","ts"} with ts in RFC 3339 (UTC). It
// returns a *StatusError for a non-2xx response.
func (n *Notifier) Notify(ctx context.Context, a pipeline.Alert) error {
	body, err := json.Marshal(payload{
		Rule: a.Rule, State: string(a.State), Value: a.Value, Threshold: a.Threshold,
		Ts: a.Ts.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("%w: rule %s: %w", ErrEncode, a.Rule, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("alerthook: request: %w", stripURL(err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("alerthook: post rule %s: %w", a.Rule, stripURL(err))
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &StatusError{StatusCode: resp.StatusCode}
	}
	return nil
}

// CloseIdleConnections closes the transport's idle connections, at
// shutdown.
func (n *Notifier) CloseIdleConnections() { n.client.CloseIdleConnections() }

// stripURL drops the *url.Error wrapper, whose message repeats the URL and
// so possibly a token in its query.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}
