// SPDX-License-Identifier: Apache-2.0

package httpadmin

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const HealthcheckTimeout = 3 * time.Second

func ProbeAddr(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// Healthcheck probes the readiness endpoint at addr and returns an error if it fails.
func Healthcheck(ctx context.Context, addr string) error {
	ctx, cancel := context.WithTimeout(ctx, HealthcheckTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ProbeAddr(addr)+"/readyz", http.NoBody)
	if err != nil {
		return fmt.Errorf("build healthcheck request: %w", err)
	}
	client := &http.Client{Timeout: HealthcheckTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: /readyz returned %d", resp.StatusCode)
	}
	return nil
}

const MaxStatusBytes = 8 << 20

// FetchStatus returns the status document served at addr.
func FetchStatus(ctx context.Context, addr string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, HealthcheckTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ProbeAddr(addr)+"/v1/status", http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("build status request: %w", err)
	}
	client := &http.Client{Timeout: HealthcheckTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch status: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxStatusBytes))
	if err != nil {
		return nil, fmt.Errorf("read status: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch status: /v1/status returned %d: %s", resp.StatusCode, body)
	}
	return body, nil
}
