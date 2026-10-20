// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// defaultDockerSocket is the Engine API socket when DOCKER_HOST is not a
// unix:// URL.
const defaultDockerSocket = "/var/run/docker.sock"

// dockerSocket returns the Engine API unix socket path.
func dockerSocket() string {
	if h, ok := strings.CutPrefix(os.Getenv("DOCKER_HOST"), "unix://"); ok && h != "" {
		return h
	}
	return defaultDockerSocket
}

// Killer sends SIGKILL to one container through the Docker Engine API. It
// takes milliseconds where `docker kill` takes a CLI start and a compose
// lookup, so a test can kill at a point it just observed.
type Killer struct {
	id string
	cl *http.Client
}

// NewKiller returns a Killer for the Wake container as it is now (resolve
// it again after the container is recreated; a restart keeps the id).
func (w *Wake) NewKiller(ctx context.Context) (*Killer, error) {
	id, err := w.ContainerID(ctx)
	if err != nil {
		return nil, err
	}
	sock := dockerSocket()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}
	return &Killer{id: id, cl: &http.Client{Transport: tr, Timeout: 10 * time.Second}}, nil
}

// Kill sends SIGKILL (kill -9) and returns once the Engine accepted it.
func (k *Killer) Kill(ctx context.Context) error {
	u := "http://docker/containers/" + url.PathEscape(k.id) + "/kill?signal=KILL"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return fmt.Errorf("stack: kill request: %w", err)
	}
	resp, err := k.cl.Do(req)
	if err != nil {
		return fmt.Errorf("stack: kill %s: %w", k.id, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("stack: kill %s: %s: %s", k.id, resp.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

// Close releases idle connections.
func (k *Killer) Close() { k.cl.CloseIdleConnections() }
