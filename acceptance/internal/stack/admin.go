// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Admin is a client of the Wake admin HTTP endpoint.
type Admin struct {
	base   string
	client *http.Client
}

// NewAdmin returns a client for addr (host:port).
func NewAdmin(addr string) *Admin {
	return &Admin{base: "http://" + addr, client: &http.Client{Timeout: 5 * time.Second}}
}

// ReadyState is the result of GET /readyz.
type ReadyState struct {
	OK     bool
	Status int
	Body   string
}

func (a *Admin) get(ctx context.Context, path string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+path, http.NoBody)
	if err != nil {
		return 0, nil, fmt.Errorf("stack: admin request: %w", err)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("stack: admin GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("stack: admin GET %s body: %w", path, err)
	}
	return resp.StatusCode, body, nil
}

// Ready issues GET /readyz.
func (a *Admin) Ready(ctx context.Context) (ReadyState, error) {
	code, body, err := a.get(ctx, "/readyz")
	if err != nil {
		return ReadyState{Body: err.Error()}, err
	}
	return ReadyState{OK: code == http.StatusOK, Status: code, Body: string(body)}, nil
}

// Status is the subset of GET /v1/status the tests read.
type Status struct {
	Source string `json:"source"`
	Slot   struct {
		Name              string `json:"name"`
		Exists            bool   `json:"exists"`
		ConfirmedFlushLSN string `json:"confirmed_flush_lsn"`
		LagBytes          int64  `json:"lag_bytes"`
	} `json:"slot"`
	ConfirmedLSN string `json:"confirmed_lsn"`
	Snapshot     struct {
		// ConsistentPoint is the initial snapshot's consistent point, set
		// once the slot was created with an exported snapshot.
		ConsistentPoint string `json:"consistent_point"`
		Runs            []struct {
			ID     uint64 `json:"id"`
			Kind   string `json:"kind"`
			Tables []struct {
				Table string `json:"table"`
				Rows  uint64 `json:"rows"`
				Done  bool   `json:"done"`
			} `json:"tables"`
		} `json:"runs"`
		Queue []string `json:"queue"`
	} `json:"snapshot"`
	Sinks []struct {
		Name   string `json:"name"`
		State  string `json:"state"`
		Code   string `json:"code"`
		Reason string `json:"reason"`
	} `json:"sinks"`
	Schemas map[string]uint64 `json:"schemas"`
}

// Status issues GET /v1/status.
func (a *Admin) Status(ctx context.Context) (Status, error) {
	var st Status
	code, body, err := a.get(ctx, "/v1/status")
	if err != nil {
		return st, err
	}
	if code != http.StatusOK {
		return st, fmt.Errorf("stack: /v1/status returned %d: %s", code, body)
	}
	if err := json.Unmarshal(body, &st); err != nil {
		return st, fmt.Errorf("stack: decode /v1/status: %w", err)
	}
	return st, nil
}

// Metrics returns the Prometheus text exposition of GET /metrics.
func (a *Admin) Metrics(ctx context.Context) (string, error) {
	code, body, err := a.get(ctx, "/metrics")
	if err != nil {
		return "", err
	}
	if code != http.StatusOK {
		return "", fmt.Errorf("stack: /metrics returned %d", code)
	}
	return string(body), nil
}

// ReadyMonitor polls /readyz and the container state in the background and
// counts stops: transitions from ready to not ready (or unreachable), and
// container restarts or exits.
type ReadyMonitor struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	polls     int
	notReady  int
	stops     int
	wasReady  bool
	events    []string
	startedAt string
	restarts  int
	exited    bool
}

// Monitor starts a ReadyMonitor polling every interval until Stop.
func (w *Wake) Monitor(interval time.Duration) *ReadyMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	m := &ReadyMonitor{cancel: cancel, done: make(chan struct{}), wasReady: true}
	if st, err := w.State(ctx); err == nil {
		m.startedAt, m.restarts = st.StartedAt, st.RestartCount
	}
	go m.loop(ctx, w, interval)
	return m
}

func (m *ReadyMonitor) loop(ctx context.Context, w *Wake, interval time.Duration) {
	defer close(m.done)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		st, err := w.admin.Ready(ctx)
		if ctx.Err() != nil {
			return
		}
		m.observeReady(st, err)
		if n%8 == 0 {
			if cs, cerr := w.State(ctx); cerr == nil {
				m.observeContainer(cs)
			}
		}
	}
}

func (m *ReadyMonitor) observeReady(st ReadyState, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.polls++
	ready := err == nil && st.OK
	if !ready {
		m.notReady++
	}
	if m.wasReady && !ready {
		m.stops++
		m.events = append(m.events, fmt.Sprintf("%s not ready: %d %s", time.Now().UTC().Format(time.RFC3339Nano),
			st.Status, truncate(st.Body, 300)))
	}
	m.wasReady = ready
}

func (m *ReadyMonitor) observeContainer(cs ContainerState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cs.StartedAt != m.startedAt || cs.RestartCount != m.restarts {
		m.stops++
		m.events = append(m.events, fmt.Sprintf("container restarted (started %s, restarts %d)", cs.StartedAt,
			cs.RestartCount))
		m.startedAt, m.restarts = cs.StartedAt, cs.RestartCount
	}
	if !cs.Running && !m.exited {
		m.exited = true
		m.stops++
		m.events = append(m.events, fmt.Sprintf("container exited with code %d", cs.ExitCode))
	}
}

// Stop ends polling and returns the result.
func (m *ReadyMonitor) Stop() MonitorResult {
	m.cancel()
	<-m.done
	m.mu.Lock()
	defer m.mu.Unlock()
	return MonitorResult{Polls: m.polls, NotReadyPolls: m.notReady, Stops: m.stops, Events: append([]string(nil),
		m.events...)}
}

// MonitorResult summarizes a ReadyMonitor.
type MonitorResult struct {
	Polls         int
	NotReadyPolls int
	Stops         int
	Events        []string
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
