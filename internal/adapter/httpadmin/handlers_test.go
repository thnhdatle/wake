// SPDX-License-Identifier: Apache-2.0

package httpadmin_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/testutil"
)

// serve starts a server with opts on a loopback port and returns its address.
func serve(t *testing.T, opts ...httpadmin.Option) (string, *httpadmin.Server) {
	t.Helper()
	reg := prometheus.NewRegistry()
	srv, err := httpadmin.New("127.0.0.1:0", testInfo, testutil.DiscardLogger(),
		append([]httpadmin.Option{httpadmin.WithRegistry(reg, reg)}, opts...)...)
	require.NoError(t, err)
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	return ln.Addr().String(), srv
}

type response struct {
	status    int
	body      []byte
	requestID string
}

func get(t *testing.T, addr, path string, header map[string]string) response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+path, http.NoBody)
	require.NoError(t, err)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return response{status: resp.StatusCode, body: body, requestID: resp.Header.Get(httpadmin.RequestIDHeader)}
}

type envelope struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
	Checks []httpadmin.Check `json:"checks"`
}

func TestReadyz_Checks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		checks     []httpadmin.Check
		wantStatus int
		wantInMsg  []string
	}{
		{
			name:       "all ok",
			checks:     []httpadmin.Check{{Name: "config", OK: true}, {Name: "replication", OK: true}},
			wantStatus: http.StatusOK,
		},
		{
			name: "retrying sink keeps ready",
			checks: []httpadmin.Check{
				{Name: "replication", OK: true},
				{Name: "sink", Sink: "k", OK: true, State: "retrying", Reason: "broker down"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "fatal sink and replication down",
			checks: []httpadmin.Check{
				{Name: "replication", OK: false, Reason: "not streaming"},
				{Name: "sink", Sink: "kafka-main", OK: false, State: "fatal", Code: "event_too_large", Reason: "t"},
			},
			wantStatus: http.StatusServiceUnavailable,
			wantInMsg:  []string{"replication: not streaming", "sink kafka-main (event_too_large): t"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr, _ := serve(t, httpadmin.WithReadiness(func(context.Context) []httpadmin.Check { return tc.checks }))

			resp := get(t, addr, "/readyz", nil)

			require.Equal(t, tc.wantStatus, resp.status)
			if tc.wantStatus == http.StatusOK {
				assert.JSONEq(t, mustJSON(t, map[string]any{"status": "ok", "checks": tc.checks}), string(resp.body))
				return
			}
			var env envelope
			require.NoError(t, json.Unmarshal(resp.body, &env))
			assert.Equal(t, httpadmin.CodeNotReady, env.Error.Code)
			assert.Equal(t, resp.requestID, env.Error.RequestID)
			assert.Equal(t, tc.checks, env.Checks)
			for _, s := range tc.wantInMsg {
				assert.Contains(t, env.Error.Message, s)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		fn         httpadmin.StatusFunc
		wantStatus int
		wantBody   string
		wantCode   string
	}{
		{
			name:       "document",
			fn:         func(context.Context) (any, error) { return map[string]any{"source": "app"}, nil },
			wantStatus: http.StatusOK,
			wantBody:   `{"source":"app"}`,
		},
		{
			name:       "unavailable",
			fn:         func(context.Context) (any, error) { return nil, errors.New("starting") },
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   httpadmin.CodeStatusUnavailable,
		},
		{
			name:       "panic is recovered",
			fn:         func(context.Context) (any, error) { panic("boom") },
			wantStatus: http.StatusInternalServerError,
			wantCode:   httpadmin.CodeInternal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr, _ := serve(t, httpadmin.WithStatus(tc.fn))

			resp := get(t, addr, "/v1/status", nil)

			require.Equal(t, tc.wantStatus, resp.status)
			if tc.wantBody != "" {
				assert.JSONEq(t, tc.wantBody, string(resp.body))
				return
			}
			var env envelope
			require.NoError(t, json.Unmarshal(resp.body, &env))
			assert.Equal(t, tc.wantCode, env.Error.Code)
			assert.NotEmpty(t, env.Error.RequestID)
		})
	}
}

func TestStatus_NotRegisteredWithoutProvider(t *testing.T) {
	t.Parallel()
	addr, srv := serve(t)

	assert.Equal(t, http.StatusNotFound, get(t, addr, "/v1/status", nil).status)
	assert.NotContains(t, srv.Routes(), "GET /v1/status")
}

func TestFetchStatus(t *testing.T) {
	t.Parallel()
	addr, _ := serve(t, httpadmin.WithStatus(func(context.Context) (any, error) {
		return map[string]string{"source": "app"}, nil
	}))
	down, _ := serve(t, httpadmin.WithStatus(func(context.Context) (any, error) {
		return nil, errors.New("starting")
	}))

	body, err := httpadmin.FetchStatus(t.Context(), addr)
	require.NoError(t, err)
	assert.JSONEq(t, `{"source":"app"}`, string(body))

	_, err = httpadmin.FetchStatus(t.Context(), down)
	require.ErrorContains(t, err, "returned 503")
	_, err = httpadmin.FetchStatus(t.Context(), "127.0.0.1:1")
	require.ErrorContains(t, err, "fetch status")
}

func TestRequestID(t *testing.T) {
	t.Parallel()
	addr, _ := serve(t)
	tests := []struct {
		name   string
		header string
		echo   bool
	}{
		{"valid id is echoed", "abc-123_X.y", true},
		{"invalid id is replaced", "bad id\twith spaces", false},
		{"missing id is generated", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := map[string]string{}
			if tc.header != "" {
				h[httpadmin.RequestIDHeader] = tc.header
			}

			resp := get(t, addr, "/healthz", h)

			if tc.echo {
				assert.Equal(t, tc.header, resp.requestID)
				return
			}
			assert.Len(t, resp.requestID, 26, "a ULID")
		})
	}
}
