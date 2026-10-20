// SPDX-License-Identifier: Apache-2.0

package httpadmin_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/platform/buildinfo"
	"github.com/thnhdatle/wake/internal/platform/metrics"
	"github.com/thnhdatle/wake/internal/testutil"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var testInfo = buildinfo.Info{Version: "1.2.3", Commit: "abc", BuildDate: "d", GoVersion: "go9"}

func startServer(t *testing.T) string {
	t.Helper()
	reg := prometheus.NewRegistry()
	srv, err := httpadmin.New("127.0.0.1:0", testInfo, testutil.DiscardLogger(), httpadmin.WithRegistry(reg, reg))
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
	return ln.Addr().String()
}

func TestEndpoints(t *testing.T) {
	t.Parallel()
	addr := startServer(t)

	tests := []struct {
		name, path, contains string
	}{
		{"healthz", "/healthz", "ok"},
		{"readyz", "/readyz", "ok"},
		{"version", "/v1/version", `"version":"1.2.3"`},
		{"metrics", "/metrics", `wake_build_info{commit="abc",go_version="go9",version="1.2.3"} 1`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+tc.path, http.NoBody)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Contains(t, string(body), tc.contains)
		})
	}
}

func TestHealthcheck(t *testing.T) {
	t.Parallel()
	addr := startServer(t)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(bad.Close)

	tests := []struct {
		name    string
		addr    string
		wantErr string
	}{
		{"ready", addr, ""},
		{"not ready", bad.Listener.Addr().String(), "returned 503"},
		{"unreachable", "127.0.0.1:1", "healthcheck:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := httpadmin.Healthcheck(t.Context(), tc.addr)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestProbeAddr(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want string }{
		{"0.0.0.0:9093", "127.0.0.1:9093"},
		{":9093", "127.0.0.1:9093"},
		{"[::]:9093", "127.0.0.1:9093"},
		{"127.0.0.1:9093", "127.0.0.1:9093"},
		{"example.org:80", "example.org:80"},
		{"garbage", "garbage"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, httpadmin.ProbeAddr(tc.in))
		})
	}
}

func TestNewErrors(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	opt := httpadmin.WithRegistry(reg, reg)

	_, err := httpadmin.New("", testInfo, testutil.DiscardLogger(), opt)
	require.Error(t, err)
	_, err = httpadmin.New("127.0.0.1:0", testInfo, testutil.DiscardLogger(), opt)
	require.NoError(t, err)
	_, err = httpadmin.New("127.0.0.1:0", testInfo, testutil.DiscardLogger(), opt)
	require.NoError(t, err, "an identical wake_build_info already registered is kept")
}

func TestNew_SharesBuildInfoWithMetricsRegistry(t *testing.T) {
	t.Parallel()
	reg, err := metrics.NewRegistry(testInfo)
	require.NoError(t, err)

	_, err = httpadmin.New("127.0.0.1:0", testInfo, testutil.DiscardLogger(), httpadmin.WithRegistry(reg, reg))
	require.NoError(t, err)

	fams, err := reg.Gather()
	require.NoError(t, err)
	n := 0
	for _, f := range fams {
		if f.GetName() == "wake_build_info" {
			n += len(f.GetMetric())
		}
	}
	assert.Equal(t, 1, n)
}

func TestRunShutsDownOnCancel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	srv, err := httpadmin.New("127.0.0.1:0", testInfo, testutil.DiscardLogger(),
		httpadmin.WithRegistry(reg, reg), httpadmin.WithShutdownTimeout(time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	cancel()
	require.NoError(t, <-done)
}
