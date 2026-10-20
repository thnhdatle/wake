// SPDX-License-Identifier: Apache-2.0

package alerthook_test

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/adapter/alerthook"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type request struct {
	method, contentType string
	body                []byte
}

func server(t *testing.T, status int) (*httptest.Server, <-chan request) {
	t.Helper()
	reqs := make(chan request, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		reqs <- request{method: r.Method, contentType: r.Header.Get("Content-Type"), body: body}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, reqs
}

var alert = pipeline.Alert{
	Rule: "slot_lag_warn", State: pipeline.AlertStateFiring, Value: 9437184, Threshold: 8388608,
	Ts: time.Date(2026, 10, 18, 14, 0, 0, 0, time.FixedZone("CEST", 2*3600)),
}

func TestNotifier_Notify_PostsAlert(t *testing.T) {
	t.Parallel()
	srv, reqs := server(t, http.StatusNoContent)
	n, err := alerthook.New(srv.URL + "/alerts")
	require.NoError(t, err)
	t.Cleanup(n.CloseIdleConnections)
	require.NoError(t, n.Notify(context.Background(), alert))
	r := <-reqs
	assert.Equal(t, http.MethodPost, r.method)
	assert.Equal(t, "application/json", r.contentType)
	assert.JSONEq(t, `{"rule":"slot_lag_warn","state":"firing","value":9437184,"threshold":8388608,`+
		`"ts":"2026-10-18T12:00:00Z"}`, string(r.body))
}

func TestNotifier_Notify_Errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		status int
		code   int
	}{
		{name: "server error", status: http.StatusInternalServerError, code: 500},
		{name: "client error", status: http.StatusUnauthorized, code: 401},
		{name: "redirect not followed", status: http.StatusFound, code: 302},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := server(t, tc.status)
			n, err := alerthook.New(srv.URL)
			require.NoError(t, err)
			t.Cleanup(n.CloseIdleConnections)
			err = n.Notify(context.Background(), alert)
			var se *alerthook.StatusError
			require.ErrorAs(t, err, &se)
			assert.Equal(t, tc.code, se.StatusCode)
			assert.Contains(t, se.Error(), "HTTP")
		})
	}
}

func TestNotifier_Notify_Timeout(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	n, err := alerthook.New(srv.URL+"/?token=sekrit", alerthook.WithTimeout(50*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(n.CloseIdleConnections)
	err = n.Notify(context.Background(), alert)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "sekrit", "the URL is not repeated in errors")
}

func TestNotifier_Notify_CanceledAndUnreachable(t *testing.T) {
	t.Parallel()
	srv, _ := server(t, http.StatusOK)
	n, err := alerthook.New(srv.URL)
	require.NoError(t, err)
	t.Cleanup(n.CloseIdleConnections)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, n.Notify(ctx, alert), context.Canceled)

	bad := alert
	bad.Value = math.NaN()
	require.ErrorIs(t, n.Notify(context.Background(), bad), alerthook.ErrEncode)
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		url  string
		opts []alerthook.Option
	}{
		{name: "empty", url: ""},
		{name: "scheme", url: "ftp://alerts.example/hook"},
		{name: "no host", url: "https://"},
		{name: "unparsable", url: "http://[::1"},
		{name: "zero timeout", url: "http://x", opts: []alerthook.Option{alerthook.WithTimeout(0)}},
		{name: "timeout above max", url: "http://x", opts: []alerthook.Option{alerthook.WithTimeout(time.Minute)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := alerthook.New(tc.url, tc.opts...)
			require.ErrorIs(t, err, alerthook.ErrInvalidConfig)
		})
	}
	n, err := alerthook.New("http://x", alerthook.WithTransport(http.DefaultTransport))
	require.NoError(t, err)
	assert.NotNil(t, n)
}
