// SPDX-License-Identifier: Apache-2.0

package httpadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// Error codes of the admin endpoints (docs/guides/errors.md).
const (
	// CodeNotReady: /readyz found a failing readiness check.
	CodeNotReady = "not_ready"
	// CodeStatusUnavailable: the status document cannot be built yet or now.
	CodeStatusUnavailable = "status_unavailable"
	// CodeInternal: a handler panicked.
	CodeInternal = "internal"
)

// Check is one readiness condition of /readyz.
type Check struct {
	// Name identifies the condition, e.g. "replication" or "sink".
	Name string `json:"name"`
	// Sink names the sink of a sink check.
	Sink string `json:"sink,omitempty"`
	OK   bool   `json:"ok"`
	// State is the sink's delivery state of a sink check.
	State string `json:"state,omitempty"`
	// Code is the error code of a failing check (docs/guides/errors.md).
	Code string `json:"code,omitempty"`
	// Reason explains a failing or degraded check; never a secret or row value.
	Reason string `json:"reason,omitempty"`
}

// ReadinessFunc evaluates the readiness checks; the server is ready when
// every check is OK.
type ReadinessFunc func(ctx context.Context) []Check

// StatusFunc returns the GET /v1/status document, encoded as JSON. An error
// is answered with 503 status_unavailable.
type StatusFunc func(ctx context.Context) (any, error)

// errorBody is the error envelope of docs/guides/errors.md; /readyz adds its checks.
type errorBody struct {
	Error  errorDetail `json:"error"`
	Checks []Check     `json:"checks,omitempty"`
}

type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// readyBody is the 200 answer of /readyz.
type readyBody struct {
	Status string  `json:"status"`
	Checks []Check `json:"checks"`
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.readiness == nil {
		s.healthz(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), DefaultCheckTimeout)
	defer cancel()
	checks := s.readiness(ctx)
	var failing []string
	for _, c := range checks {
		if c.OK {
			continue
		}
		desc := c.Name
		if c.Sink != "" {
			desc += " " + c.Sink
		}
		if c.Code != "" {
			desc += " (" + c.Code + ")"
		}
		if c.Reason != "" {
			desc += ": " + c.Reason
		}
		failing = append(failing, desc)
	}
	if len(failing) == 0 {
		s.writeJSON(w, http.StatusOK, readyBody{Status: "ok", Checks: checks})
		return
	}
	s.writeJSON(w, http.StatusServiceUnavailable, errorBody{
		Error: errorDetail{
			Code: CodeNotReady, Message: "not ready: " + strings.Join(failing, "; "),
			RequestID: requestIDFrom(r.Context()),
		},
		Checks: checks,
	})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, s.info)
}

func (s *Server) statusDoc(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), DefaultCheckTimeout)
	defer cancel()
	doc, err := s.status(ctx)
	if err != nil {
		s.writeError(w, r, http.StatusServiceUnavailable, CodeStatusUnavailable, err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, doc)
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	s.writeJSON(w, status, errorBody{Error: errorDetail{
		Code: code, Message: msg, RequestID: requestIDFrom(r.Context()),
	}})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Warn("write admin response", "error", err)
	}
}
