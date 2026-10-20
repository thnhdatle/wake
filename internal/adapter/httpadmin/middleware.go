// SPDX-License-Identifier: Apache-2.0

package httpadmin

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"runtime/debug"

	"github.com/oklog/ulid/v2"
)

// RequestIDHeader carries the request id.
const RequestIDHeader = "X-Request-Id"

// requestIDRE accepts client request ids: printable token characters only,
// bounded length, so they are safe to echo and log.
var requestIDRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type requestIDKey struct{}

// requestID accepts a valid X-Request-Id or generates a ULID, stores it in
// the request context and echoes it in the response.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !requestIDRE.MatchString(id) {
			id = ulid.Make().String()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// recoverer turns a handler panic into a logged 500 with the error envelope.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler { //nolint:errorlint // sentinel compared as panic value per net/http
					panic(v)
				}
				s.log.Error("admin handler panic", "error", fmt.Sprint(v), "operation", r.URL.Path,
					"stack", string(debug.Stack()))
				// The request id middleware runs inside this one; it already
				// set the response header.
				s.writeJSON(w, http.StatusInternalServerError, errorBody{Error: errorDetail{
					Code: CodeInternal, Message: "internal error", RequestID: w.Header().Get(RequestIDHeader),
				}})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
