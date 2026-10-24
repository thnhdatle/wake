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

const RequestIDHeader = "X-Request-Id"

var requestIDRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type requestIDKey struct{}

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

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler { //nolint:errorlint // sentinel compared as panic value per net/http
					panic(v)
				}
				s.log.Error("admin handler panic", "error", fmt.Sprint(v), "operation", r.URL.Path,
					"stack", string(debug.Stack()))
				s.writeJSON(w, http.StatusInternalServerError, errorBody{Error: errorDetail{
					Code: CodeInternal, Message: "internal error", RequestID: w.Header().Get(RequestIDHeader),
				}})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
