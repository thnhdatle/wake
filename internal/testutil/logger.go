// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"io"
	"log/slog"
)

// DiscardLogger returns a logger that drops every record.
func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
