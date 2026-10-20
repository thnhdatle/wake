// SPDX-License-Identifier: Apache-2.0

// Command wake is the Wake change-data-capture service and its CLI.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "wake:", describe(err))
		stop()
		os.Exit(exitCode(err)) //nolint:gocritic // stop() is called explicitly above
	}
}
