// SPDX-License-Identifier: Apache-2.0

// Command runner is the acceptance runner behind `make acceptance`; see
// acceptance/README.md.
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/thnhdatle/wake/acceptance/internal/runner"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := runner.Main(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr, runner.OSExec{})
	stop()
	os.Exit(code)
}
