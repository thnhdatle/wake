// SPDX-License-Identifier: Apache-2.0

// Package runner runs the acceptance suite behind `make acceptance`:
// build, bring up the compose stack, probe environment requirements, execute
// each criterion, render acceptance/report.md and run the source checks.
// All external commands go through the Exec interface so the logic is testable
// without Docker.
package runner
