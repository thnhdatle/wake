// SPDX-License-Identifier: Apache-2.0

//go:build integration && !race

package main

// raceEnabled builds the wake binary of the integration tests with -race
// when the tests themselves run with it.
const raceEnabled = false
