// SPDX-License-Identifier: Apache-2.0

package redisl2_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a bus goroutine outlives Close.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
