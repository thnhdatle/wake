// SPDX-License-Identifier: Apache-2.0

package pipelinetest

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

const (
	testTimeout  = 30 * time.Second
	pollInterval = time.Millisecond
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
