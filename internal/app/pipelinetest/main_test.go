// SPDX-License-Identifier: Apache-2.0

// Package pipelinetest wires the stream and snapshot use cases together
// over an in-memory Postgres (fakedb_test.go) and checks the end-to-end
// guarantees of snapshots and acknowledgements: the final sink state equals the database
// after initial snapshots, table snapshots under concurrent writes, chunk
// retries and a crash during the initial snapshot. It contains tests only.
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
