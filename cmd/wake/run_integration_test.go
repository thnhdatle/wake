// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
)

func TestWake(t *testing.T) { //nolint:paralleltest,tparallel // owns the shared Kafka; not parallel to other tests
	k := kafkatest.Start(t)
	tests := []struct {
		name   string
		source string
		run    func(e *env)
	}{
		{"F3_InitialSnapshotUnderWrites", "f3", testInitialSnapshotUnderWrites},
		{"F3_KillDuringInitialSnapshot", "f3kill", testKillDuringInitialSnapshot},
		{"F3b_AddTableUnderLoad", "f3b", testAddTableUnderLoad},
		{"F1_SIGHUPReloadsGlobs", "hup", testSIGHUPReloadsGlobs},
		{"F6_SchemaChanges", "f6", testSchemaChanges},
		{"F7_HeartbeatAdvancesIdleSlot", "f7", testHeartbeat},
		{"F1_KeylessExistingMode", "f1e", testKeylessExisting},
		{"F1_KeylessManagedMode", "f1m", testKeylessManaged},
		{"F5_OversizedEventBlocksSink", "rz", testOversizedEventBlocksSink},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(newEnv(t, k, tc.source))
		})
	}
}

func testInitialSnapshotUnderWrites(e *env) {
	t := e.t
	e.createItems("items", 5000)
	stop := e.startWriter("items", 1_000_000, 1)
	cfg := e.config()

	p := e.startWake(&cfg)
	p.waitSnapshotsIdle()
	p.waitReady()
	after := e.currentLSN()
	p.waitFor(waitShort, "stream past the snapshot", func() bool {
		doc, ok := p.status()
		c, err := lsnOf(doc.ConfirmedLSN)
		return ok && err == nil && c > after
	})
	stop()
	e.sync(p)

	stats := e.assertSinkEqualsSource("items")
	assert.Positive(t, stats.Events)
	p.stop()
}
