// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func healthyFacts() *CheckFacts {
	return &CheckFacts{
		Settings: pipeline.ServerSettings{
			ServerVersionNum: 170002, WALLevel: "logical", MaxReplicationSlots: 10, MaxWALSenders: 10,
			MaxSlotWALKeepSize: 1 << 30,
		},
		Usage:      Usage{Slots: 2, Senders: 1},
		System:     pipeline.SystemInfo{SystemID: "7"},
		CanCreate:  true,
		Candidates: []Table{tbl("orders", 1, true, true)},
		Main:       ExistingPublication{Name: "wake_app"},
		Insert:     ExistingPublication{Name: "wake_app_insert"},
		Existing: ExistingPublication{
			Name: "pub", Exists: true, ViaRoot: true,
			Tables: []Table{tbl("orders", 1, true, false)},
		},
	}
}

func managedConfig(t *testing.T, emit, skip bool) *CheckConfig {
	t.Helper()
	p, err := pipeline.NewTablePolicy(pipeline.TablePolicyConfig{DB: "app", EmitUnkeyed: emit})
	require.NoError(t, err)
	return &CheckConfig{
		SlotName: "wake_app", Managed: true, Source: "app", EmitUnkeyed: emit, SkipUnkeyed: skip,
		Resolver: p,
	}
}

func byCheck(r *Report) map[string]string {
	out := map[string]string{}
	for _, f := range r.Findings {
		key := f.Check
		for n := 2; ; n++ {
			if _, dup := out[key]; !dup {
				break
			}
			key = f.Check + "#" + string(rune('0'+n))
		}
		out[key] = string(f.Status) + ":" + f.Code
	}
	return out
}

func TestEvaluate_HealthyManagedSourcePasses(t *testing.T) {
	t.Parallel()
	r := Evaluate(healthyFacts(), managedConfig(t, false, false))
	assert.False(t, r.Failed())
	assert.Equal(t, map[string]string{
		CheckServerVersion: "ok:", CheckWALLevel: "ok:", CheckReplication: "ok:", CheckSlot: "ok:",
		CheckWALSenders: "ok:", CheckPublication: "ok:", CheckTables: "ok:", CheckKeyless: "ok:",
		CheckIdentifiers: "ok:", CheckOwnership: "ok:", CheckWALRetention: "ok:",
	}, byCheck(&r))
}

func TestEvaluate_ReportsEachProblem(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		edit  func(*CheckFacts)
		cfg   func(*testing.T) *CheckConfig
		check string
		want  string
	}{
		{
			name: "old server", edit: func(f *CheckFacts) { f.Settings.ServerVersionNum = 130010 },
			check: CheckServerVersion, want: "fail:" + CodeServerVersion,
		},
		{
			name: "wal level", edit: func(f *CheckFacts) { f.Settings.WALLevel = "replica" },
			check: CheckWALLevel, want: "fail:" + CodeWALLevel,
		},
		{
			name: "privilege", edit: func(f *CheckFacts) { f.ReplicationErr = errors.New("denied") },
			check: CheckReplication, want: "fail:" + CodeReplicationPrivilege,
		},
		{name: "coded replication error", edit: func(f *CheckFacts) {
			f.ReplicationErr = codedError(CodeNoFreeWALSender, ErrInvalidArgument, "full")
		}, check: CheckReplication, want: "fail:" + CodeNoFreeWALSender},
		{
			name: "slot lost", edit: func(f *CheckFacts) { f.Slot = pipeline.SlotStatus{Exists: true, WALStatus: "lost"} },
			check: CheckSlot, want: "fail:" + CodeSlotInvalidated,
		},
		{
			name: "slot in use", edit: func(f *CheckFacts) { f.Slot = pipeline.SlotStatus{Exists: true, ActivePID: 9} },
			check: CheckSlot, want: "fail:" + CodeSlotInUse,
		},
		{name: "existing slot ok", edit: func(f *CheckFacts) {
			f.Slot = pipeline.SlotStatus{Exists: true}
			f.Usage.Slots = 10
		}, check: CheckSlot, want: "ok:"},
		{
			name: "no free slot", edit: func(f *CheckFacts) { f.Usage.Slots = 10 },
			check: CheckSlot, want: "fail:" + CodeNoFreeSlot,
		},
		{
			name: "no free sender", edit: func(f *CheckFacts) { f.Usage.Senders = 10 },
			check: CheckWALSenders, want: "fail:" + CodeNoFreeWALSender,
		},
		{
			name: "cannot create publication", edit: func(f *CheckFacts) { f.CanCreate = false },
			check: CheckPublication, want: "fail:" + CodePublicationPrivilege,
		},
		{name: "existing managed publication without create", edit: func(f *CheckFacts) {
			f.CanCreate, f.Main.Exists, f.Main.Owned = false, true, true
		}, check: CheckPublication, want: "ok:"},
		{
			name: "foreign publication", edit: func(f *CheckFacts) { f.Main.Exists = true },
			check: CheckPublication, want: "fail:" + CodePublicationPrivilege,
		},
		{
			name: "all tables publication", edit: func(f *CheckFacts) { f.Main.Exists, f.Main.AllTables = true, true },
			check: CheckPublication, want: "fail:" + CodePublicationConflict,
		},
		{
			name: "insert publication checked with emit_unkeyed", edit: func(f *CheckFacts) {
				f.Main.Exists, f.Main.Owned, f.Insert.Exists = true, true, true
			}, cfg: func(t *testing.T) *CheckConfig { t.Helper(); return managedConfig(t, true, false) },
			check: CheckPublication + "#2", want: "fail:" + CodePublicationPrivilege,
		},
		{
			name: "keyless fails", edit: func(f *CheckFacts) { f.Candidates[0].HasKey = false },
			check: CheckKeyless, want: "fail:" + CodeTableKeyless,
		},
		{
			name: "keyless skipped warns", edit: func(f *CheckFacts) { f.Candidates[0].HasKey = false },
			cfg:   func(t *testing.T) *CheckConfig { t.Helper(); return managedConfig(t, false, true) },
			check: CheckKeyless, want: "warn:" + CodeTableKeyless,
		},
		{
			name: "keyless emitted", edit: func(f *CheckFacts) { f.Candidates[0].HasKey = false },
			cfg:   func(t *testing.T) *CheckConfig { t.Helper(); return managedConfig(t, true, false) },
			check: CheckKeyless, want: "ok:",
		},
		{
			name: "bad identifier", edit: func(f *CheckFacts) { f.Candidates[0].Name = "bad name" },
			check: CheckIdentifiers, want: "fail:" + CodeIdentifierInvalid,
		},
		{
			name: "not owned", edit: func(f *CheckFacts) { f.Candidates[0].Owned = false },
			check: CheckOwnership, want: "fail:" + CodePublicationPrivilege,
		},
		{
			name: "unlimited retention", edit: func(f *CheckFacts) { f.Settings.MaxSlotWALKeepSize = -1 },
			check: CheckWALRetention, want: "warn:" + CodeWALRetention,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := healthyFacts()
			tc.edit(f)
			cfg := managedConfig(t, false, false)
			if tc.cfg != nil {
				cfg = tc.cfg(t)
			}
			r := Evaluate(f, cfg)
			assert.Equal(t, tc.want, byCheck(&r)[tc.check])
			assert.Equal(t, strings.HasPrefix(tc.want, "fail"), r.Failed())
		})
	}
}

func TestEvaluate_ExistingMode(t *testing.T) {
	t.Parallel()
	cfg := managedConfig(t, false, false)
	cfg.Managed, cfg.Publication = false, "pub"
	f := healthyFacts()
	r := Evaluate(f, cfg)
	assert.False(t, r.Failed())
	got := byCheck(&r)
	assert.Equal(t, "ok:", got[CheckPublication])
	assert.NotContains(t, got, CheckOwnership, "existing mode does not need ownership")

	f.Existing.ViaRoot = false
	f.Existing.Tables = append(f.Existing.Tables, Table{
		Schema: "public", Name: "orders_2026", Partition: true,
		HasKey: true,
	})
	r = Evaluate(f, cfg)
	got = byCheck(&r)
	assert.Equal(t, "warn:"+CodePartitionRoot, got[CheckPublication+"#2"])
	for _, finding := range r.Findings {
		if finding.Code == CodePartitionRoot {
			assert.Equal(t, []string{"public.orders_2026"}, finding.Tables)
		}
	}

	f.Existing.Exists = false
	r = Evaluate(f, cfg)
	assert.Equal(t, "fail:"+CodePublicationMissing, byCheck(&r)[CheckPublication])
}
