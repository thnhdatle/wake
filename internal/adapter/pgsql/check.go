// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

type Status string

const (
	StatusOK   Status = "ok"
	StatusWarn Status = "warn"
	StatusFail Status = "fail"
)

const (
	CheckServerVersion = "server_version"
	CheckWALLevel      = "wal_level"
	CheckReplication   = "replication_privilege"
	CheckSlot          = "replication_slot"
	CheckWALSenders    = "wal_senders"
	CheckPublication   = "publication"
	CheckTables        = "tables"
	CheckKeyless       = "keyless_tables"
	CheckIdentifiers   = "identifiers"
	CheckOwnership     = "table_ownership"
	CheckWALRetention  = "wal_retention"
)

const minServerVersion = 140000

type Finding struct {
	Check   string
	Status  Status
	Code    string
	Message string
	Tables  []string
}

type Report struct {
	Findings []Finding
}

func (r *Report) Failed() bool {
	for _, f := range r.Findings {
		if f.Status == StatusFail {
			return true
		}
	}
	return false
}

func (r *Report) add(check string, st Status, code, msg string, tables ...string) {
	r.Findings = append(r.Findings, Finding{Check: check, Status: st, Code: code, Message: msg, Tables: tables})
}

type CheckConfig struct {
	SlotName    string
	Managed     bool
	Source      string
	Publication string
	EmitUnkeyed bool
	SkipUnkeyed bool
	Resolver    pipeline.TableResolver
}

type CheckFacts struct {
	Settings       pipeline.ServerSettings
	Usage          Usage
	Slot           pipeline.SlotStatus
	System         pipeline.SystemInfo
	ReplicationErr error
	CanCreate      bool
	Candidates     []Table
	Main           ExistingPublication
	Insert         ExistingPublication
	Existing       ExistingPublication
}

// Evaluate turns the gathered facts into a check report.
func Evaluate(f *CheckFacts, cfg *CheckConfig) Report {
	var r Report
	checkServer(&r, f)
	checkReplication(&r, f, cfg)
	if cfg.Managed {
		checkManagedPublications(&r, f, cfg)
		checkTables(&r, f.Candidates, cfg, true)
	} else {
		checkExistingPublication(&r, f)
		checkTables(&r, f.Existing.Tables, cfg, false)
	}
	checkRetention(&r, f)
	return r
}

func checkServer(r *Report, f *CheckFacts) {
	s := f.Settings
	if s.ServerVersionNum < minServerVersion {
		r.add(CheckServerVersion, StatusFail, CodeServerVersion,
			fmt.Sprintf("server_version_num %d: Postgres 14 or later is required", s.ServerVersionNum))
	} else {
		r.add(CheckServerVersion, StatusOK, "", "server_version_num "+strconv.Itoa(s.ServerVersionNum))
	}
	if s.WALLevel != "logical" {
		r.add(CheckWALLevel, StatusFail, CodeWALLevel, fmt.Sprintf("wal_level is %q; set wal_level=logical and "+
			"restart (RDS/Aurora: rds.logical_replication=1 and reboot; Cloud SQL: cloudsql.logical_decoding=on; "+
			"Azure: wal_level=logical and restart)", s.WALLevel))
	} else {
		r.add(CheckWALLevel, StatusOK, "", "wal_level is logical")
	}
}

func codeOf(err error, def string) string {
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		return coded.ErrorCode()
	}
	return def
}

func checkReplication(r *Report, f *CheckFacts, cfg *CheckConfig) {
	if f.ReplicationErr != nil {
		r.add(CheckReplication, StatusFail, codeOf(f.ReplicationErr, CodeReplicationPrivilege),
			"replication connection failed: "+f.ReplicationErr.Error())
	} else {
		r.add(CheckReplication, StatusOK, "", "IDENTIFY_SYSTEM succeeded (system "+f.System.SystemID+")")
	}
	s, u, slot := f.Settings, f.Usage, f.Slot
	switch {
	case slot.Exists && slot.WALStatus == "lost":
		r.add(CheckSlot, StatusFail, CodeSlotInvalidated, fmt.Sprintf("slot %s is invalidated (wal_status lost); "+
			"drop it with `wake slot drop --yes` and start with --reinit-state", cfg.SlotName))
	case slot.Exists && slot.ActivePID != 0:
		r.add(CheckSlot, StatusFail, CodeSlotInUse, fmt.Sprintf("slot %s is used by pid %d", cfg.SlotName,
			slot.ActivePID))
	case slot.Exists:
		r.add(CheckSlot, StatusOK, "", fmt.Sprintf("slot %s exists (confirmed_flush_lsn %s, lag %d bytes)",
			cfg.SlotName, slot.ConfirmedFlush, slot.LagBytes))
	case u.Slots >= s.MaxReplicationSlots:
		r.add(CheckSlot, StatusFail, CodeNoFreeSlot, fmt.Sprintf("slot %s must be created but all %d "+
			"max_replication_slots are in use; raise it (restart) or drop an unused slot", cfg.SlotName,
			s.MaxReplicationSlots))
	default:
		r.add(CheckSlot, StatusOK, "", fmt.Sprintf("slot %s will be created (%d of %d max_replication_slots in use)",
			cfg.SlotName, u.Slots, s.MaxReplicationSlots))
	}
	if u.Senders >= s.MaxWALSenders {
		r.add(CheckWALSenders, StatusFail, CodeNoFreeWALSender, fmt.Sprintf("all %d max_wal_senders are in use; "+
			"raise it (restart) or stop another replication client", s.MaxWALSenders))
	} else {
		r.add(CheckWALSenders, StatusOK, "", fmt.Sprintf("%d of %d max_wal_senders in use", u.Senders,
			s.MaxWALSenders))
	}
}

func checkManagedPublications(r *Report, f *CheckFacts, cfg *CheckConfig) {
	pubs := []*ExistingPublication{&f.Main}
	if cfg.EmitUnkeyed {
		pubs = append(pubs, &f.Insert)
	}
	for _, p := range pubs {
		switch {
		case p.Exists && p.AllTables:
			r.add(CheckPublication, StatusFail, CodePublicationConflict, fmt.Sprintf("publication %s is FOR ALL "+
				"TABLES; managed mode cannot alter it", p.Name))
		case p.Exists && !p.Owned:
			r.add(CheckPublication, StatusFail, CodePublicationPrivilege, fmt.Sprintf("publication %s is owned "+
				"by another role; managed mode must own it", p.Name))
		case p.Exists:
			r.add(CheckPublication, StatusOK, "", fmt.Sprintf("publication %s exists and is owned", p.Name))
		case !f.CanCreate:
			r.add(CheckPublication, StatusFail, CodePublicationPrivilege, fmt.Sprintf("publication %s must be "+
				"created but the role lacks CREATE on the database; grant it or use existing mode", p.Name))
		default:
			r.add(CheckPublication, StatusOK, "", fmt.Sprintf("publication %s will be created (CREATE on the "+
				"database granted)", p.Name))
		}
	}
}

func checkExistingPublication(r *Report, f *CheckFacts) {
	p := &f.Existing
	if !p.Exists {
		r.add(CheckPublication, StatusFail, CodePublicationMissing, fmt.Sprintf("publication %s does not exist; "+
			"create it (CREATE PUBLICATION … WITH (publish_via_partition_root = true)) or use managed mode", p.Name))
		return
	}
	r.add(CheckPublication, StatusOK, "", fmt.Sprintf("publication %s exists (%d tables)", p.Name, len(p.Tables)))
	if leaves := p.PartitionLeaves(); len(leaves) > 0 {
		r.add(CheckPublication, StatusWarn, CodePartitionRoot, fmt.Sprintf("publication %s has "+
			"publish_via_partition_root = false: events of partitioned tables carry the leaf partition names",
			p.Name), names(leaves)...)
	}
}

func checkTables(r *Report, tables []Table, cfg *CheckConfig, managed bool) {
	var captured, invalid, keyless, unkeyed, notOwned []string
	for i := range tables {
		t := &tables[i]
		d := cfg.Resolver.Resolve(t.Info())
		switch {
		case d.Capture:
			captured = append(captured, t.QualifiedName())
			if !t.HasKey {
				unkeyed = append(unkeyed, t.QualifiedName())
			}
			if managed && !t.Owned {
				notOwned = append(notOwned, t.QualifiedName())
			}
		case d.Skip == pipeline.SkipIdentifierInvalid:
			invalid = append(invalid, t.QualifiedName())
		case d.Skip == pipeline.SkipKeyless:
			keyless = append(keyless, t.QualifiedName())
		}
	}
	r.add(CheckTables, StatusOK, "", fmt.Sprintf("%d tables captured", len(captured)), captured...)
	switch {
	case len(keyless) > 0 && cfg.SkipUnkeyed:
		r.add(CheckKeyless, StatusWarn, CodeTableKeyless, "keyless tables are skipped (tables.skip_unkeyed): no "+
			"events", keyless...)
	case len(keyless) > 0:
		r.add(CheckKeyless, StatusFail, CodeTableKeyless, "keyless tables (no primary key or replica identity): "+
			"add a key, set tables.emit_unkeyed (inserts only, key null) or tables.skip_unkeyed", keyless...)
	case len(unkeyed) > 0:
		r.add(CheckKeyless, StatusOK, "", "keyless tables emit inserts only with key null (tables.emit_unkeyed)",
			unkeyed...)
	default:
		r.add(CheckKeyless, StatusOK, "", "no keyless tables")
	}
	if len(invalid) > 0 {
		r.add(CheckIdentifiers, StatusFail, CodeIdentifierInvalid, "schema or table names outside "+
			"[A-Za-z0-9_-]+: map them in tables.name_map", invalid...)
	} else {
		r.add(CheckIdentifiers, StatusOK, "", "all names are valid identifiers")
	}
	if len(notOwned) > 0 {
		r.add(CheckOwnership, StatusFail, CodePublicationPrivilege, "managed mode must own every matched table",
			notOwned...)
	} else if managed {
		r.add(CheckOwnership, StatusOK, "", "the role owns every matched table")
	}
}

func checkRetention(r *Report, f *CheckFacts) {
	keep, lag := f.Settings.MaxSlotWALKeepSize, f.Slot.LagBytes
	if keep < 0 {
		r.add(CheckWALRetention, StatusWarn, CodeWALRetention, fmt.Sprintf("max_slot_wal_keep_size is unlimited "+
			"(-1): a stopped Wake retains WAL until the disk fills; size it per docs/guides/operations.md "+
			"(current slot lag %d bytes)", lag))
		return
	}
	r.add(CheckWALRetention, StatusOK, "", fmt.Sprintf("max_slot_wal_keep_size is %d bytes (current slot lag %d "+
		"bytes); see docs/guides/operations.md for sizing", keep, lag))
}

func names(ts []Table) []string {
	out := make([]string, len(ts))
	for i := range ts {
		out[i] = ts[i].QualifiedName()
	}
	return out
}
