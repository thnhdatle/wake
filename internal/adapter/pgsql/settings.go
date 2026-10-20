// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// settingNames are the pg_settings rows ServerSettings reads.
var settingNames = []string{
	"server_version_num", "wal_level", "wal_sender_timeout", "max_replication_slots", "max_wal_senders",
	"max_slot_wal_keep_size",
}

// setting is one pg_settings row: the value in the setting's unit.
type setting struct {
	value string
	unit  string
}

// parseSettings converts the pg_settings rows of settingNames.
func parseSettings(rows map[string]setting) (pipeline.ServerSettings, error) {
	var s pipeline.ServerSettings
	for _, name := range settingNames {
		if _, ok := rows[name]; !ok {
			return pipeline.ServerSettings{}, fmt.Errorf("pgsql: server setting %s missing", name)
		}
	}
	var err error
	if s.ServerVersionNum, err = intSetting(rows, "server_version_num"); err != nil {
		return pipeline.ServerSettings{}, err
	}
	if s.MaxReplicationSlots, err = intSetting(rows, "max_replication_slots"); err != nil {
		return pipeline.ServerSettings{}, err
	}
	if s.MaxWALSenders, err = intSetting(rows, "max_wal_senders"); err != nil {
		return pipeline.ServerSettings{}, err
	}
	s.WALLevel = rows["wal_level"].value
	if s.WALSenderTimeout, err = durationSetting(rows["wal_sender_timeout"]); err != nil {
		return pipeline.ServerSettings{}, fmt.Errorf("pgsql: wal_sender_timeout: %w", err)
	}
	keep := rows["max_slot_wal_keep_size"]
	if keep.value == "-1" {
		s.MaxSlotWALKeepSize = -1
	} else if s.MaxSlotWALKeepSize, err = sizeSetting(keep); err != nil {
		return pipeline.ServerSettings{}, fmt.Errorf("pgsql: max_slot_wal_keep_size: %w", err)
	}
	return s, nil
}

func intSetting(rows map[string]setting, name string) (int, error) {
	v, err := strconv.Atoi(rows[name].value)
	if err != nil {
		return 0, fmt.Errorf("pgsql: setting %s: %w", name, err)
	}
	return v, nil
}

// splitUnit splits a pg_settings unit such as "8kB" or "ms" into its
// multiplier and base unit.
func splitUnit(unit string) (int64, string, error) {
	i := 0
	for i < len(unit) && unit[i] >= '0' && unit[i] <= '9' {
		i++
	}
	mult := int64(1)
	if i > 0 {
		m, err := strconv.ParseInt(unit[:i], 10, 64)
		if err != nil {
			return 0, "", fmt.Errorf("unit %q: %w", unit, err)
		}
		mult = m
	}
	return mult, strings.TrimSpace(unit[i:]), nil
}

// durationSetting converts a time setting; 0 stays 0.
func durationSetting(s setting) (time.Duration, error) {
	v, err := strconv.ParseInt(s.value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value %q: %w", s.value, err)
	}
	mult, base, err := splitUnit(s.unit)
	if err != nil {
		return 0, err
	}
	units := map[string]time.Duration{
		"us": time.Microsecond, "ms": time.Millisecond, "s": time.Second, "min": time.Minute, "h": time.Hour,
		"d": 24 * time.Hour,
	}
	u, ok := units[base]
	if !ok {
		return 0, fmt.Errorf("unknown time unit %q", s.unit)
	}
	return time.Duration(v*mult) * u, nil
}

// sizeSetting converts a memory setting to bytes.
func sizeSetting(s setting) (int64, error) {
	v, err := strconv.ParseInt(s.value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value %q: %w", s.value, err)
	}
	mult, base, err := splitUnit(s.unit)
	if err != nil {
		return 0, err
	}
	units := map[string]int64{"": 1, "B": 1, "kB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40}
	u, ok := units[base]
	if !ok {
		return 0, fmt.Errorf("unknown size unit %q", s.unit)
	}
	return v * mult * u, nil
}
