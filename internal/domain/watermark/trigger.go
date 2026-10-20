// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"encoding/json"
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/change"
)

// TriggerPrefix is the logical message prefix of snapshot requests written
// by `wake snapshot table`.
const TriggerPrefix = "wake.snapshot"

// Trigger is the JSON content of a wake.snapshot message:
// {"table":"<schema>.<table>"}.
type Trigger struct {
	// Table is "<schema>.<table>" (Postgres names).
	Table string
}

// ParseTrigger decodes and validates a wake.snapshot payload with the same
// strictness as ParsePayload.
func ParseTrigger(b []byte) (Trigger, error) {
	var t Trigger
	if err := t.UnmarshalJSON(b); err != nil {
		return Trigger{}, err
	}
	return t, nil
}

// Validate checks that Table is "<schema>.<table>" (SplitTable).
func (t Trigger) Validate() error {
	if _, _, ok := SplitTable(t.Table); !ok {
		return fmt.Errorf("%w: table %q", ErrInvalidTrigger, t.Table)
	}
	return nil
}

// Split returns the schema and table names.
func (t Trigger) Split() (schema, table string) {
	schema, table, _ = SplitTable(t.Table)
	return schema, table
}

// MarshalJSON validates and encodes the trigger.
func (t Trigger) MarshalJSON() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	dst := append(make([]byte, 0, 16+len(t.Table)), `{"table":`...)
	dst = change.AppendString(dst, t.Table)
	return append(dst, '}'), nil
}

// UnmarshalJSON decodes strictly and validates.
func (t *Trigger) UnmarshalJSON(b []byte) error {
	var (
		v    Trigger
		seen bool
	)
	err := decodeObject(b, func(name string, dec *json.Decoder) error {
		if name != memberTable {
			return errField
		}
		seen = true
		var err error
		v.Table, err = decodeString(dec)
		return err
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidTrigger, err)
	}
	if !seen {
		return fmt.Errorf("%w: missing \"table\"", ErrInvalidTrigger)
	}
	if err := v.Validate(); err != nil {
		return err
	}
	*t = v
	return nil
}
