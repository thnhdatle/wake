// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"encoding/json"
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/change"
)

const TriggerPrefix = "wake.snapshot"

// Trigger is a snapshot request for one table.
type Trigger struct {
	Table string
}

// ParseTrigger decodes a snapshot trigger.
func ParseTrigger(b []byte) (Trigger, error) {
	var t Trigger
	if err := t.UnmarshalJSON(b); err != nil {
		return Trigger{}, err
	}
	return t, nil
}

func (t Trigger) Validate() error {
	if _, _, ok := SplitTable(t.Table); !ok {
		return fmt.Errorf("%w: table %q", ErrInvalidTrigger, t.Table)
	}
	return nil
}

func (t Trigger) Split() (schema, table string) {
	schema, table, _ = SplitTable(t.Table)
	return schema, table
}

func (t Trigger) MarshalJSON() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	dst := append(make([]byte, 0, 16+len(t.Table)), `{"table":`...)
	dst = change.AppendString(dst, t.Table)
	return append(dst, '}'), nil
}

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
