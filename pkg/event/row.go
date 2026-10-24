// SPDX-License-Identifier: Apache-2.0

package event

import "encoding/json"

// Row is a row image or key mapping column names to raw JSON values.
type Row map[string]json.RawMessage

// Has reports whether the row carries the column, including with a null value.
func (r Row) Has(col string) bool {
	_, ok := r[col]
	return ok
}

// Value returns the raw value of the column and whether the column is present.
func (r Row) Value(col string) (Value, bool) {
	v, ok := r[col]
	return Value(v), ok
}
