// SPDX-License-Identifier: Apache-2.0

package event

import "encoding/json"

// Row is a row image or key: column name to the exact raw JSON value of that
// column. Values are kept as received so integers above 2^53 lose no precision;
// use the typed accessors of Value to read them.
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
