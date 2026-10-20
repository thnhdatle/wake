// SPDX-License-Identifier: Apache-2.0

package event

// Source identifies the table a change belongs to.
type Source struct {
	// DB is the logical source name (the producer's source.name).
	DB string `json:"db"`
	// Schema is the Postgres schema name.
	Schema string `json:"schema"`
	// Table is the Postgres table name.
	Table string `json:"table"`
}

// valid reports whether the identifying parts are present.
func (s Source) valid() bool {
	return s.Schema != "" && s.Table != ""
}
