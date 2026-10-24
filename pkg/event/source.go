// SPDX-License-Identifier: Apache-2.0

package event

// Source identifies the table a change belongs to.
type Source struct {
	// DB is the logical source name of the producer.
	DB     string `json:"db"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

func (s Source) valid() bool {
	return s.Schema != "" && s.Table != ""
}
