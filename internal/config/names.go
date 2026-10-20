// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"regexp"
	"strings"
)

// MaxTopicLength is the longest Kafka topic name Wake accepts (Kafka's own
// limit is 249 characters).
const MaxTopicLength = 249

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ValidIdentifier reports whether s is a schema, table or prefix name that may
// be used in topic and subject names without mapping: [A-Za-z0-9_-]+.
func ValidIdentifier(s string) bool { return identifierRE.MatchString(s) }

// SplitQualified splits "<schema>.<table>" at the first dot. ok is false when
// either part is empty or there is no dot.
func SplitQualified(name string) (schema, table string, ok bool) {
	schema, table, found := strings.Cut(name, ".")
	if !found || schema == "" || table == "" {
		return "", "", false
	}
	return schema, table, true
}

// TopicName returns "<prefix>.<schema>.<table>" after checking that the three
// parts are valid identifiers and the result fits MaxTopicLength. The names
// must already be mapped through tables.name_map. It is exported so the
// application can check tables detected at runtime.
func TopicName(prefix, schema, table string) (string, error) {
	for _, part := range []struct{ what, value string }{
		{"prefix", prefix}, {"schema", schema}, {"table", table},
	} {
		if !ValidIdentifier(part.value) {
			return "", fmt.Errorf("%w: %s %q must match [A-Za-z0-9_-]+", ErrIdentifierInvalid, part.what, part.value)
		}
	}
	name := prefix + "." + schema + "." + table
	if len(name) > MaxTopicLength {
		return "", fmt.Errorf("%w: %d characters (limit %d): %s", ErrTopicTooLong, len(name), MaxTopicLength, name)
	}
	return name, nil
}

// MappedName returns the schema and table names events carry and topics use:
// the tables.name_map target when one exists, else the inputs unchanged.
func (t Tables) MappedName(schema, table string) (mappedSchema, mappedTable string) {
	if target, ok := t.NameMap[schema+"."+table]; ok {
		if s, tb, valid := SplitQualified(target); valid {
			return s, tb
		}
	}
	return schema, table
}
