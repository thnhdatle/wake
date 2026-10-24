// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"regexp"
	"strings"
)

const MaxTopicLength = 249

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func ValidIdentifier(s string) bool { return identifierRE.MatchString(s) }

func SplitQualified(name string) (schema, table string, ok bool) {
	schema, table, found := strings.Cut(name, ".")
	if !found || schema == "" || table == "" {
		return "", "", false
	}
	return schema, table, true
}

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

func (t Tables) MappedName(schema, table string) (mappedSchema, mappedTable string) {
	if target, ok := t.NameMap[schema+"."+table]; ok {
		if s, tb, valid := SplitQualified(target); valid {
			return s, tb
		}
	}
	return schema, table
}
