// SPDX-License-Identifier: Apache-2.0

package event

import (
	"encoding/json"
	"fmt"
)

const (
	nullLiteral = "null"

	fieldType          = "type"
	fieldID            = "id"
	fieldSource        = "source"
	fieldOp            = "op"
	fieldTsMs          = "ts_ms"
	fieldLSN           = "lsn"
	fieldKey           = "key"
	fieldBefore        = "before"
	fieldAfter         = "after"
	fieldSchemaVersion = "schema_version"
	fieldColsBefore    = "columns_before"
	fieldColsAfter     = "columns_after"
	fieldTransactional = "transactional"
	fieldPrefix        = "prefix"
	fieldContent       = "content_base64"
)

func probe(data []byte, required, nonNull []string) error {
	if IsKafkaTombstone(data) {
		return ErrTombstone
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if members == nil {
		return fmt.Errorf("%w: not a JSON object", ErrInvalid)
	}
	for _, name := range required {
		if _, ok := members[name]; !ok {
			return fmt.Errorf("%w: missing field %q", ErrInvalid, name)
		}
	}
	for _, name := range nonNull {
		if string(members[name]) == nullLiteral {
			return fmt.Errorf("%w: field %q is null", ErrInvalid, name)
		}
	}
	return nil
}

func unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func checkCommon(lsn string, tsMs, schemaVersion int64, src Source) error {
	if _, err := ParseLSN(lsn); err != nil {
		return err
	}
	switch {
	case tsMs < 0:
		return fmt.Errorf("%w: negative ts_ms", ErrInvalid)
	case schemaVersion < 1:
		return fmt.Errorf("%w: schema_version %d below 1", ErrInvalid, schemaVersion)
	case !src.valid():
		return fmt.Errorf("%w: source needs schema and table", ErrInvalid)
	}
	return nil
}
