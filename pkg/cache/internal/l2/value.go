// SPDX-License-Identifier: Apache-2.0

package l2

import (
	"bytes"
	"fmt"
	"strconv"
)

// StoredValue is the L2 value record {gen, expires_at, payload}.
//
// Stored form, written by the scripts as one Redis string so Lua can compare
// the generation as a string without parsing numbers:
//
//	<gen>|<expires_at_ms>|<payload>
type StoredValue struct {
	Gen         uint64
	ExpiresAtMS int64
	Payload     []byte
}

// EncodeValue returns the stored form of v.
func EncodeValue(v StoredValue) []byte {
	b := make([]byte, 0, len(v.Payload)+40)
	b = strconv.AppendUint(b, v.Gen, 10)
	b = append(b, '|')
	b = strconv.AppendInt(b, v.ExpiresAtMS, 10)
	b = append(b, '|')
	return append(b, v.Payload...)
}

// DecodeValue parses the stored form. The payload aliases b.
func DecodeValue(b []byte) (StoredValue, error) {
	genText, rest, ok1 := bytes.Cut(b, []byte(sep))
	expText, payload, ok2 := bytes.Cut(rest, []byte(sep))
	gen, err1 := strconv.ParseUint(string(genText), 10, 64)
	exp, err2 := strconv.ParseInt(string(expText), 10, 64)
	if !ok1 || !ok2 || err1 != nil || err2 != nil {
		return StoredValue{}, fmt.Errorf("decode value: %w", ErrMalformed)
	}
	return StoredValue{Gen: gen, ExpiresAtMS: exp, Payload: payload}, nil
}
