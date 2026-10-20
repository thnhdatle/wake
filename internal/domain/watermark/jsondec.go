// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// errField is returned by field decoders for an unknown or null member; the
// caller wraps it with its own sentinel.
var errField = errors.New("bad member")

// decodeObject decodes a single JSON object strictly: valid UTF-8, no
// duplicate or unknown members (field returns errField for unknown names),
// nothing after the closing brace. field decodes the value of one member.
func decodeObject(b []byte, field func(name string, dec *json.Decoder) error) error {
	if !utf8.Valid(b) {
		return errors.New("invalid utf-8")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return errors.New("not an object")
	}
	seen := make(map[string]bool, 8)
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("member name: %w", err)
		}
		name, _ := tok.(string)
		if seen[name] {
			return fmt.Errorf("duplicate member %q", name)
		}
		seen[name] = true
		if err := field(name, dec); err != nil {
			return fmt.Errorf("member %q: %w", name, err)
		}
	}
	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("object end: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("data after object")
	}
	return nil
}

// decodeString decodes a non-null JSON string.
func decodeString(dec *json.Decoder) (string, error) {
	var s *string
	if err := dec.Decode(&s); err != nil {
		return "", fmt.Errorf("decode string: %w", err)
	}
	if s == nil {
		return "", errField
	}
	return *s, nil
}

// decodeUint decodes a non-null JSON integer in uint64 range.
func decodeUint(dec *json.Decoder) (uint64, error) {
	var v *uint64
	if err := dec.Decode(&v); err != nil {
		return 0, fmt.Errorf("decode integer: %w", err)
	}
	if v == nil {
		return 0, errField
	}
	return *v, nil
}

// decodeUints decodes a non-null JSON array of non-null uint64 integers.
// An empty array decodes as nil.
func decodeUints(dec *json.Decoder) ([]uint64, error) {
	var raw *[]*uint64
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode integer array: %w", err)
	}
	if raw == nil {
		return nil, errField
	}
	if len(*raw) == 0 {
		return nil, nil
	}
	out := make([]uint64, 0, len(*raw))
	for _, v := range *raw {
		if v == nil {
			return nil, errField
		}
		out = append(out, *v)
	}
	return out, nil
}
