// SPDX-License-Identifier: Apache-2.0

package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
)

// Value is the raw JSON of one column value.
type Value json.RawMessage

// Raw returns the value as raw JSON.
func (v Value) Raw() json.RawMessage {
	return json.RawMessage(bytes.TrimSpace(v))
}

// IsNull reports whether the value is JSON null or empty.
func (v Value) IsNull() bool {
	t := bytes.TrimSpace(v)
	return len(t) == 0 || string(t) == nullLiteral
}

// Int64 returns the value as an int64.
func (v Value) Int64() (int64, error) {
	lit, err := v.integer()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s does not fit int64", ErrValueType, lit)
	}
	return n, nil
}

// Uint64 returns the value as a uint64.
func (v Value) Uint64() (uint64, error) {
	lit, err := v.integer()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(lit, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s does not fit uint64", ErrValueType, lit)
	}
	return n, nil
}

// BigInt returns the value as an arbitrary-precision integer.
func (v Value) BigInt() (*big.Int, error) {
	lit, err := v.integer()
	if err != nil {
		return nil, err
	}
	n, ok := new(big.Int).SetString(lit, 10)
	if !ok {
		return nil, fmt.Errorf("%w: %s is not an integer", ErrValueType, lit)
	}
	return n, nil
}

// Number returns the value as a json.Number holding the exact literal.
func (v Value) Number() (json.Number, error) {
	t := bytes.TrimSpace(v)
	if !isNumber(t) {
		return "", fmt.Errorf("%w: not a JSON number", ErrValueType)
	}
	return json.Number(t), nil
}

// String returns the unescaped content of a JSON string.
func (v Value) String() (string, error) {
	t := bytes.TrimSpace(v)
	if len(t) == 0 || t[0] != '"' {
		return "", fmt.Errorf("%w: not a JSON string", ErrValueType)
	}
	var s string
	if err := json.Unmarshal(t, &s); err != nil {
		return "", fmt.Errorf("%w: bad JSON string: %w", ErrValueType, err)
	}
	return s, nil
}

// Bool returns the value as a bool.
func (v Value) Bool() (bool, error) {
	switch string(bytes.TrimSpace(v)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, fmt.Errorf("%w: not a JSON boolean", ErrValueType)
}

// Text renders a scalar for use in a template or a key.
func (v Value) Text() (string, error) {
	t := bytes.TrimSpace(v)
	switch {
	case len(t) == 0, string(t) == nullLiteral:
		return "", fmt.Errorf("%w: null has no text form", ErrValueType)
	case t[0] == '"':
		return v.String()
	case string(t) == "true", string(t) == "false":
		return string(t), nil
	case isNumber(t):
		return string(t), nil
	}
	return "", fmt.Errorf("%w: object or array has no text form", ErrValueType)
}

func (v Value) integer() (string, error) {
	t := bytes.TrimSpace(v)
	if !isNumber(t) {
		return "", fmt.Errorf("%w: not a JSON number", ErrValueType)
	}
	for _, c := range t {
		if c == '.' || c == 'e' || c == 'E' {
			return "", fmt.Errorf("%w: %s is not an integer", ErrValueType, t)
		}
	}
	return string(t), nil
}

func isNumber(b []byte) bool {
	if len(b) == 0 || (b[0] != '-' && (b[0] < '0' || b[0] > '9')) {
		return false
	}
	return json.Valid(b)
}
