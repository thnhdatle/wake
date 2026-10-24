// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/thnhdatle/wake/internal/domain/change"
)

// AppendJSON appends a Postgres text value of the given type as JSON.
func AppendJSON(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	info, ok := builtinTypes[typeOID]
	if !ok {
		return change.AppendStringBytes(dst, text), nil
	}
	out, err := appendValue(dst, typeOID, info, text)
	if err != nil {
		return dst, err
	}
	return out, nil
}

func appendElement(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	info, ok := builtinTypes[typeOID]
	if !ok {
		return change.AppendStringBytes(dst, text), nil
	}
	return appendValue(dst, typeOID, info, text)
}

func elementError(typeOID uint32, text []byte, err error) error {
	var ve *ValueError
	if errors.As(err, &ve) {
		return newValueError(typeOID, text, "invalid element: "+ve.Reason)
	}
	return err
}

func appendValue(dst []byte, typeOID uint32, info typeInfo, text []byte) ([]byte, error) {
	switch info.kind {
	case kindText:
		return change.AppendStringBytes(dst, text), nil
	case kindInt:
		return appendInt(dst, typeOID, text)
	case kindFloat:
		return appendFloat(dst, typeOID, text)
	case kindBool:
		return appendBool(dst, typeOID, text)
	case kindBytea:
		return appendBytea(dst, typeOID, text)
	case kindJSON:
		return appendRawJSON(dst, typeOID, text)
	case kindDate, kindTimestamp, kindTimestampTZ:
		return appendDateTime(dst, typeOID, info.kind, text)
	case kindTime, kindTimeTZ:
		return appendTimeOfDay(dst, typeOID, info.kind == kindTimeTZ, text)
	case kindInterval:
		return appendInterval(dst, typeOID, text)
	case kindArray:
		return appendArray(dst, typeOID, info, text)
	case kindRange:
		return appendRange(dst, typeOID, info, text)
	case kindMultirange:
		return appendMultirange(dst, typeOID, info, text)
	}
	return dst, newValueError(typeOID, text, "unsupported type kind")
}

func appendInt(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	if !isJSONInteger(text) {
		return dst, newValueError(typeOID, text, "not an integer")
	}
	return append(dst, text...), nil
}

func appendFloat(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	switch string(text) {
	case "NaN", "Infinity", "-Infinity":
		return change.AppendStringBytes(dst, text), nil
	}
	if !isJSONNumber(text) {
		return dst, newValueError(typeOID, text, "not a number")
	}
	return append(dst, text...), nil
}

func appendBool(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	switch string(text) {
	case "t", "true":
		return append(dst, "true"...), nil
	case "f", "false":
		return append(dst, "false"...), nil
	}
	return dst, newValueError(typeOID, text, "not a boolean")
}

func appendRawJSON(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	buf := bytes.NewBuffer(dst)
	if err := json.Compact(buf, text); err != nil {
		return dst, newValueError(typeOID, text, "not valid json")
	}
	return buf.Bytes(), nil
}

func isJSONInteger(b []byte) bool {
	if len(b) > 0 && b[0] == '-' {
		b = b[1:]
	}
	if len(b) == 0 || (b[0] == '0' && len(b) > 1) {
		return false
	}
	return allDigits(b)
}

func isJSONNumber(b []byte) bool {
	end := len(b)
	if i := bytes.IndexAny(b, "eE"); i >= 0 {
		exp := b[i+1:]
		if len(exp) > 0 && (exp[0] == '+' || exp[0] == '-') {
			exp = exp[1:]
		}
		if len(exp) == 0 || !allDigits(exp) {
			return false
		}
		end = i
	}
	mant := b[:end]
	if i := bytes.IndexByte(mant, '.'); i >= 0 {
		if frac := mant[i+1:]; len(frac) == 0 || !allDigits(frac) {
			return false
		}
		mant = mant[:i]
	}
	return isJSONInteger(mant)
}

func allDigits(b []byte) bool {
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

const byteaChunk = 48

func appendBytea(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	if len(text) >= 2 && text[0] == '\\' && text[1] == 'x' {
		return appendHexBytea(dst, typeOID, text)
	}
	raw, ok := unescapeBytea(text)
	if !ok {
		return dst, newValueError(typeOID, text, "invalid escape format")
	}
	dst = append(dst, '"')
	dst = change.AppendBase64(dst, raw)
	return append(dst, '"'), nil
}

func appendHexBytea(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	digits := text[2:]
	if len(digits)%2 != 0 {
		return dst, newValueError(typeOID, text, "odd number of hex digits")
	}
	start := len(dst)
	dst = append(dst, '"')
	var chunk [byteaChunk]byte
	for len(digits) > 0 {
		n := min(len(digits)/2, byteaChunk)
		if _, err := hex.Decode(chunk[:n], digits[:2*n]); err != nil {
			return dst[:start], newValueError(typeOID, text, "invalid hex digit")
		}
		dst = change.AppendBase64(dst, chunk[:n])
		digits = digits[2*n:]
	}
	return append(dst, '"'), nil
}

func unescapeBytea(text []byte) ([]byte, bool) {
	out := make([]byte, 0, len(text))
	for i := 0; i < len(text); {
		c := text[i]
		if c != '\\' {
			out = append(out, c)
			i++
			continue
		}
		switch {
		case i+1 < len(text) && text[i+1] == '\\':
			out = append(out, '\\')
			i += 2
		case i+3 < len(text) && text[i+1] <= '3' && isOctal(text[i+1]) && isOctal(text[i+2]) && isOctal(text[i+3]):
			out = append(out, (text[i+1]-'0')<<6|(text[i+2]-'0')<<3|(text[i+3]-'0'))
			i += 4
		default:
			return nil, false
		}
	}
	return out, true
}

func isOctal(c byte) bool { return c >= '0' && c <= '7' }
