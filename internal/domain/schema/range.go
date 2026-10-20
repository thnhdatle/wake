// SPDX-License-Identifier: Apache-2.0

package schema

import "bytes"

// rangeBound is one parsed bound; infinite reports an omitted (unbounded)
// side, which is distinct from a bound whose value is 'infinity'.
type rangeBound struct {
	value    []byte
	infinite bool
}

// appendRange converts range output ("[1,10)", "(,5]", "empty") to
// {"lower":L,"upper":U,"lower_inc":b,"upper_inc":b} with bounds mapped by
// the bound type, null for an unbounded side, or {"empty":true}.
func appendRange(dst []byte, typeOID uint32, info typeInfo, text []byte) ([]byte, error) {
	out, n, err := appendRangeAt(dst, typeOID, info.elem, text, text)
	if err != nil {
		return dst, err
	}
	if n != len(text) {
		return dst, newValueError(typeOID, text, "trailing characters after range")
	}
	return out, nil
}

// appendMultirange converts multirange output ("{[1,3),[5,7)}", "{}") to a
// JSON array of range objects.
func appendMultirange(dst []byte, typeOID uint32, info typeInfo, text []byte) ([]byte, error) {
	bound, _ := ElementType(info.elem)
	s := bytes.TrimSpace(text)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return dst, newValueError(typeOID, text, "expected '{...}'")
	}
	s = bytes.TrimSpace(s[1 : len(s)-1])
	start := len(dst)
	dst = append(dst, '[')
	for first := true; len(s) > 0; first = false {
		if !first {
			if s[0] != ',' {
				return dst[:start], newValueError(typeOID, text, "expected ',' between ranges")
			}
			s = bytes.TrimLeft(s[1:], " ")
			dst = append(dst, ',')
		}
		var (
			n   int
			err error
		)
		if dst, n, err = appendRangeAt(dst, typeOID, bound, s, text); err != nil {
			return dst[:start], err
		}
		s = bytes.TrimLeft(s[n:], " ")
	}
	return append(dst, ']'), nil
}

// appendRangeAt converts the range at the start of s and returns the number
// of bytes consumed. text is the whole value, for error messages.
func appendRangeAt(dst []byte, typeOID, bound uint32, s, text []byte) ([]byte, int, error) {
	const empty = "empty"
	if len(s) >= len(empty) && bytes.EqualFold(s[:len(empty)], []byte(empty)) {
		return append(dst, `{"empty":true}`...), len(empty), nil
	}
	if len(s) == 0 || (s[0] != '[' && s[0] != '(') {
		return dst, 0, newValueError(typeOID, text, "expected '[' or '('")
	}
	lowerInc := s[0] == '['
	lower, pos, ok := scanBound(s, 1)
	if !ok || pos >= len(s) || s[pos] != ',' {
		return dst, 0, newValueError(typeOID, text, "malformed lower bound")
	}
	upper, pos, ok := scanBound(s, pos+1)
	if !ok || pos >= len(s) || (s[pos] != ']' && s[pos] != ')') {
		return dst, 0, newValueError(typeOID, text, "malformed upper bound")
	}
	upperInc := s[pos] == ']'
	start := len(dst)
	dst, err := appendBound(append(dst, `{"lower":`...), bound, lower)
	if err != nil {
		return dst[:start], 0, elementError(typeOID, text, err)
	}
	if dst, err = appendBound(append(dst, `,"upper":`...), bound, upper); err != nil {
		return dst[:start], 0, elementError(typeOID, text, err)
	}
	dst = appendBoolField(append(dst, `,"lower_inc":`...), lowerInc)
	dst = appendBoolField(append(dst, `,"upper_inc":`...), upperInc)
	return append(dst, '}'), pos + 1, nil
}

func appendBound(dst []byte, typeOID uint32, b rangeBound) ([]byte, error) {
	if b.infinite {
		return append(dst, "null"...), nil
	}
	return appendElement(dst, typeOID, b.value)
}

func appendBoolField(dst []byte, v bool) []byte {
	if v {
		return append(dst, "true"...)
	}
	return append(dst, "false"...)
}

// scanBound reads a bound starting at s[pos] up to an unquoted ',', ')' or
// ']' and returns the position of that terminator. It returns a sub-slice of
// s when the bound has no quotes or escapes.
func scanBound(s []byte, pos int) (rangeBound, int, bool) {
	if pos < len(s) && isBoundEnd(s[pos]) {
		return rangeBound{infinite: true}, pos, true
	}
	for end := pos; end < len(s); end++ {
		switch c := s[end]; {
		case isBoundEnd(c):
			return rangeBound{value: s[pos:end]}, end, true
		case c == '"' || c == '\\':
			return unescapeBound(s, pos)
		}
	}
	return rangeBound{}, len(s), false
}

// unescapeBound is the slow path of scanBound: inside double quotes "" is a
// literal quote, and a backslash escapes the next byte everywhere
// (range_parse_bound).
func unescapeBound(s []byte, pos int) (rangeBound, int, bool) {
	out := make([]byte, 0, len(s)-pos)
	inQuote := false
	for ; pos < len(s); pos++ {
		c := s[pos]
		switch {
		case !inQuote && isBoundEnd(c):
			return rangeBound{value: out}, pos, true
		case c == '\\':
			pos++
			if pos >= len(s) {
				return rangeBound{}, pos, false
			}
			out = append(out, s[pos])
		case c == '"' && inQuote && pos+1 < len(s) && s[pos+1] == '"':
			pos++
			out = append(out, '"')
		case c == '"':
			inQuote = !inQuote
		default:
			out = append(out, c)
		}
	}
	return rangeBound{}, pos, false
}

func isBoundEnd(c byte) bool { return c == ',' || c == ')' || c == ']' }
