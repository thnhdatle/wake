// SPDX-License-Identifier: Apache-2.0

package change

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"unicode/utf8"
)

const hexDigits = "0123456789abcdef"

func AppendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := plainPrefix(s); i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			dst = appendEscapedByte(append(dst, s[start:i]...), b)
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			dst = append(append(dst, s[start:i]...), "\ufffd"...)
		case r == '\u2028' || r == '\u2029':
			dst = append(append(dst, s[start:i]...), '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
		default:
			i += size
			continue
		}
		i += size
		start = i
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

func AppendStringBytes(dst, b []byte) []byte {
	i := plainPrefixBytes(b)
	for i < len(b) && plainByte(b[i]) {
		i++
	}
	if i < len(b) {
		return AppendString(dst, string(b))
	}
	dst = append(dst, '"')
	dst = append(dst, b...)
	return append(dst, '"')
}

func plainPrefix(s string) int {
	i := 0
	for i+8 <= len(s) {
		v := uint64(s[i]) | uint64(s[i+1])<<8 | uint64(s[i+2])<<16 | uint64(s[i+3])<<24 |
			uint64(s[i+4])<<32 | uint64(s[i+5])<<40 | uint64(s[i+6])<<48 | uint64(s[i+7])<<56
		if !plainWord(v) {
			break
		}
		i += 8
	}
	return i
}

func plainPrefixBytes(b []byte) int {
	i := 0
	for i+8 <= len(b) && plainWord(binary.LittleEndian.Uint64(b[i:])) {
		i += 8
	}
	return i
}

func plainByte(b byte) bool { return b >= 0x20 && b < utf8.RuneSelf && b != '"' && b != '\\' }

const (
	swarOnes = 0x0101010101010101
	swarHigh = 0x8080808080808080
)

func plainWord(v uint64) bool {
	quote := v ^ (swarOnes * '"')
	backslash := v ^ (swarOnes * '\\')
	return (v|(v-swarOnes*0x20)|(quote-swarOnes)|(backslash-swarOnes))&swarHigh == 0
}

func appendEscapedByte(dst []byte, b byte) []byte {
	switch b {
	case '\\', '"':
		return append(dst, '\\', b)
	case '\n':
		return append(dst, '\\', 'n')
	case '\r':
		return append(dst, '\\', 'r')
	case '\t':
		return append(dst, '\\', 't')
	case '\b':
		return append(dst, '\\', 'b')
	case '\f':
		return append(dst, '\\', 'f')
	default:
		return append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
	}
}

func AppendCanonicalKey(dst []byte, key Row) []byte {
	if key == nil {
		return append(dst, "null"...)
	}
	var stack [8]int
	order := stack[:0]
	for i := range key {
		order = append(order, i)
	}
	if !slices.IsSortedFunc(order, func(a, b int) int { return compareNames(key[a].Name, key[b].Name) }) {
		slices.SortFunc(order, func(a, b int) int { return compareNames(key[a].Name, key[b].Name) })
	}
	dst = append(dst, '{')
	for j, i := range order {
		if j > 0 {
			dst = append(dst, ',')
		}
		dst = AppendString(dst, key[i].Name)
		dst = append(dst, ':')
		dst = appendCanonicalValue(dst, key[i].Value)
	}
	return append(dst, '}')
}

func CanonicalValue(raw []byte) ([]byte, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidJSON, raw)
	}
	return appendCanonicalValue(nil, raw), nil
}

func compareNames(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func appendCanonicalValue(dst, raw []byte) []byte {
	if isCanonicalScalar(raw) {
		return append(dst, raw...)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return append(dst, raw...)
	}
	return appendCanonicalAny(dst, v)
}

func isCanonicalScalar(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	switch c := raw[0]; {
	case c == '"':
		return bytes.IndexByte(raw, '\\') < 0 && !bytes.Contains(raw, []byte(" ")) &&
			!bytes.Contains(raw, []byte(" ")) && utf8.Valid(raw)
	case c == '-' || (c >= '0' && c <= '9'):
		return !bytes.ContainsAny(raw, " \t\r\n")
	default:
		s := string(raw)
		return s == "true" || s == "false" || s == "null"
	}
}

func appendCanonicalAny(dst []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...)
	case bool:
		return strconv.AppendBool(dst, x)
	case json.Number:
		return append(dst, x...)
	case string:
		return AppendString(dst, x)
	case []any:
		dst = append(dst, '[')
		for i, e := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendCanonicalAny(dst, e)
		}
		return append(dst, ']')
	case map[string]any:
		names := make([]string, 0, len(x))
		for k := range x {
			names = append(names, k)
		}
		slices.Sort(names)
		dst = append(dst, '{')
		for i, k := range names {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = AppendString(dst, k)
			dst = append(dst, ':')
			dst = appendCanonicalAny(dst, x[k])
		}
		return append(dst, '}')
	default:
		return append(dst, "null"...)
	}
}

func appendRow(dst []byte, r Row) []byte {
	if r == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, '{')
	for i := range r {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = AppendString(dst, r[i].Name)
		dst = append(dst, ':')
		dst = append(dst, r[i].Value...)
	}
	return append(dst, '}')
}

func appendSource(dst []byte, s Source) []byte {
	dst = append(dst, `{"db":`...)
	dst = AppendString(dst, s.DB)
	dst = append(dst, `,"schema":`...)
	dst = AppendString(dst, s.Schema)
	dst = append(dst, `,"table":`...)
	dst = AppendString(dst, s.Table)
	return append(dst, '}')
}

// AppendJSON appends the event as JSON.
func (e *Event) AppendJSON(dst []byte) []byte {
	dst = append(dst, `{"id":"`...)
	dst = e.ID.AppendText(dst)
	dst = append(dst, `","source":`...)
	dst = appendSource(dst, e.Source)
	dst = append(dst, `,"op":"`...)
	dst = append(dst, e.Op...)
	dst = append(dst, `","ts_ms":`...)
	dst = strconv.AppendInt(dst, e.TsMs, 10)
	dst = append(dst, `,"lsn":"`...)
	dst = e.ID.LSN.AppendText(dst)
	dst = append(dst, '"')
	if e.TxID != 0 {
		dst = append(dst, `,"tx_id":`...)
		dst = strconv.AppendUint(dst, uint64(e.TxID), 10)
	}
	dst = append(dst, `,"key":`...)
	dst = AppendCanonicalKey(dst, e.Key)
	dst = append(dst, `,"before":`...)
	dst = appendRow(dst, e.Before)
	dst = append(dst, `,"after":`...)
	dst = appendRow(dst, e.After)
	if len(e.Unchanged) > 0 {
		dst = append(dst, `,"unchanged_columns":[`...)
		for i, c := range e.Unchanged {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = AppendString(dst, c)
		}
		dst = append(dst, ']')
	}
	dst = append(dst, `,"schema_version":`...)
	dst = strconv.AppendUint(dst, e.SchemaVersion, 10)
	return append(dst, '}')
}

func (s *SchemaChange) AppendJSON(dst []byte) []byte {
	dst = append(dst, `{"type":"schema_change","id":"`...)
	dst = s.ID.AppendText(dst)
	dst = append(dst, `","source":`...)
	dst = appendSource(dst, s.Source)
	dst = append(dst, `,"lsn":"`...)
	dst = s.ID.LSN.AppendText(dst)
	dst = append(dst, `","ts_ms":`...)
	dst = strconv.AppendInt(dst, s.TsMs, 10)
	dst = append(dst, `,"schema_version":`...)
	dst = strconv.AppendUint(dst, s.SchemaVersion, 10)
	dst = append(dst, `,"columns_before":`...)
	dst = appendColumnTypes(dst, s.ColumnsBefore)
	dst = append(dst, `,"columns_after":`...)
	if s.ColumnsAfter == nil {
		dst = append(dst, "[]"...)
	} else {
		dst = appendColumnTypes(dst, s.ColumnsAfter)
	}
	return append(dst, '}')
}

func appendColumnTypes(dst []byte, cols []ColumnType) []byte {
	if cols == nil {
		return append(dst, "null"...)
	}
	dst = append(dst, '[')
	for i, c := range cols {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"name":`...)
		dst = AppendString(dst, c.Name)
		dst = append(dst, `,"type":`...)
		dst = AppendString(dst, c.Type)
		dst = append(dst, '}')
	}
	return append(dst, ']')
}

func (m *Message) AppendJSON(dst []byte) []byte {
	dst = append(dst, `{"type":"message","id":"`...)
	dst = m.ID.AppendText(dst)
	dst = append(dst, `","lsn":"`...)
	dst = m.ID.LSN.AppendText(dst)
	dst = append(dst, `","ts_ms":`...)
	dst = strconv.AppendInt(dst, m.TsMs, 10)
	dst = append(dst, `,"transactional":`...)
	dst = strconv.AppendBool(dst, m.Transactional)
	dst = append(dst, `,"prefix":`...)
	dst = AppendString(dst, m.Prefix)
	dst = append(dst, `,"content_base64":"`...)
	dst = AppendBase64(dst, m.Content)
	return append(dst, `"}`...)
}

func AppendBase64(dst, b []byte) []byte {
	return base64.StdEncoding.AppendEncode(dst, b)
}
