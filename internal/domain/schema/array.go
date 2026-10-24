// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"bytes"
)

const maxArrayDepth = 6

type arrayParser struct {
	s       []byte
	pos     int
	text    []byte
	typeOID uint32
	elem    uint32
	delim   byte
}

func appendArray(dst []byte, typeOID uint32, info typeInfo, text []byte) ([]byte, error) {
	s := text
	if len(s) > 0 && s[0] == '[' {
		i := bytes.IndexByte(s, '=')
		if i < 0 {
			return dst, newValueError(typeOID, text, "invalid array dimensions")
		}
		s = s[i+1:]
	}
	p := arrayParser{s: s, text: text, typeOID: typeOID, elem: info.elem, delim: info.delim}
	out, err := p.parseArray(dst, 1)
	if err != nil {
		return dst, err
	}
	p.skipSpace()
	if p.pos != len(p.s) {
		return dst, p.fail("trailing characters after array")
	}
	return out, nil
}

func (p *arrayParser) fail(reason string) error { return newValueError(p.typeOID, p.text, reason) }

func (p *arrayParser) skipSpace() {
	for p.pos < len(p.s) && isSpace(p.s[p.pos]) {
		p.pos++
	}
}

func (p *arrayParser) parseArray(dst []byte, depth int) ([]byte, error) {
	if depth > maxArrayDepth {
		return dst, p.fail("too many array dimensions")
	}
	p.skipSpace()
	if p.pos >= len(p.s) || p.s[p.pos] != '{' {
		return dst, p.fail("expected '{'")
	}
	p.pos++
	dst = append(dst, '[')
	p.skipSpace()
	if p.pos < len(p.s) && p.s[p.pos] == '}' {
		p.pos++
		return append(dst, ']'), nil
	}
	for first := true; ; first = false {
		if !first {
			dst = append(dst, ',')
		}
		p.skipSpace()
		var err error
		if p.pos < len(p.s) && p.s[p.pos] == '{' {
			dst, err = p.parseArray(dst, depth+1)
		} else {
			dst, err = p.parseElement(dst)
		}
		if err != nil {
			return dst, err
		}
		p.skipSpace()
		if p.pos >= len(p.s) {
			return dst, p.fail("unterminated array")
		}
		c := p.s[p.pos]
		p.pos++
		switch c {
		case p.delim:
		case '}':
			return append(dst, ']'), nil
		default:
			return dst, p.fail("expected delimiter or '}'")
		}
	}
}

func (p *arrayParser) parseElement(dst []byte) ([]byte, error) {
	var (
		val []byte
		ok  bool
	)
	if p.pos < len(p.s) && p.s[p.pos] == '"' {
		val, ok = p.scanQuoted()
	} else {
		var isNull bool
		val, isNull, ok = p.scanUnquoted()
		if ok && isNull {
			return append(dst, "null"...), nil
		}
	}
	if !ok {
		return dst, p.fail("malformed array element")
	}
	out, err := appendElement(dst, p.elem, val)
	if err != nil {
		return dst, elementError(p.typeOID, p.text, err)
	}
	return out, nil
}

func (p *arrayParser) scanQuoted() ([]byte, bool) {
	start := p.pos + 1
	var out []byte
	for i := start; i < len(p.s); i++ {
		switch p.s[i] {
		case '"':
			p.pos = i + 1
			if out == nil {
				return p.s[start:i], true
			}
			return out, true
		case '\\':
			if out == nil {
				out = append(make([]byte, 0, len(p.s)-start), p.s[start:i]...)
			}
			i++
			if i >= len(p.s) {
				return nil, false
			}
			out = append(out, p.s[i])
		default:
			if out != nil {
				out = append(out, p.s[i])
			}
		}
	}
	return nil, false
}

func (p *arrayParser) scanUnquoted() (val []byte, isNull, ok bool) {
	start := p.pos
	escaped := false
	end := start
	for end < len(p.s) {
		c := p.s[end]
		if c == p.delim || c == '}' {
			break
		}
		if c == '{' || c == '"' {
			return nil, false, false
		}
		if c == '\\' {
			escaped = true
			end++
		}
		end++
	}
	if end > len(p.s) || end == start {
		return nil, false, false
	}
	p.pos = end
	raw := p.s[start:end]
	if !escaped {
		raw = bytes.TrimRight(raw, " \t\n\r\v\f")
		return raw, len(raw) == 4 && bytes.EqualFold(raw, []byte("NULL")), len(raw) > 0
	}
	out := make([]byte, 0, len(raw))
	lastEscaped := 0
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' {
			i++
			out = append(out, raw[i])
			lastEscaped = len(out)
			continue
		}
		out = append(out, raw[i])
	}
	for len(out) > lastEscaped && isSpace(out[len(out)-1]) {
		out = out[:len(out)-1]
	}
	return out, false, true
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
