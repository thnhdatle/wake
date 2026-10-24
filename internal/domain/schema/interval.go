// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"bytes"
	"strconv"

	"github.com/thnhdatle/wake/internal/domain/change"
)

const maxIntervalDigits = 18

type intervalParts struct {
	year, month, day int64
	hour             int64
	minute, second   int
	frac             []byte
	negTime          bool
	seen             [4]bool
}

func appendInterval(dst []byte, typeOID uint32, text []byte) ([]byte, error) {
	switch string(text) {
	case "infinity", "-infinity":
		return change.AppendStringBytes(dst, text), nil
	}
	if len(text) > 0 && text[0] == 'P' {
		if !isISODuration(text) {
			return dst, newValueError(typeOID, text, "invalid iso 8601 duration")
		}
		return change.AppendStringBytes(dst, text), nil
	}
	p, ok := parseInterval(text)
	if !ok {
		return dst, newValueError(typeOID, text, "invalid interval (IntervalStyle must be postgres)")
	}
	return append(p.appendISO(append(dst, '"')), '"'), nil
}

func parseInterval(s []byte) (intervalParts, bool) {
	var p intervalParts
	for len(s) > 0 {
		tok, rest := nextToken(s)
		if bytes.IndexByte(tok, ':') >= 0 {
			if p.seen[3] || !p.parseTime(tok) {
				return p, false
			}
			p.seen[3] = true
			s = rest
			continue
		}
		v, ok := parseSigned(tok)
		unit, after := nextToken(rest)
		if !ok || !p.setField(unit, v) {
			return p, false
		}
		s = after
	}
	return p, p.seen != [4]bool{}
}

func nextToken(s []byte) (tok, rest []byte) {
	if i := bytes.IndexByte(s, ' '); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, nil
}

func (p *intervalParts) setField(unit []byte, v int64) bool {
	var idx int
	switch string(unit) {
	case "year", "years":
		idx, p.year = 0, v
	case "mon", "mons":
		idx, p.month = 1, v
	case "day", "days":
		idx, p.day = 2, v
	default:
		return false
	}
	if p.seen[idx] {
		return false
	}
	p.seen[idx] = true
	return true
}

func (p *intervalParts) parseTime(tok []byte) bool {
	if len(tok) > 0 && (tok[0] == '+' || tok[0] == '-') {
		p.negTime = tok[0] == '-'
		tok = tok[1:]
	}
	colon := bytes.IndexByte(tok, ':')
	if colon < 1 || colon > maxIntervalDigits || !allDigits(tok[:colon]) {
		return false
	}
	p.hour, _ = strconv.ParseInt(string(tok[:colon]), 10, 64)
	c, rest, ok := parseClock(append([]byte("00"), tok[colon:]...))
	if !ok || len(rest) != 0 || c.minute > 59 || c.second > 59 {
		return false
	}
	p.minute, p.second = c.minute, c.second
	if len(c.frac) > 0 {
		p.frac = bytes.TrimRight(c.frac[1:], "0")
	}
	return true
}

func parseSigned(tok []byte) (int64, bool) {
	digits := tok
	if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
		digits = digits[1:]
	}
	if len(digits) == 0 || len(digits) > maxIntervalDigits || !allDigits(digits) {
		return 0, false
	}
	v, err := strconv.ParseInt(string(digits), 10, 64)
	if err != nil {
		return 0, false
	}
	if tok[0] == '-' {
		v = -v
	}
	return v, true
}

func (p *intervalParts) appendISO(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, 'P')
	dst = appendField(dst, p.year, 'Y')
	dst = appendField(dst, p.month, 'M')
	dst = appendField(dst, p.day, 'D')
	if p.hour == 0 && p.minute == 0 && p.second == 0 && len(p.frac) == 0 {
		if len(dst) == start+1 {
			dst = append(dst, "T0S"...)
		}
		return dst
	}
	sign := int64(1)
	if p.negTime {
		sign = -1
	}
	dst = append(dst, 'T')
	dst = appendField(dst, sign*p.hour, 'H')
	dst = appendField(dst, sign*int64(p.minute), 'M')
	if p.second == 0 && len(p.frac) == 0 {
		return dst
	}
	if p.negTime {
		dst = append(dst, '-')
	}
	dst = strconv.AppendInt(dst, int64(p.second), 10)
	if len(p.frac) > 0 {
		dst = append(append(dst, '.'), p.frac...)
	}
	return append(dst, 'S')
}

func appendField(dst []byte, v int64, designator byte) []byte {
	if v == 0 {
		return dst
	}
	return append(strconv.AppendInt(dst, v, 10), designator)
}

func isISODuration(s []byte) bool {
	if len(s) < 3 || s[0] != 'P' {
		return false
	}
	last := s[len(s)-1]
	if last < 'A' || last > 'Z' {
		return false
	}
	for _, c := range s[1:] {
		switch {
		case c >= '0' && c <= '9', c == '-', c == '.':
		case c == 'Y', c == 'M', c == 'D', c == 'T', c == 'H', c == 'S', c == 'W':
		default:
			return false
		}
	}
	return true
}
