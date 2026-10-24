// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"bytes"
	"time"

	"github.com/thnhdatle/wake/internal/domain/change"
)

const (
	secondsPerMinute = 60
	secondsPerHour   = 3600
	secondsPerDay    = 86400
	maxYearDigits    = 9
)

type civilDate struct {
	year, month, day int
}

type clockTime struct {
	hour, minute, second int
	frac                 []byte
}

type utcOffset int

func appendDateTime(dst []byte, typeOID uint32, kind valueKind, text []byte) ([]byte, error) {
	switch string(text) {
	case "infinity", "-infinity":
		return change.AppendStringBytes(dst, text), nil
	}
	s, bc := bytes.CutSuffix(text, []byte(" BC"))
	d, rest, ok := parseDate(s, bc)
	if !ok {
		return dst, newValueError(typeOID, text, "invalid date")
	}
	if kind == kindDate {
		if len(rest) != 0 {
			return dst, newValueError(typeOID, text, "trailing characters after date")
		}
		return append(appendDate(append(dst, '"'), d), '"'), nil
	}
	if len(rest) == 0 || rest[0] != ' ' {
		return dst, newValueError(typeOID, text, "missing time of day")
	}
	c, rest, ok := parseClock(rest[1:])
	if !ok {
		return dst, newValueError(typeOID, text, "invalid time of day")
	}
	if kind == kindTimestamp {
		if len(rest) != 0 {
			return dst, newValueError(typeOID, text, "trailing characters after time")
		}
		return appendTimestamp(dst, d, c, nil), nil
	}
	off, whole, ok := parseOffset(rest)
	if !ok {
		return dst, newValueError(typeOID, text, "invalid utc offset")
	}
	if !whole {
		d, c = toUTC(d, c, off)
		off = 0
	}
	return appendTimestamp(dst, d, c, &off), nil
}

func appendTimeOfDay(dst []byte, typeOID uint32, withZone bool, text []byte) ([]byte, error) {
	c, rest, ok := parseClock(text)
	if !ok {
		return dst, newValueError(typeOID, text, "invalid time of day")
	}
	if !withZone {
		if len(rest) != 0 {
			return dst, newValueError(typeOID, text, "trailing characters after time")
		}
		return append(appendClock(append(dst, '"'), c), '"'), nil
	}
	off, whole, ok := parseOffset(rest)
	if !ok {
		return dst, newValueError(typeOID, text, "invalid utc offset")
	}
	if !whole {
		secs := c.hour*secondsPerHour + c.minute*secondsPerMinute + c.second - int(off)
		secs = ((secs % secondsPerDay) + secondsPerDay) % secondsPerDay
		c.hour, c.minute, c.second = secs/secondsPerHour, secs%secondsPerHour/secondsPerMinute, secs%secondsPerMinute
		off = 0
	}
	out := appendClock(append(dst, '"'), c)
	return append(appendOffset(out, off), '"'), nil
}

func appendTimestamp(dst []byte, d civilDate, c clockTime, off *utcOffset) []byte {
	dst = appendDate(append(dst, '"'), d)
	dst = appendClock(append(dst, 'T'), c)
	if off != nil {
		dst = appendOffset(dst, *off)
	}
	return append(dst, '"')
}

func toUTC(d civilDate, c clockTime, off utcOffset) (civilDate, clockTime) {
	t := time.Date(d.year, time.Month(d.month), d.day, c.hour, c.minute, c.second, 0,
		time.FixedZone("", int(off))).UTC()
	return civilDate{year: t.Year(), month: int(t.Month()), day: t.Day()},
		clockTime{hour: t.Hour(), minute: t.Minute(), second: t.Second(), frac: c.frac}
}

func parseDate(s []byte, bc bool) (civilDate, []byte, bool) {
	n := 0
	for n < len(s) && n <= maxYearDigits && isDigit(s[n]) {
		n++
	}
	if n < 4 || n > maxYearDigits || len(s) < n+6 || s[n] != '-' || s[n+3] != '-' {
		return civilDate{}, nil, false
	}
	d := civilDate{year: atoi(s[:n])}
	var ok1, ok2 bool
	d.month, ok1 = twoDigits(s[n+1:])
	d.day, ok2 = twoDigits(s[n+4:])
	if !ok1 || !ok2 || !d.valid() {
		return civilDate{}, nil, false
	}
	if bc {
		d.year = 1 - d.year
	}
	return d, s[n+6:], true
}

func (d civilDate) valid() bool {
	return d.year > 0 && d.month >= 1 && d.month <= 12 && d.day >= 1 && d.day <= 31
}

func parseClock(s []byte) (clockTime, []byte, bool) {
	if len(s) < 8 || s[2] != ':' || s[5] != ':' {
		return clockTime{}, nil, false
	}
	var c clockTime
	var ok1, ok2, ok3 bool
	c.hour, ok1 = twoDigits(s)
	c.minute, ok2 = twoDigits(s[3:])
	c.second, ok3 = twoDigits(s[6:])
	if !ok1 || !ok2 || !ok3 || c.hour > 24 || c.minute > 59 || c.second > 60 {
		return clockTime{}, nil, false
	}
	frac, rest, ok := scanFraction(s[8:])
	c.frac = frac
	return c, rest, ok
}

func scanFraction(s []byte) (frac, rest []byte, ok bool) {
	if len(s) == 0 || s[0] != '.' {
		return nil, s, true
	}
	n := 1
	for n < len(s) && isDigit(s[n]) {
		n++
	}
	if n == 1 {
		return nil, nil, false
	}
	return s[:n], s[n:], true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func parseOffset(s []byte) (off utcOffset, whole, ok bool) {
	if len(s) < 3 || (s[0] != '+' && s[0] != '-') {
		return 0, false, false
	}
	sign := 1
	if s[0] == '-' {
		sign = -1
	}
	parts := [3]int{}
	rest := s[1:]
	for i := range parts {
		v, ok := twoDigits(rest)
		if !ok || (i > 0 && v > 59) {
			return 0, false, false
		}
		parts[i] = v
		rest = rest[2:]
		if len(rest) == 0 {
			secs := parts[0]*secondsPerHour + parts[1]*secondsPerMinute + parts[2]
			return utcOffset(sign * secs), parts[2] == 0, true
		}
		if rest[0] != ':' || i == len(parts)-1 {
			return 0, false, false
		}
		rest = rest[1:]
	}
	return 0, false, false
}

func appendDate(dst []byte, d civilDate) []byte {
	year := d.year
	if year < 0 {
		dst = append(dst, '-')
		year = -year
	}
	for p := 1000; p > 1 && year < p; p /= 10 {
		dst = append(dst, '0')
	}
	dst = appendUint(dst, uint64(year)) //nolint:gosec // G115: year is non-negative here
	dst = append2(append(dst, '-'), d.month)
	return append2(append(dst, '-'), d.day)
}

func appendClock(dst []byte, c clockTime) []byte {
	dst = append2(dst, c.hour)
	dst = append2(append(dst, ':'), c.minute)
	dst = append2(append(dst, ':'), c.second)
	return append(dst, c.frac...)
}

func appendOffset(dst []byte, off utcOffset) []byte {
	if off < 0 {
		dst = append(dst, '-')
		off = -off
	} else {
		dst = append(dst, '+')
	}
	dst = append2(dst, int(off)/secondsPerHour)
	return append2(append(dst, ':'), int(off)%secondsPerHour/secondsPerMinute)
}

const decimalDigits = "0123456789"

func append2(dst []byte, v int) []byte {
	return append(dst, decimalDigits[v/10%10], decimalDigits[v%10])
}

func appendUint(dst []byte, v uint64) []byte {
	var buf [20]byte
	i := len(buf)
	for {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
		if v == 0 {
			break
		}
	}
	return append(dst, buf[i:]...)
}

func twoDigits(s []byte) (int, bool) {
	if len(s) < 2 || s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' {
		return 0, false
	}
	return int(s[0]-'0')*10 + int(s[1]-'0'), true
}

func atoi(s []byte) int {
	v := 0
	for _, c := range s {
		v = v*10 + int(c-'0')
	}
	return v
}
