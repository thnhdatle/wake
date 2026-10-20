// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

const (
	hoursPerDay = 24
	day         = hoursPerDay * time.Hour
)

// dayUnit matches a number followed by the 'd' day suffix (24h).
var dayUnit = regexp.MustCompile(`(\d+(?:\.\d*)?|\.\d+)d`)

// ParseDuration parses a Go duration string extended with a 'd' suffix that
// means 24h: "30s", "14d", "1d12h". Negative durations are rejected.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("%w: empty duration", ErrUnit)
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("%w: duration %q is negative", ErrUnit, s)
	}
	var convErr error
	expanded := dayUnit.ReplaceAllStringFunc(s, func(m string) string {
		n, err := strconv.ParseFloat(strings.TrimSuffix(m, "d"), 64)
		if err != nil {
			convErr = err
			return m
		}
		return strconv.FormatFloat(n*hoursPerDay, 'f', -1, 64) + "h"
	})
	if convErr != nil {
		return 0, fmt.Errorf("%w: duration %q: %w", ErrUnit, s, convErr)
	}
	d, err := time.ParseDuration(expanded)
	if err != nil {
		return 0, fmt.Errorf("%w: duration %q: %w", ErrUnit, s, err)
	}
	return d, nil
}

// Duration is a time.Duration that reads Go duration strings plus a 'd'
// suffix in YAML, env vars and flags.
type Duration time.Duration

// Std returns the value as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// String renders whole days as "Nd" and everything else as a compact Go string.
func (d Duration) String() string {
	v := time.Duration(d)
	if v > 0 && v%day == 0 {
		return strconv.FormatInt(int64(v/day), 10) + "d"
	}
	s := v.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error { return unmarshalScalar(n, d) }

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// Byte size units (binary).
const (
	// KiB is 1024 bytes.
	KiB int64 = 1 << 10
	// MiB is 1024 KiB.
	MiB = KiB << 10
	// GiB is 1024 MiB.
	GiB = MiB << 10
	// TiB is 1024 GiB.
	TiB = GiB << 10
)

var (
	sizeRE    = regexp.MustCompile(`^(\d+)\s*([A-Za-z]*)$`)
	sizeUnits = []struct {
		suffix string
		mult   int64
	}{{"TiB", TiB}, {"GiB", GiB}, {"MiB", MiB}, {"KiB", KiB}}
)

// ParseByteSize parses a size in bytes with an optional binary suffix
// (B, KiB, MiB, GiB, TiB): "512", "64MiB", "1GiB".
func ParseByteSize(s string) (int64, error) {
	m := sizeRE.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("%w: size %q (want bytes with an optional KiB, MiB, GiB or TiB suffix)", ErrUnit, s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: size %q: %w", ErrUnit, s, err)
	}
	mult := int64(1)
	if m[2] != "" && m[2] != "B" {
		mult = 0
		for _, u := range sizeUnits {
			if u.suffix == m[2] {
				mult = u.mult
			}
		}
		if mult == 0 {
			return 0, fmt.Errorf("%w: size %q has unknown unit %q (use B, KiB, MiB, GiB or TiB)", ErrUnit, s, m[2])
		}
	}
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("%w: size %q overflows", ErrUnit, s)
	}
	return n * mult, nil
}

// ByteSize is a size in bytes that reads "64MiB" style strings.
type ByteSize int64

// Int64 returns the size in bytes.
func (b ByteSize) Int64() int64 { return int64(b) }

// String renders the size with the largest binary unit that divides it exactly.
func (b ByteSize) String() string {
	n := int64(b)
	if n > 0 {
		for _, u := range sizeUnits {
			if n%u.mult == 0 {
				return strconv.FormatInt(n/u.mult, 10) + u.suffix
			}
		}
	}
	return strconv.FormatInt(n, 10)
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := ParseByteSize(string(text))
	if err != nil {
		return err
	}
	*b = ByteSize(v)
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (b ByteSize) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

// UnmarshalYAML implements yaml.Unmarshaler. A bare YAML integer is bytes.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error { return unmarshalScalar(n, b) }

// MarshalYAML implements yaml.Marshaler.
func (b ByteSize) MarshalYAML() (any, error) { return b.String(), nil }

type textUnmarshaler interface{ UnmarshalText([]byte) error }

// unmarshalScalar feeds the text of a YAML scalar to u, naming the line on error.
func unmarshalScalar(n *yaml.Node, u textUnmarshaler) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: %w: expected a scalar", n.Line, ErrUnit)
	}
	if err := u.UnmarshalText([]byte(n.Value)); err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	return nil
}
