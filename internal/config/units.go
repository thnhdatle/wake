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

var dayUnit = regexp.MustCompile(`(\d+(?:\.\d*)?|\.\d+)d`)

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

type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

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

func (d *Duration) UnmarshalText(text []byte) error {
	v, err := ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error { return unmarshalScalar(n, d) }

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

const (
	KiB int64 = 1 << 10
	MiB       = KiB << 10
	GiB       = MiB << 10
	TiB       = GiB << 10
)

var (
	sizeRE    = regexp.MustCompile(`^(\d+)\s*([A-Za-z]*)$`)
	sizeUnits = []struct {
		suffix string
		mult   int64
	}{{"TiB", TiB}, {"GiB", GiB}, {"MiB", MiB}, {"KiB", KiB}}
)

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

type ByteSize int64

func (b ByteSize) Int64() int64 { return int64(b) }

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

func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := ParseByteSize(string(text))
	if err != nil {
		return err
	}
	*b = ByteSize(v)
	return nil
}

func (b ByteSize) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error { return unmarshalScalar(n, b) }

func (b ByteSize) MarshalYAML() (any, error) { return b.String(), nil }

type textUnmarshaler interface{ UnmarshalText([]byte) error }

func unmarshalScalar(n *yaml.Node, u textUnmarshaler) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: %w: expected a scalar", n.Line, ErrUnit)
	}
	if err := u.UnmarshalText([]byte(n.Value)); err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	return nil
}
