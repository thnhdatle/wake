// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Snapshot is a Postgres transaction snapshot.
type Snapshot struct {
	Xmin uint64
	Xmax uint64
	XIP  []uint64
}

// ParseSnapshot parses the text form of a snapshot.
func ParseSnapshot(s string) (Snapshot, error) {
	xminText, rest, ok1 := strings.Cut(s, ":")
	xmaxText, xipText, ok2 := strings.Cut(rest, ":")
	if !ok1 || !ok2 {
		return Snapshot{}, fmt.Errorf("%w: %q", ErrInvalidSnapshot, s)
	}
	xmin, err1 := parseXID(xminText)
	xmax, err2 := parseXID(xmaxText)
	if err1 != nil || err2 != nil {
		return Snapshot{}, fmt.Errorf("%w: %q", ErrInvalidSnapshot, s)
	}
	snap := Snapshot{Xmin: xmin, Xmax: xmax}
	if xipText != "" {
		for part := range strings.SplitSeq(xipText, ",") {
			x, err := parseXID(part)
			if err != nil {
				return Snapshot{}, fmt.Errorf("%w: %q", ErrInvalidSnapshot, s)
			}
			if n := len(snap.XIP); n > 0 && snap.XIP[n-1] == x {
				continue
			}
			snap.XIP = append(snap.XIP, x)
		}
	}
	if err := snap.Validate(); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %q", ErrInvalidSnapshot, s)
	}
	return snap, nil
}

func parseXID(s string) (uint64, error) {
	if s == "" || (len(s) > 1 && s[0] == '0') || s[0] < '0' || s[0] > '9' {
		return 0, ErrInvalidSnapshot
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, ErrInvalidSnapshot
	}
	return v, nil
}

func (s Snapshot) IsZero() bool {
	return s.Xmin == 0 && s.Xmax == 0 && len(s.XIP) == 0
}

func (s Snapshot) Validate() error {
	switch {
	case uint32(s.Xmin) == 0 || uint32(s.Xmax) == 0: //nolint:gosec // G115: Postgres validity looks at the low half
		return fmt.Errorf("%w: invalid xmin or xmax", ErrInvalidSnapshot)
	case s.Xmin > s.Xmax:
		return fmt.Errorf("%w: xmin after xmax", ErrInvalidSnapshot)
	case s.Xmax-s.Xmin > halfXIDSpace:
		return fmt.Errorf("%w: xid range beyond the wraparound horizon", ErrInvalidSnapshot)
	}
	for i, x := range s.XIP {
		if x < s.Xmin || x >= s.Xmax || (i > 0 && x <= s.XIP[i-1]) {
			return fmt.Errorf("%w: xip %d out of order or range", ErrInvalidSnapshot, x)
		}
	}
	return nil
}

func (s Snapshot) String() string {
	return string(s.AppendText(nil))
}

func (s Snapshot) AppendText(dst []byte) []byte {
	dst = strconv.AppendUint(dst, s.Xmin, 10)
	dst = append(dst, ':')
	dst = strconv.AppendUint(dst, s.Xmax, 10)
	dst = append(dst, ':')
	for i, x := range s.XIP {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = strconv.AppendUint(dst, x, 10)
	}
	return dst
}

func (s Snapshot) MarshalText() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s.AppendText(nil), nil
}

func (s *Snapshot) UnmarshalText(b []byte) error {
	v, err := ParseSnapshot(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

func (s Snapshot) HasXIP(xid uint32) bool {
	_, found := slices.BinarySearch(s.XIP, FullXID(xid, s.Xmax))
	return found
}

func (s Snapshot) IsVisible(xid uint32) bool {
	full := FullXID(xid, s.Xmax)
	if full >= s.Xmax {
		return false
	}
	_, found := slices.BinarySearch(s.XIP, full)
	return !found
}
