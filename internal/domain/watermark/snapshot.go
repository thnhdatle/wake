// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Snapshot is a Postgres pg_snapshot as returned by pg_current_snapshot():
// transactions with xid >= Xmax had not completed when the snapshot was
// taken, and neither had the ones listed in XIP; every other xid had. Values
// are full 64-bit xids (xid8). The zero Snapshot means "none" (a LOW
// watermark carries none).
type Snapshot struct {
	Xmin uint64
	Xmax uint64
	// XIP lists the in-progress xids in strictly increasing order, all in
	// [Xmin, Xmax).
	XIP []uint64
}

// ParseSnapshot parses the pg_snapshot text form "xmin:xmax:xip1,xip2,...".
// Like Postgres it accepts xip lists with duplicates and drops them, and it
// requires decimal numbers without signs or leading zeros, xmin and xmax with
// non-zero low 32 bits, xmin <= xmax, xmax-xmin <= 2^31 (the wraparound
// horizon), and a non-decreasing xip list inside [xmin, xmax).
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

// parseXID parses a canonical unsigned decimal.
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

// IsZero reports whether s is the zero Snapshot.
func (s Snapshot) IsZero() bool {
	return s.Xmin == 0 && s.Xmax == 0 && len(s.XIP) == 0
}

// Validate checks the rules ParseSnapshot enforces, with a strictly
// increasing XIP.
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

// String returns the pg_snapshot text form.
func (s Snapshot) String() string {
	return string(s.AppendText(nil))
}

// AppendText appends the pg_snapshot text form to dst.
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

// MarshalText implements encoding.TextMarshaler after validating s.
func (s Snapshot) MarshalText() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s.AppendText(nil), nil
}

// UnmarshalText implements encoding.TextUnmarshaler with ParseSnapshot.
func (s *Snapshot) UnmarshalText(b []byte) error {
	v, err := ParseSnapshot(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

// HasXIP reports whether xip lists the 32-bit xid, matched by low 32 bits
// (see MatchXID): the transaction was running when the snapshot was taken.
func (s Snapshot) HasXIP(xid uint32) bool {
	_, found := slices.BinarySearch(s.XIP, FullXID(xid, s.Xmax))
	return found
}

// IsVisible reports whether the effects of xid, a transaction known to have
// committed, are visible to the snapshot. Postgres' rule (XidInMVCCSnapshot)
// makes a committed xid invisible when it is listed in xip or when it is >=
// xmax. The second case matters: a transaction can write its commit record
// before another one's and still get an xid at or above the snapshot's xmax
// when it is the last to leave the proc array, so it is invisible without
// being listed in xip. The xid is widened against xmax (FullXID).
func (s Snapshot) IsVisible(xid uint32) bool {
	full := FullXID(xid, s.Xmax)
	if full >= s.Xmax {
		return false
	}
	_, found := slices.BinarySearch(s.XIP, full)
	return !found
}
