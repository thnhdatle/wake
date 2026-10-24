// SPDX-License-Identifier: Apache-2.0

package change

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type IDKind uint8

const (
	IDKindStream IDKind = iota
	IDKindSnapshot
	IDKindSchema
	IDKindMessage
)

// ID identifies a change event.
type ID struct {
	LSN  lsn.LSN
	Kind IDKind
	Run  uint64
	N    uint64
}

func StreamID(commit lsn.LSN, k uint64) ID {
	return ID{LSN: commit, Kind: IDKindStream, N: k}
}

func SnapshotID(at lsn.LSN, run, n uint64) ID {
	return ID{LSN: at, Kind: IDKindSnapshot, Run: run, N: n}
}

func SchemaID(commit lsn.LSN, k uint64) ID {
	return ID{LSN: commit, Kind: IDKindSchema, N: k}
}

func MessageID(at lsn.LSN, n uint64) ID {
	return ID{LSN: at, Kind: IDKindMessage, N: n}
}

func (id ID) Seq() Seq {
	return Seq{LSN: id.LSN, K: id.N, Schema: id.Kind == IDKindSchema}
}

func (id ID) String() string {
	var buf [64]byte
	return string(id.AppendText(buf[:0]))
}

func (id ID) AppendText(dst []byte) []byte {
	dst = id.LSN.AppendText(dst)
	dst = append(dst, '-')
	switch id.Kind {
	case IDKindStream:
	case IDKindSnapshot:
		dst = append(dst, 'r')
		dst = strconv.AppendUint(dst, id.Run, 10)
		dst = append(dst, '-')
	case IDKindSchema:
		dst = append(dst, 's')
	case IDKindMessage:
		dst = append(dst, 'm')
	}
	return strconv.AppendUint(dst, id.N, 10)
}

// ParseID parses the text form of an ID.
func ParseID(s string) (ID, error) {
	at, rest, ok := strings.Cut(s, "-")
	if !ok || rest == "" {
		return ID{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
	}
	l, err := lsn.Parse(at)
	if err != nil || strings.ToUpper(at) != at {
		return ID{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
	}
	id := ID{LSN: l}
	switch rest[0] {
	case 'r':
		run, n, ok := strings.Cut(rest[1:], "-")
		if !ok {
			return ID{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
		}
		id.Kind = IDKindSnapshot
		if id.Run, err = parseDecimal(run); err != nil {
			return ID{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
		}
		rest = "-" + n
	case 's':
		id.Kind = IDKindSchema
	case 'm':
		id.Kind = IDKindMessage
	default:
		rest = "-" + rest
	}
	if id.N, err = parseDecimal(rest[1:]); err != nil {
		return ID{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
	}
	return id, nil
}

func parseDecimal(s string) (uint64, error) {
	if s == "" || s[0] < '0' || s[0] > '9' {
		return 0, ErrInvalidID
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, ErrInvalidID
	}
	return v, nil
}
