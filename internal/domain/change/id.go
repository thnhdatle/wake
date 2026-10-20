// SPDX-License-Identifier: Apache-2.0

package change

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// IDKind tells which CDC event id form an ID uses.
type IDKind uint8

const (
	// IDKindStream is a stream event id: <commit_lsn>-<index_in_tx>.
	IDKindStream IDKind = iota
	// IDKindSnapshot is a snapshot read id: <lsn>-r<run_id>-<n>.
	IDKindSnapshot
	// IDKindSchema is a schema-change id: <commit_lsn>-s<index_in_tx>.
	IDKindSchema
	// IDKindMessage is a forwarded logical message id: <lsn>-m<n> (outside the CDC event contract).
	IDKindMessage
)

// ID identifies one emitted record. N is index_in_tx for stream and schema
// ids, the snapshot counter n for snapshot ids and the message counter for
// message ids. Run is the snapshot run id and is zero otherwise.
type ID struct {
	LSN  lsn.LSN
	Kind IDKind
	Run  uint64
	N    uint64
}

// StreamID returns the id of the k-th captured event of a transaction.
func StreamID(commit lsn.LSN, k uint64) ID {
	return ID{LSN: commit, Kind: IDKindStream, N: k}
}

// SnapshotID returns the id of the n-th row of snapshot run run.
func SnapshotID(at lsn.LSN, run, n uint64) ID {
	return ID{LSN: at, Kind: IDKindSnapshot, Run: run, N: n}
}

// SchemaID returns the id of the schema-change event emitted before the k-th
// captured event of a transaction.
func SchemaID(commit lsn.LSN, k uint64) ID {
	return ID{LSN: commit, Kind: IDKindSchema, N: k}
}

// MessageID returns the id of the n-th forwarded logical message.
func MessageID(at lsn.LSN, n uint64) ID {
	return ID{LSN: at, Kind: IDKindMessage, N: n}
}

// Seq returns the record's position in the CDC event total order.
func (id ID) Seq() Seq {
	return Seq{LSN: id.LSN, K: id.N, Schema: id.Kind == IDKindSchema}
}

// String formats the id per its grammar.
func (id ID) String() string {
	var buf [64]byte
	return string(id.AppendText(buf[:0]))
}

// AppendText appends the textual id to dst.
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

// ParseID parses any of the four id forms.
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
