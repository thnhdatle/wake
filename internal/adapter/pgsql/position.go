// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Chunk positions (pipeline.ChunkRequest.After) are opaque to the
// application and persisted in the state file:
//
//   - keyset tables: a JSON array of the pagination key values of the last
//     row in their Postgres text form, e.g. ["42","2026-10-18 12:00:00+00"];
//     the next chunk reads rows whose key is greater, comparing in the
//     column types.
//   - ctid tables: {"rel":<leaf table oid>,"ctid":"(<page>,<item>)"}; the
//     next chunk reads rows of that leaf after the ctid, then of the leaves
//     with higher OIDs (one leaf for a plain table).

// encodeKeyPosition encodes the key values of a row.
func encodeKeyPosition(values []string) (json.RawMessage, error) {
	b, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("pgsql: encode position: %w", err)
	}
	return b, nil
}

// decodeKeyPosition decodes a keyset position with n values.
func decodeKeyPosition(raw json.RawMessage, n int) ([]string, error) {
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil || len(values) != n {
		return nil, fmt.Errorf("%w: want %d key values, got %s", ErrBadPosition, n, raw)
	}
	return values, nil
}

// tid is a tuple identifier.
type tid struct {
	page uint32
	item uint16
}

func (t tid) String() string {
	return "(" + strconv.FormatUint(uint64(t.page), 10) + "," + strconv.FormatUint(uint64(t.item), 10) + ")"
}

// parseTID parses the text form "(page,item)".
func parseTID(s string) (tid, error) {
	inner, okPrefix := strings.CutPrefix(s, "(")
	inner, okSuffix := strings.CutSuffix(inner, ")")
	if !okPrefix || !okSuffix {
		return tid{}, fmt.Errorf("%w: ctid %q", ErrBadPosition, s)
	}
	pageText, itemText, ok := strings.Cut(inner, ",")
	if !ok {
		return tid{}, fmt.Errorf("%w: ctid %q", ErrBadPosition, s)
	}
	page, err1 := strconv.ParseUint(pageText, 10, 32)
	item, err2 := strconv.ParseUint(itemText, 10, 16)
	if err1 != nil || err2 != nil {
		return tid{}, fmt.Errorf("%w: ctid %q", ErrBadPosition, s)
	}
	return tid{page: uint32(page), item: uint16(item)}, nil
}

// ctidPosition is the position of a ctid table.
type ctidPosition struct {
	Rel  uint32 `json:"rel"`
	CTID string `json:"ctid"`
}

func encodeCTIDPosition(rel uint32, t tid) (json.RawMessage, error) {
	b, err := json.Marshal(ctidPosition{Rel: rel, CTID: t.String()})
	if err != nil {
		return nil, fmt.Errorf("pgsql: encode position: %w", err)
	}
	return b, nil
}

func decodeCTIDPosition(raw json.RawMessage) (uint32, tid, error) {
	var p ctidPosition
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return 0, tid{}, fmt.Errorf("%w: %s", ErrBadPosition, raw)
	}
	t, err := parseTID(p.CTID)
	if err != nil {
		return 0, tid{}, err
	}
	return p.Rel, t, nil
}
