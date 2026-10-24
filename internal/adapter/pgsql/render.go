// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

type renderer struct {
	table  string
	cols   []column
	offset int
	keyIdx []int
}

func newRenderer(m *tableMeta, offset int) (*renderer, error) {
	r := &renderer{table: m.schema + "." + m.name, cols: m.cols, offset: offset}
	for _, name := range m.eventKey() {
		i := columnIndex(m.cols, name)
		if i < 0 {
			return nil, fmt.Errorf("%w: key column %q of %s is not published (generated?)", ErrInvalidArgument, name,
				r.table)
		}
		r.keyIdx = append(r.keyIdx, i)
	}
	return r, nil
}

func columnIndex(cols []column, name string) int {
	for i := range cols {
		if cols[i].name == name {
			return i
		}
	}
	return -1
}

func (r *renderer) row(values [][]byte) (pipeline.ChunkRow, error) {
	if len(values) != r.offset+len(r.cols) {
		return pipeline.ChunkRow{}, fmt.Errorf("pgsql: %s: row has %d values, want %d", r.table, len(values),
			r.offset+len(r.cols))
	}
	values = values[r.offset:]
	size := 0
	for _, v := range values {
		size += len(v) + 8
	}
	buf := make([]byte, 0, size)
	ends := make([]int, len(values))
	for i, v := range values {
		if v == nil {
			buf = append(buf, "null"...)
		} else {
			var err error
			if buf, err = schema.AppendJSON(buf, r.cols[i].typeOID, v); err != nil {
				return pipeline.ChunkRow{}, fmt.Errorf("%w: %s.%s: %w", ErrValue, r.table, r.cols[i].name, err)
			}
		}
		ends[i] = len(buf)
	}
	after := make(change.Row, len(values))
	start := 0
	for i := range values {
		after[i] = change.Column{Name: r.cols[i].name, Value: buf[start:ends[i]:ends[i]]}
		start = ends[i]
	}
	out := pipeline.ChunkRow{After: after}
	if r.keyIdx != nil {
		out.Key = make(change.Row, len(r.keyIdx))
		for i, idx := range r.keyIdx {
			out.Key[i] = after[idx]
		}
	}
	return out, nil
}
