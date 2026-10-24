// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

type RowImages struct {
	Op        change.Op
	Key       change.Row
	Before    change.Row
	After     change.Row
	Unchanged []string
}

// RowImages returns the before and after rows of a change message.
func (d *Decoder) RowImages(m Message) (RowImages, error) {
	var (
		oid    uint32
		tuples []Tuple
		build  func(b *imageBuilder) (RowImages, error)
	)
	switch msg := m.(type) {
	case *Insert:
		oid, tuples = msg.RelationOID, []Tuple{msg.New}
		build = func(b *imageBuilder) (RowImages, error) { return b.insert(msg) }
	case *Update:
		oid, tuples = msg.RelationOID, []Tuple{msg.New, msg.Old}
		build = func(b *imageBuilder) (RowImages, error) { return b.update(msg) }
	case *Delete:
		oid, tuples = msg.RelationOID, []Tuple{msg.Old}
		build = func(b *imageBuilder) (RowImages, error) { return b.delete(msg) }
	default:
		return RowImages{}, ErrNotRowMessage
	}
	e, err := d.entry(oid)
	if err != nil {
		return RowImages{}, err
	}
	b := newImageBuilder(e, tuples, d.cells)
	img, err := build(b)
	clear(b.scratch)
	d.cells = b.scratch[:0]
	return img, err
}

type cell struct {
	value     []byte
	unchanged bool
}

type imageBuilder struct {
	e       *relationEntry
	buf     []byte
	cols    []change.Column
	scratch []cell
}

func newImageBuilder(e *relationEntry, tuples []Tuple, scratch []cell) *imageBuilder {
	size := 0
	for _, t := range tuples {
		for i := range t {
			size += len(t[i].Data) + 8
		}
	}
	return &imageBuilder{e: e, buf: make([]byte, 0, size+size/4), scratch: scratch[:0]}
}

func (b *imageBuilder) reserveRows(n int) {
	b.cols = make([]change.Column, 0, n)
}

func (b *imageBuilder) newRow(n int) change.Row {
	if n == 0 {
		return change.Row{}
	}
	if cap(b.cols)-len(b.cols) < n {
		b.cols = make([]change.Column, 0, n)
	}
	start := len(b.cols)
	b.cols = b.cols[:start+n]
	return b.cols[start : start : start+n]
}

func (b *imageBuilder) newCells(n int) []cell {
	start := len(b.scratch)
	if cap(b.scratch)-start < n {
		grown := make([]cell, start, 2*(start+n))
		copy(grown, b.scratch)
		b.scratch = grown
	}
	b.scratch = b.scratch[:start+n]
	out := b.scratch[start : start+n : start+n]
	clear(out)
	return out
}

func (b *imageBuilder) keyLen() int {
	if b.e.keyless() {
		return 0
	}
	return len(b.e.key)
}

func (b *imageBuilder) beforeLen(k OldKind) int {
	if k == OldFull {
		return len(b.e.rel.Columns)
	}
	return len(b.e.identity)
}

func (b *imageBuilder) columnError(sentinel error, col int) error {
	return fmt.Errorf("%w: %s column %q", sentinel, b.e.rel.QualifiedName(), b.e.rel.Columns[col].Name)
}

func (b *imageBuilder) cells(t Tuple) ([]cell, error) {
	cols := b.e.rel.Columns
	if len(t) != len(cols) {
		return nil, fmt.Errorf("%w: %s has %d columns, tuple has %d",
			ErrColumnCount, b.e.rel.QualifiedName(), len(cols), len(t))
	}
	out := b.newCells(len(t))
	for i := range t {
		start := len(b.buf)
		switch t[i].Kind {
		case TupleNull:
			b.buf = append(b.buf, "null"...)
		case TupleUnchanged:
			out[i].unchanged = true
			continue
		case TupleText:
			var err error
			if b.buf, err = schema.AppendJSON(b.buf, cols[i].TypeOID, t[i].Data); err != nil {
				return nil, fmt.Errorf("pgoutput: %s column %q: %w", b.e.rel.QualifiedName(), cols[i].Name, err)
			}
		case TupleBinary:
			return nil, b.columnError(ErrBinaryValue, i)
		default:
			return nil, fmt.Errorf("%w: tuple column kind %q", ErrMalformed, byte(t[i].Kind))
		}
		end := len(b.buf)
		out[i].value = b.buf[start:end:end]
	}
	return out, nil
}

func (b *imageBuilder) rowAll(cells []cell) change.Row {
	row := b.newRow(len(cells))
	for i := range cells {
		if !cells[i].unchanged {
			row = append(row, change.Column{Name: b.e.rel.Columns[i].Name, Value: cells[i].value})
		}
	}
	return row
}

func (b *imageBuilder) rowOf(cells []cell, idx []int) change.Row {
	row := b.newRow(len(idx))
	for _, i := range idx {
		if !cells[i].unchanged {
			row = append(row, change.Column{Name: b.e.rel.Columns[i].Name, Value: cells[i].value})
		}
	}
	return row
}

func (b *imageBuilder) required(cells, fallback []cell, idx []int) (change.Row, error) {
	row := b.newRow(len(idx))
	for _, i := range idx {
		c := cells[i]
		if c.unchanged && fallback != nil {
			c = fallback[i]
		}
		if c.unchanged {
			return nil, b.columnError(ErrUnchangedKey, i)
		}
		row = append(row, change.Column{Name: b.e.rel.Columns[i].Name, Value: c.value})
	}
	return row, nil
}

func (b *imageBuilder) key(cells, fallback []cell) (change.Row, error) {
	if b.e.keyless() {
		return nil, nil
	}
	return b.required(cells, fallback, b.e.key)
}

func (b *imageBuilder) insert(m *Insert) (RowImages, error) {
	cells, err := b.cells(m.New)
	if err != nil {
		return RowImages{}, err
	}
	b.reserveRows(len(cells) + b.keyLen())
	for i := range cells {
		if cells[i].unchanged {
			return RowImages{}, b.columnError(ErrUnexpectedUnchanged, i)
		}
	}
	img := RowImages{Op: change.OpCreate, After: b.rowAll(cells)}
	if img.Key, err = b.key(cells, nil); err != nil {
		return RowImages{}, err
	}
	return img, nil
}

func (b *imageBuilder) update(m *Update) (RowImages, error) {
	newCells, err := b.cells(m.New)
	if err != nil {
		return RowImages{}, err
	}
	var oldCells []cell
	if m.OldKind != OldNone {
		if oldCells, err = b.cells(m.Old); err != nil {
			return RowImages{}, err
		}
	}
	full := b.e.rel.Identity == schema.IdentityFull
	b.reserveRows(b.beforeLen(m.OldKind) + len(newCells) + b.keyLen())
	img := RowImages{Op: change.OpUpdate}
	switch m.OldKind {
	case OldFull:
		img.Before = b.rowAll(oldCells)
	case OldKey:
		img.Before = b.rowOf(oldCells, b.e.identity)
	case OldNone:
		if full {
			return RowImages{}, fmt.Errorf("%w: update of %s (REPLICA IDENTITY FULL)",
				ErrMissingOldTuple, b.e.rel.QualifiedName())
		}
		if img.Before, err = b.required(newCells, nil, b.e.identity); err != nil {
			return RowImages{}, err
		}
	}
	if img.Unchanged, err = b.resolveUnchanged(newCells, oldCells, full && m.OldKind == OldFull); err != nil {
		return RowImages{}, err
	}
	img.After = b.rowAll(newCells)
	if img.Key, err = b.key(newCells, oldCells); err != nil {
		return RowImages{}, err
	}
	return img, nil
}

func (b *imageBuilder) resolveUnchanged(newCells, oldCells []cell, copyFromOld bool) ([]string, error) {
	full := b.e.rel.Identity == schema.IdentityFull
	var unchanged []string
	for i := range newCells {
		switch {
		case !newCells[i].unchanged:
		case copyFromOld && !oldCells[i].unchanged:
			newCells[i] = oldCells[i]
		case full:
			return nil, b.columnError(ErrUnexpectedUnchanged, i)
		default:
			unchanged = append(unchanged, b.e.rel.Columns[i].Name)
		}
	}
	return unchanged, nil
}

func (b *imageBuilder) delete(m *Delete) (RowImages, error) {
	if m.OldKind == OldNone {
		return RowImages{}, fmt.Errorf("%w: delete from %s", ErrMissingOldTuple, b.e.rel.QualifiedName())
	}
	oldCells, err := b.cells(m.Old)
	if err != nil {
		return RowImages{}, err
	}
	b.reserveRows(b.beforeLen(m.OldKind) + b.keyLen())
	img := RowImages{Op: change.OpDelete}
	if m.OldKind == OldFull {
		img.Before = b.rowAll(oldCells)
	} else {
		img.Before = b.rowOf(oldCells, b.e.identity)
	}
	if img.Key, err = b.key(oldCells, nil); err != nil {
		return RowImages{}, err
	}
	return img, nil
}
