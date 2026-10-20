// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

// RowImages are the CDC event row images of one Insert, Update or Delete.
type RowImages struct {
	Op change.Op
	// Key holds the key columns in key order; nil for keyless tables.
	Key change.Row
	// Before is nil for inserts. For updates and deletes it holds every
	// column of an 'O' tuple (REPLICA IDENTITY FULL), otherwise only the
	// replica identity columns.
	Before change.Row
	// After is nil for deletes.
	After change.Row
	// Unchanged lists columns omitted from After because Postgres sent an
	// unchanged-TOAST marker; always empty under REPLICA IDENTITY FULL.
	Unchanged []string
}

// RowImages converts an Insert, Update or Delete to CDC event row images using the
// cached relation ("before" and TOAST rules, key rule):
//
//   - Key: the key columns (KeyColumns) of the new row for c and u, of the
//     old tuple for d. When an update's new key value is an unchanged-TOAST
//     marker, the old key tuple's value is used.
//   - Before (u, d): every column of an 'O' tuple; the replica identity
//     columns of a 'K' tuple; for an update without old tuple (key
//     unchanged) the replica identity columns of the new row. An update of
//     a REPLICA IDENTITY FULL table without 'O' tuple is ErrMissingOldTuple.
//   - After (c, u): the new row. An unchanged-TOAST column is copied from
//     the 'O' tuple under REPLICA IDENTITY FULL, otherwise omitted and
//     listed in Unchanged.
//
// Column values are compact JSON per schema.AppendJSON; they live in one
// buffer per call and do not alias the message.
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
	// The cell scratch is reused by the next call; clearing it drops the
	// references to this call's value buffer.
	clear(b.scratch)
	d.cells = b.scratch[:0]
	return img, err
}

// cell is one converted column value; unchanged marks a 'u' column.
type cell struct {
	value     []byte
	unchanged bool
}

// imageBuilder builds the row images of one RowImages call. Column values
// are appended to buf and row images are carved from cols, so a call
// allocates two buffers whatever the column count; scratch holds the
// converted cells, which nothing returned refers to.
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

// reserveRows allocates the column arena for rows of n columns in total.
func (b *imageBuilder) reserveRows(n int) {
	b.cols = make([]change.Column, 0, n)
}

// newRow returns an empty, non-nil row with room for n columns, carved
// from the arena (or allocated when the arena is short). Its capacity is
// capped, so appending past n never overwrites another row.
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

// newCells returns n zeroed cells from the scratch.
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

// keyLen is the number of key columns, zero for keyless tables.
func (b *imageBuilder) keyLen() int {
	if b.e.keyless() {
		return 0
	}
	return len(b.e.key)
}

// beforeLen bounds the columns of a before image built from an old tuple
// of kind k.
func (b *imageBuilder) beforeLen(k OldKind) int {
	if k == OldFull {
		return len(b.e.rel.Columns)
	}
	return len(b.e.identity)
}

func (b *imageBuilder) columnError(sentinel error, col int) error {
	return fmt.Errorf("%w: %s column %q", sentinel, b.e.rel.QualifiedName(), b.e.rel.Columns[col].Name)
}

// cells converts a tuple's values to JSON.
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

// rowAll builds a row image of every column, skipping unchanged cells.
func (b *imageBuilder) rowAll(cells []cell) change.Row {
	row := b.newRow(len(cells))
	for i := range cells {
		if !cells[i].unchanged {
			row = append(row, change.Column{Name: b.e.rel.Columns[i].Name, Value: cells[i].value})
		}
	}
	return row
}

// rowOf builds a row image of the columns idx, skipping unchanged cells.
func (b *imageBuilder) rowOf(cells []cell, idx []int) change.Row {
	row := b.newRow(len(idx))
	for _, i := range idx {
		if !cells[i].unchanged {
			row = append(row, change.Column{Name: b.e.rel.Columns[i].Name, Value: cells[i].value})
		}
	}
	return row
}

// required builds a row image from the columns idx, taking a column from
// fallback (when not nil) if it is unchanged in cells; a value still
// missing is ErrUnchangedKey.
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

// resolveUnchanged applies the TOAST rule to the new row: under FULL
// identity (copyFromOld) an unchanged value is copied from the old row;
// otherwise the column stays unchanged and its name is returned.
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
