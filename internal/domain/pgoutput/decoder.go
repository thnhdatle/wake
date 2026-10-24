// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"fmt"
	"slices"

	"github.com/thnhdatle/wake/internal/domain/schema"
)

// Decoder decodes messages and remembers the relations it has seen.
type Decoder struct {
	relations map[uint32]*relationEntry
	types     map[uint32]TypeMessage
	cells     []cell
}

type relationEntry struct {
	rel      schema.Relation
	override []string
	identity []int
	key      []int
}

// NewDecoder returns a decoder with no known relations.
func NewDecoder() *Decoder {
	return &Decoder{relations: make(map[uint32]*relationEntry), types: make(map[uint32]TypeMessage)}
}

// Decode decodes one message and records relations.
func (d *Decoder) Decode(data []byte) (Message, error) {
	m, err := Parse(data)
	if err != nil {
		return nil, err
	}
	switch msg := m.(type) {
	case *TypeMessage:
		d.types[msg.OID] = *msg
	case *Relation:
		for i := range msg.Columns {
			msg.Columns[i].TypeName = d.typeName(msg.Columns[i].TypeOID)
		}
		d.storeRelation(&msg.Relation)
	}
	return m, nil
}

func (d *Decoder) typeName(oid uint32) string {
	if name, ok := schema.TypeName(oid); ok {
		return name
	}
	t, ok := d.types[oid]
	if !ok {
		return schema.FallbackTypeName(oid)
	}
	switch t.Namespace {
	case "", "public", "pg_catalog":
		return t.Name
	}
	return t.Namespace + "." + t.Name
}

func (d *Decoder) storeRelation(rel *schema.Relation) {
	e := &relationEntry{rel: rel.Clone()}
	if old, ok := d.relations[rel.OID]; ok && old.override != nil && e.hasColumns(old.override) {
		e.override = old.override
	}
	e.index()
	d.relations[rel.OID] = e
}

func (e *relationEntry) hasColumns(names []string) bool {
	for _, n := range names {
		if e.rel.ColumnIndex(n) < 0 {
			return false
		}
	}
	return true
}

func (e *relationEntry) index() {
	e.identity = nil
	for i := range e.rel.Columns {
		if e.rel.Columns[i].Key {
			e.identity = append(e.identity, i)
		}
	}
	e.key = e.identity
	if e.rel.Identity == schema.IdentityFull && len(e.override) > 0 {
		e.key = make([]int, len(e.override))
		for i, n := range e.override {
			e.key[i] = e.rel.ColumnIndex(n)
		}
	}
}

func (e *relationEntry) keyless() bool {
	return e.rel.Identity == schema.IdentityNothing || len(e.key) == 0
}

func (d *Decoder) Relation(oid uint32) (schema.Relation, bool) {
	e, ok := d.relations[oid]
	if !ok {
		return schema.Relation{}, false
	}
	return e.rel, true
}

func (d *Decoder) SetKeyColumns(oid uint32, names []string) error {
	e, ok := d.relations[oid]
	if !ok {
		return fmt.Errorf("%w: oid %d", ErrUnknownRelation, oid)
	}
	for i, n := range names {
		if e.rel.ColumnIndex(n) < 0 || slices.Contains(names[:i], n) {
			return fmt.Errorf("%w: %q in %s (missing or repeated)", ErrUnknownColumn, n, e.rel.QualifiedName())
		}
	}
	e.override = nil
	if len(names) > 0 {
		e.override = slices.Clone(names)
	}
	e.index()
	return nil
}

func (d *Decoder) KeyColumns(oid uint32) ([]string, bool) {
	e, ok := d.relations[oid]
	if !ok {
		return nil, false
	}
	if e.keyless() {
		return nil, true
	}
	names := make([]string, len(e.key))
	for i, c := range e.key {
		names[i] = e.rel.Columns[c].Name
	}
	return names, true
}

func (d *Decoder) entry(oid uint32) (*relationEntry, error) {
	e, ok := d.relations[oid]
	if !ok {
		return nil, fmt.Errorf("%w: oid %d", ErrUnknownRelation, oid)
	}
	return e, nil
}
