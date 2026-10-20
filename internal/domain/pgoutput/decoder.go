// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"fmt"
	"slices"

	"github.com/thnhdatle/wake/internal/domain/schema"
)

// Decoder parses messages of one replication stream and keeps the relation
// cache (by relation OID) and the type cache that Relation and Type messages
// maintain. It is not safe for concurrent use; one stream has one Decoder.
type Decoder struct {
	relations map[uint32]*relationEntry
	types     map[uint32]TypeMessage
	// cells is RowImages' reusable scratch for converted tuple values.
	cells []cell
}

// relationEntry is a cached relation with its precomputed column indexes.
type relationEntry struct {
	rel schema.Relation
	// override is the caller's key column list (SetKeyColumns), applied
	// under REPLICA IDENTITY FULL only.
	override []string
	// identity holds the indexes of the flagged replica identity columns;
	// key the indexes of the CDC event key columns.
	identity []int
	key      []int
}

// NewDecoder returns a Decoder with empty caches.
func NewDecoder() *Decoder {
	return &Decoder{relations: make(map[uint32]*relationEntry), types: make(map[uint32]TypeMessage)}
}

// Decode parses one message like Parse and applies it to the caches: a Type
// message is remembered for naming the columns of later relations; a
// Relation message gets its column TypeName resolved (built-in name, Type
// message name qualified by its schema unless it is "public" or pg_catalog,
// or schema.FallbackTypeName) and replaces the cached relation. A key
// column override survives a new Relation message only while every named
// column still exists.
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

// Relation returns the cached relation. Its Columns slice is shared with
// the cache and must not be modified.
func (d *Decoder) Relation(oid uint32) (schema.Relation, bool) {
	e, ok := d.relations[oid]
	if !ok {
		return schema.Relation{}, false
	}
	return e.rel, true
}

// SetKeyColumns sets the CDC event key columns of a relation from the catalog (its
// primary key, in key order). It matters under REPLICA IDENTITY FULL, where
// the Relation message flags every column and cannot tell the primary key;
// under other identities the flagged columns are the key and the override
// is kept but not applied. Empty names clears the override.
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

// KeyColumns returns the names of the CDC event key columns of a relation: the
// override under REPLICA IDENTITY FULL when set, otherwise the flagged
// replica identity columns; nil for a keyless relation.
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
