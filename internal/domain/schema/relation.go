// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"slices"

	"github.com/thnhdatle/wake/internal/domain/change"
)

// ReplicaIdentity is pg_class.relreplident as sent in a Relation message.
type ReplicaIdentity byte

// Replica identity settings.
const (
	// IdentityDefault identifies rows by the primary key, if any.
	IdentityDefault ReplicaIdentity = 'd'
	// IdentityNothing logs no old row data.
	IdentityNothing ReplicaIdentity = 'n'
	// IdentityFull logs the whole old row.
	IdentityFull ReplicaIdentity = 'f'
	// IdentityIndex identifies rows by the columns of a chosen unique index.
	IdentityIndex ReplicaIdentity = 'i'
)

// Valid reports whether ri is one of the four Postgres settings.
func (ri ReplicaIdentity) Valid() bool {
	switch ri {
	case IdentityDefault, IdentityNothing, IdentityFull, IdentityIndex:
		return true
	}
	return false
}

// Relation describes a published table as of its latest pgoutput Relation
// message. Columns are in attribute-number order and exclude dropped and
// generated columns.
type Relation struct {
	OID uint32
	// Namespace is the schema name; pgoutput sends "" for pg_catalog.
	Namespace string
	Name      string
	Identity  ReplicaIdentity
	Columns   []Column
}

// QualifiedName returns "<namespace>.<name>", the registry key of the table.
func (r *Relation) QualifiedName() string {
	ns := r.Namespace
	if ns == "" {
		ns = "pg_catalog"
	}
	return ns + "." + r.Name
}

// KeyColumnNames returns the names of the columns flagged as part of the
// replica identity, in column order: the primary key or identity index
// columns, every column under REPLICA IDENTITY FULL, none for NOTHING or a
// DEFAULT identity without primary key.
func (r *Relation) KeyColumnNames() []string {
	var names []string
	for i := range r.Columns {
		if r.Columns[i].Key {
			names = append(names, r.Columns[i].Name)
		}
	}
	return names
}

// IsKeyless reports whether rows of the table have no key: identity NOTHING,
// or no column flagged as part of the identity (DEFAULT without primary
// key). Such tables are published insert-only.
func (r *Relation) IsKeyless() bool {
	if r.Identity == IdentityNothing {
		return true
	}
	for i := range r.Columns {
		if r.Columns[i].Key {
			return false
		}
	}
	return true
}

// ColumnIndex returns the position of the named column, or -1.
func (r *Relation) ColumnIndex(name string) int {
	for i := range r.Columns {
		if r.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

// SameSchema reports whether r and o have the same schema version shape:
// replica identity and, column by column, name, type OID, type modifier and
// identity flag. OID, namespace, name and type names are ignored.
func (r *Relation) SameSchema(o *Relation) bool {
	return r.Identity == o.Identity && sameColumns(r.Columns, o.Columns)
}

func sameColumns(a, b []Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].sameShape(&b[i]) {
			return false
		}
	}
	return true
}

// ColumnTypes returns the columns as schema-change event entries.
func (r *Relation) ColumnTypes() []change.ColumnType {
	return columnTypes(r.Columns)
}

func columnTypes(cols []Column) []change.ColumnType {
	out := make([]change.ColumnType, len(cols))
	for i := range cols {
		out[i] = change.ColumnType{Name: cols[i].Name, Type: cols[i].TypeName}
	}
	return out
}

// UnknownTypes returns the distinct type OIDs of columns whose type is not
// in the mapping table, in column order, so the caller can count a warning
// once per type.
func (r *Relation) UnknownTypes() []uint32 {
	var oids []uint32
	for i := range r.Columns {
		oid := r.Columns[i].TypeOID
		if !IsKnownType(oid) && !slices.Contains(oids, oid) {
			oids = append(oids, oid)
		}
	}
	return oids
}

// Clone returns a deep copy of r.
func (r *Relation) Clone() Relation {
	c := *r
	c.Columns = slices.Clone(r.Columns)
	return c
}
