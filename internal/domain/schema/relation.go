// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"slices"

	"github.com/thnhdatle/wake/internal/domain/change"
)

type ReplicaIdentity byte

const (
	IdentityDefault ReplicaIdentity = 'd'
	IdentityNothing ReplicaIdentity = 'n'
	IdentityFull    ReplicaIdentity = 'f'
	IdentityIndex   ReplicaIdentity = 'i'
)

func (ri ReplicaIdentity) Valid() bool {
	switch ri {
	case IdentityDefault, IdentityNothing, IdentityFull, IdentityIndex:
		return true
	}
	return false
}

// Relation describes one published table.
type Relation struct {
	OID       uint32
	Namespace string
	Name      string
	Identity  ReplicaIdentity
	Columns   []Column
}

func (r *Relation) QualifiedName() string {
	ns := r.Namespace
	if ns == "" {
		ns = "pg_catalog"
	}
	return ns + "." + r.Name
}

func (r *Relation) KeyColumnNames() []string {
	var names []string
	for i := range r.Columns {
		if r.Columns[i].Key {
			names = append(names, r.Columns[i].Name)
		}
	}
	return names
}

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

func (r *Relation) ColumnIndex(name string) int {
	for i := range r.Columns {
		if r.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

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

func (r *Relation) Clone() Relation {
	c := *r
	c.Columns = slices.Clone(r.Columns)
	return c
}
