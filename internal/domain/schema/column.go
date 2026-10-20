// SPDX-License-Identifier: Apache-2.0

package schema

// Column is one column of a relation as described by a pgoutput Relation
// message.
type Column struct {
	// Name is the column name.
	Name string
	// TypeOID is the column's type (pg_attribute.atttypid).
	TypeOID uint32
	// TypeMod is the type modifier (pg_attribute.atttypmod); -1 when none.
	TypeMod int32
	// TypeName is the name used in schema-change events: TypeName for
	// built-in types, the pgoutput Type message name for other types,
	// FallbackTypeName otherwise. It does not take part in comparisons.
	TypeName string
	// Key reports pgoutput column flag 1: the column is part of the replica
	// identity (every column under REPLICA IDENTITY FULL).
	Key bool
}

// sameShape compares the attributes that define a schema version.
func (c *Column) sameShape(o *Column) bool {
	return c.Name == o.Name && c.TypeOID == o.TypeOID && c.TypeMod == o.TypeMod && c.Key == o.Key
}
