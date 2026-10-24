// SPDX-License-Identifier: Apache-2.0

package schema

type Column struct {
	Name     string
	TypeOID  uint32
	TypeMod  int32
	TypeName string
	Key      bool
}

func (c *Column) sameShape(o *Column) bool {
	return c.Name == o.Name && c.TypeOID == o.TypeOID && c.TypeMod == o.TypeMod && c.Key == o.Key
}
