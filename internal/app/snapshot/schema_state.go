// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"maps"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func RegistryState(st *pipeline.State) schema.RegistryState {
	rs := schema.RegistryState{Floor: st.SchemaFloor, Tables: make([]schema.TableState, 0, len(st.Schemas))}
	for name, ts := range maps.All(st.Schemas) {
		t := schema.TableState{Table: name, Version: ts.Version}
		if len(ts.ReplicaIdentity) == 1 {
			t.Identity = schema.ReplicaIdentity(ts.ReplicaIdentity[0])
		}
		if ts.Columns != nil {
			t.Columns = make([]schema.Column, len(ts.Columns))
			for i, c := range ts.Columns {
				t.Columns[i] = schema.Column{Name: c.Name, TypeOID: c.TypeOID, TypeMod: c.TypeMod, TypeName: c.Type, Key: c.Key}
			}
		}
		rs.Tables = append(rs.Tables, t)
	}
	return rs
}

func setRegistryState(st *pipeline.State, rs *schema.RegistryState) {
	st.SchemaFloor = rs.Floor
	st.Schemas = make(map[string]pipeline.TableSchema, len(rs.Tables))
	for _, t := range rs.Tables {
		ts := pipeline.TableSchema{Version: t.Version}
		if t.Identity != 0 {
			ts.ReplicaIdentity = string(rune(t.Identity))
		}
		if t.Columns != nil {
			ts.Columns = make([]pipeline.SchemaColumn, len(t.Columns))
			for i, c := range t.Columns {
				ts.Columns[i] = pipeline.SchemaColumn{
					Name: c.Name, Type: c.TypeName, TypeOID: c.TypeOID, TypeMod: c.TypeMod, Key: c.Key,
				}
			}
		}
		st.Schemas[t.Table] = ts
	}
}
