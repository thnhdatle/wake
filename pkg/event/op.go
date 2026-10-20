// SPDX-License-Identifier: Apache-2.0

package event

// Op is the operation code of a CDC data event.
type Op string

// Operations defined by the CDC event contract. Consumers must ignore (and count) any
// other value; see Op.Known.
const (
	// OpCreate is an inserted row.
	OpCreate Op = "c"
	// OpUpdate is an updated row.
	OpUpdate Op = "u"
	// OpDelete is a deleted row.
	OpDelete Op = "d"
	// OpRead is a snapshot read of an existing row.
	OpRead Op = "r"
	// OpTruncate is a table truncation; it carries no key.
	OpTruncate Op = "t"
)

// Known reports whether o is one of the operations defined by the CDC event contract.
func (o Op) Known() bool {
	switch o {
	case OpCreate, OpUpdate, OpDelete, OpRead, OpTruncate:
		return true
	}
	return false
}
