// SPDX-License-Identifier: Apache-2.0

package event

// Op is the operation code of a CDC data event.
type Op string

// Operations defined by the CDC event contract.
const (
	OpCreate   Op = "c"
	OpUpdate   Op = "u"
	OpDelete   Op = "d"
	OpRead     Op = "r"
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
