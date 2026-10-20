// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"errors"
	"fmt"
	"strings"
)

// Error codes. They appear at the start of error messages after the package
// prefix and are stable: operators and tests match on them.
const (
	// CodeTemplateInvalid reports a key template that does not parse.
	CodeTemplateInvalid = "template_invalid"
	// CodeMappingInvalid reports a malformed table name or an empty template
	// list in a mapping.
	CodeMappingInvalid = "mapping_invalid"
	// CodeTemplateRequiresFullIdentity reports a template that references
	// a non-key column of a table whose replica identity is not FULL.
	CodeTemplateRequiresFullIdentity = "template_requires_full_identity"
	// CodeTemplateUnknownColumn reports a template that references a column the
	// table does not publish.
	CodeTemplateUnknownColumn = "template_unknown_column"
	// CodeTemplateUnknownTable reports a mapped table that does not exist.
	CodeTemplateUnknownTable = "template_unknown_table"
	// CodeInvalidConfig reports an invalid option or source configuration.
	CodeInvalidConfig = "invalid_config"
	// CodeSourceNotRunning reports a health check on a source that is not
	// consuming.
	CodeSourceNotRunning = "source_not_running"
)

var (
	// ErrTemplate is matched by every *TemplateError.
	ErrTemplate = errors.New("cdcsource: " + CodeTemplateInvalid)
	// ErrMapping reports a malformed mapping (table name or template list).
	ErrMapping = errors.New("cdcsource: " + CodeMappingInvalid)
	// ErrRequiresFullIdentity is matched by every *IdentityError.
	ErrRequiresFullIdentity = errors.New("cdcsource: " + CodeTemplateRequiresFullIdentity)
	// ErrUnknownColumn reports a template column that the table does not have.
	ErrUnknownColumn = errors.New("cdcsource: " + CodeTemplateUnknownColumn)
	// ErrUnknownTable reports a mapped table missing from the table metadata.
	ErrUnknownTable = errors.New("cdcsource: " + CodeTemplateUnknownTable)
	// ErrInvalidConfig reports an invalid option or source configuration.
	ErrInvalidConfig = errors.New("cdcsource: " + CodeInvalidConfig)
	// ErrSourceNotRunning is returned by the Check method of a source that is
	// not consuming (before Consume or after it returned).
	ErrSourceNotRunning = errors.New("cdcsource: " + CodeSourceNotRunning)
)

// TemplateError describes why a key template does not parse.
type TemplateError struct {
	// Template is the template as given.
	Template string
	// Offset is the byte offset in Template where the problem was found.
	Offset int
	// Reason is a short description of the problem.
	Reason string
}

// Error implements error.
func (e *TemplateError) Error() string {
	return fmt.Sprintf("cdcsource: %s: %q: %s at offset %d", CodeTemplateInvalid, e.Template, e.Reason, e.Offset)
}

// Is reports whether target is ErrTemplate.
func (e *TemplateError) Is(target error) bool { return target == ErrTemplate }

// IdentityError reports templates of a table that reference columns which
// are not part of the table's replica identity key while the identity is not
// FULL: the before image of updates and deletes lacks those columns, so the
// keys rendered from the old values cannot be invalidated.
type IdentityError struct {
	// Table is "<schema>.<table>" as in the events.
	Table string
	// Columns are the referenced non-key columns, sorted.
	Columns []string
}

// Error implements error.
func (e *IdentityError) Error() string {
	return fmt.Sprintf("cdcsource: %s: table %s: templates reference non-key columns [%s] "+
		"but the replica identity is not FULL (ALTER TABLE ... REPLICA IDENTITY FULL)",
		CodeTemplateRequiresFullIdentity, e.Table, strings.Join(e.Columns, ", "))
}

// Is reports whether target is ErrRequiresFullIdentity.
func (e *IdentityError) Is(target error) bool { return target == ErrRequiresFullIdentity }

func invalidConfig(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}

// codeOrder lists the sentinels in the order ErrorCode tests them, most
// specific first.
var codeOrder = []struct {
	err  error
	code string
}{
	{ErrRequiresFullIdentity, CodeTemplateRequiresFullIdentity},
	{ErrUnknownTable, CodeTemplateUnknownTable},
	{ErrUnknownColumn, CodeTemplateUnknownColumn},
	{ErrTemplate, CodeTemplateInvalid},
	{ErrMapping, CodeMappingInvalid},
	{ErrInvalidConfig, CodeInvalidConfig},
	{ErrSourceNotRunning, CodeSourceNotRunning},
}

// ErrorCode returns the stable code (one of the Code constants) of the most
// specific cdcsource error in err's tree, or "" when there is none.
func ErrorCode(err error) string {
	for _, c := range codeOrder {
		if errors.Is(err, c.err) {
			return c.code
		}
	}
	return ""
}
