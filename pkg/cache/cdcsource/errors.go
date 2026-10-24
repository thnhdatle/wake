// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"errors"
	"fmt"
	"strings"
)

// Error codes.
const (
	CodeTemplateInvalid              = "template_invalid"
	CodeMappingInvalid               = "mapping_invalid"
	CodeTemplateRequiresFullIdentity = "template_requires_full_identity"
	CodeTemplateUnknownColumn        = "template_unknown_column"
	CodeTemplateUnknownTable         = "template_unknown_table"
	CodeInvalidConfig                = "invalid_config"
	CodeSourceNotRunning             = "source_not_running"
)

var (
	// ErrTemplate is matched by every *TemplateError.
	ErrTemplate = errors.New("cdcsource: " + CodeTemplateInvalid)
	// ErrMapping reports a malformed mapping.
	ErrMapping = errors.New("cdcsource: " + CodeMappingInvalid)
	// ErrRequiresFullIdentity is matched by every *IdentityError.
	ErrRequiresFullIdentity = errors.New("cdcsource: " + CodeTemplateRequiresFullIdentity)
	// ErrUnknownColumn reports a template column that the table does not have.
	ErrUnknownColumn = errors.New("cdcsource: " + CodeTemplateUnknownColumn)
	// ErrUnknownTable reports a mapped table missing from the table metadata.
	ErrUnknownTable = errors.New("cdcsource: " + CodeTemplateUnknownTable)
	// ErrInvalidConfig reports an invalid option or source configuration.
	ErrInvalidConfig = errors.New("cdcsource: " + CodeInvalidConfig)
	// ErrSourceNotRunning is returned by the Check method of a source that is not consuming.
	ErrSourceNotRunning = errors.New("cdcsource: " + CodeSourceNotRunning)
)

// TemplateError describes why a key template does not parse.
type TemplateError struct {
	Template string
	// Offset is the byte offset of the problem in Template.
	Offset int
	Reason string
}

// Error implements error.
func (e *TemplateError) Error() string {
	return fmt.Sprintf("cdcsource: %s: %q: %s at offset %d", CodeTemplateInvalid, e.Template, e.Reason, e.Offset)
}

// Is reports whether target is ErrTemplate.
func (e *TemplateError) Is(target error) bool { return target == ErrTemplate }

// IdentityError reports templates that reference columns missing from the table's replica identity.
type IdentityError struct {
	Table   string
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

// ErrorCode returns the stable code of the most specific cdcsource error in err, or "" when there
// is none.
func ErrorCode(err error) string {
	for _, c := range codeOrder {
		if errors.Is(err, c.err) {
			return c.code
		}
	}
	return ""
}
