// SPDX-License-Identifier: Apache-2.0

package pgrepl

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const maxIdentifierBytes = 63

func ValidateSlotName(name string) error {
	if name == "" || len(name) > maxIdentifierBytes {
		return fmt.Errorf("%w: slot name %q must have 1 to %d characters", ErrInvalidArgument, name, maxIdentifierBytes)
	}
	for i := range len(name) {
		ch := name[i]
		if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '_' {
			return fmt.Errorf("%w: slot name %q may only contain [a-z0-9_]", ErrInvalidArgument, name)
		}
	}
	return nil
}

func startCommand(opts *pipeline.StartOptions) (string, error) {
	if err := ValidateSlotName(opts.Slot); err != nil {
		return "", err
	}
	if opts.ProtoVersion < 1 {
		return "", fmt.Errorf("%w: proto_version %d", ErrInvalidArgument, opts.ProtoVersion)
	}
	if len(opts.Publications) == 0 {
		return "", fmt.Errorf("%w: no publication", ErrInvalidArgument)
	}
	names := make([]string, len(opts.Publications))
	for i, p := range opts.Publications {
		if p == "" || len(p) > maxIdentifierBytes || strings.ContainsRune(p, 0) {
			return "", fmt.Errorf("%w: publication name %q", ErrInvalidArgument, p)
		}
		names[i] = pgx.Identifier{p}.Sanitize()
	}
	var b strings.Builder
	b.WriteString("START_REPLICATION SLOT ")
	b.WriteString(opts.Slot)
	b.WriteString(" LOGICAL ")
	b.WriteString(opts.Start.String())
	fmt.Fprintf(&b, " (proto_version '%d', publication_names ", opts.ProtoVersion)
	b.WriteString(quoteLiteral(strings.Join(names, ",")))
	if opts.Messages {
		b.WriteString(", messages 'true'")
	}
	b.WriteString(")")
	return b.String(), nil
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
