// SPDX-License-Identifier: Apache-2.0

package event

import (
	"encoding/json"
	"fmt"
)

// Kind tells which decoder a payload needs.
type Kind string

// Kinds of payload.
const (
	KindEvent        Kind = "event"
	KindSchemaChange Kind = "schema_change"
	KindMessage      Kind = "message"
)

// KindOf classifies one JSON payload by its "type" member.
func KindOf(data []byte) (Kind, error) {
	if IsKafkaTombstone(data) {
		return "", ErrTombstone
	}
	var head struct {
		Type json.RawMessage `json:"type"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if len(head.Type) == 0 {
		return KindEvent, nil
	}
	var t string
	if err := json.Unmarshal(head.Type, &t); err != nil {
		return "", fmt.Errorf("%w: type is not a string", ErrInvalid)
	}
	switch Kind(t) {
	case KindSchemaChange, KindMessage:
		return Kind(t), nil
	case KindEvent:
	}
	return "", fmt.Errorf("%w: unknown type %q", ErrInvalid, t)
}
