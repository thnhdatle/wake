// SPDX-License-Identifier: Apache-2.0

package event

import (
	"encoding/json"
	"fmt"
)

// Kind tells which decoder a payload needs.
type Kind string

// Kinds of payload that can share a webhook array or a transport.
const (
	// KindEvent is a CDC data event (no "type" member); use Decode.
	KindEvent Kind = "event"
	// KindSchemaChange is a schema-change event; use DecodeSchemaChange.
	KindSchemaChange Kind = "schema_change"
	// KindMessage is a forwarded logical message; use DecodeMessage.
	KindMessage Kind = "message"
)

// KindOf classifies one JSON payload by its "type" member: absent means a data
// event, "schema_change" and "message" select the other decoders. It does not
// validate the payload. A nil or zero-length input yields ErrTombstone and an
// unknown or non-string type yields an error wrapping ErrInvalid.
//
// To dispatch a webhook body, decode it into a []json.RawMessage and call
// KindOf on each element.
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
