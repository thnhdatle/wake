// SPDX-License-Identifier: Apache-2.0

package event

import (
	"encoding/json"
	"fmt"
	"strings"
)

// messageType is the value of the "type" member of forwarded logical messages.
const messageType = "message"

// Message is a forwarded Postgres logical message (api/schema/
// logical-message.v1.json, outside the CDC event contract). Its JSON form carries "type":
// "message" and the content as the base64 string "content_base64".
type Message struct {
	// ID has the form "<lsn>-m<n>".
	ID string `json:"id"`
	// LSN is the commit LSN for transactional messages, the message LSN
	// otherwise.
	LSN string `json:"lsn"`
	// TsMs is the time in Unix milliseconds.
	TsMs int64 `json:"ts_ms"`
	// Transactional tells whether the message was emitted inside a transaction.
	Transactional bool `json:"transactional"`
	// Prefix is the message prefix.
	Prefix string `json:"prefix"`
	// Content is the decoded message payload.
	Content []byte `json:"content_base64"`
}

// messageAlias drops the methods of Message to avoid recursion.
type messageAlias Message

// messageWire is the JSON form: the alias plus the constant type member.
type messageWire struct {
	Type string `json:"type"`
	messageAlias
}

var (
	messageRequired = []string{
		fieldType, fieldID, fieldLSN, fieldTsMs, fieldTransactional, fieldPrefix, fieldContent,
	}
	messageNonNull = messageRequired
)

// DecodeMessage decodes and validates a forwarded logical message. Errors
// follow the rules of Decode; content_base64 must be valid standard base64.
func DecodeMessage(data []byte) (*Message, error) {
	if err := probe(data, messageRequired, messageNonNull); err != nil {
		return nil, err
	}
	var w messageWire
	if err := unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.Type != messageType {
		return nil, fmt.Errorf("%w: type %q is not %q", ErrInvalid, w.Type, messageType)
	}
	m := Message(w.messageAlias)
	if err := m.validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// validate checks the id and the shared fields.
func (m *Message) validate() error {
	lsn, n, ok := strings.Cut(m.ID, "-m")
	if !ok {
		return fmt.Errorf("%w: message id %q needs the m<n> suffix", ErrInvalid, m.ID)
	}
	if _, err := ParseLSN(lsn); err != nil {
		return err
	}
	if _, err := parseDecimal(n); err != nil {
		return err
	}
	if _, err := ParseLSN(m.LSN); err != nil {
		return err
	}
	if m.TsMs < 0 {
		return fmt.Errorf("%w: negative ts_ms", ErrInvalid)
	}
	return nil
}

// MarshalJSON encodes the message with its "type": "message" member.
func (m Message) MarshalJSON() ([]byte, error) {
	if m.Content == nil {
		m.Content = []byte{} // encode as "" rather than null
	}
	b, err := json.Marshal(messageWire{Type: messageType, messageAlias: messageAlias(m)})
	if err != nil {
		return nil, fmt.Errorf("event: marshal message: %w", err)
	}
	return b, nil
}
