// SPDX-License-Identifier: Apache-2.0

package event

import (
	"encoding/json"
	"fmt"
	"strings"
)

const messageType = "message"

// Message is a forwarded Postgres logical message.
type Message struct {
	// ID has the form "<lsn>-m<n>".
	ID string `json:"id"`
	// LSN is the commit LSN for transactional messages, the message LSN otherwise.
	LSN           string `json:"lsn"`
	TsMs          int64  `json:"ts_ms"`
	Transactional bool   `json:"transactional"`
	Prefix        string `json:"prefix"`
	Content       []byte `json:"content_base64"`
}

type messageAlias Message

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

// DecodeMessage decodes and validates a forwarded logical message.
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

// MarshalJSON encodes the message with its type member.
func (m Message) MarshalJSON() ([]byte, error) {
	if m.Content == nil {
		m.Content = []byte{}
	}
	b, err := json.Marshal(messageWire{Type: messageType, messageAlias: messageAlias(m)})
	if err != nil {
		return nil, fmt.Errorf("event: marshal message: %w", err)
	}
	return b, nil
}
