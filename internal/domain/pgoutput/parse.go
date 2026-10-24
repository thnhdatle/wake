// SPDX-License-Identifier: Apache-2.0

package pgoutput

import "fmt"

// Parse decodes one pgoutput message.
func Parse(data []byte) (Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty message", ErrMalformed)
	}
	r := &reader{buf: data, off: 1, typ: data[0]}
	var m Message
	switch MessageType(data[0]) {
	case MessageBegin:
		m = parseBegin(r)
	case MessageCommit:
		m = parseCommit(r)
	case MessageOrigin:
		m = parseOrigin(r)
	case MessageRelation:
		m = parseRelation(r)
	case MessageTypeDef:
		m = parseType(r)
	case MessageInsert:
		m = parseInsert(r)
	case MessageUpdate:
		m = parseUpdate(r)
	case MessageDelete:
		m = parseDelete(r)
	case MessageTruncate:
		m = parseTruncate(r)
	case MessageLogical:
		m = parseLogicalMessage(r)
	default:
		return nil, &UnsupportedMessageError{Type: data[0]}
	}
	if err := r.finish(); err != nil {
		return nil, err
	}
	return m, nil
}
