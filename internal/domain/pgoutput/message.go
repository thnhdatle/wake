// SPDX-License-Identifier: Apache-2.0

package pgoutput

type MessageType byte

const (
	MessageBegin    MessageType = 'B'
	MessageCommit   MessageType = 'C'
	MessageOrigin   MessageType = 'O'
	MessageRelation MessageType = 'R'
	MessageTypeDef  MessageType = 'Y'
	MessageInsert   MessageType = 'I'
	MessageUpdate   MessageType = 'U'
	MessageDelete   MessageType = 'D'
	MessageTruncate MessageType = 'T'
	MessageLogical  MessageType = 'M'
)

// Message is a decoded pgoutput message.
type Message interface {
	MessageType() MessageType
}

const pgEpochUnixMs = 946_684_800_000

func pgTimeToUnixMs(us int64) int64 {
	ms := us / 1000
	if us%1000 < 0 {
		ms--
	}
	return ms + pgEpochUnixMs
}
