// SPDX-License-Identifier: Apache-2.0

package pgoutput

// MessageType is the first byte of a pgoutput message.
type MessageType byte

// Message types of protocol version 1 (with messages 'true').
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

// Message is one decoded pgoutput message: *Begin, *Commit, *Origin,
// *Relation, *TypeMessage, *Insert, *Update, *Delete, *Truncate or
// *LogicalMessage.
type Message interface {
	// MessageType returns the message's protocol type byte.
	MessageType() MessageType
}

// pgEpochUnixMs is 2000-01-01T00:00:00Z, the Postgres timestamp epoch, in
// Unix milliseconds.
const pgEpochUnixMs = 946_684_800_000

// pgTimeToUnixMs converts a Postgres timestamp (microseconds since
// 2000-01-01 UTC) to Unix milliseconds, rounding towards negative infinity.
func pgTimeToUnixMs(us int64) int64 {
	ms := us / 1000
	if us%1000 < 0 {
		ms--
	}
	return ms + pgEpochUnixMs
}
