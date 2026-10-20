// SPDX-License-Identifier: Apache-2.0

package pipeline

// AckToken identifies one delivery (record × sink) in the ack tracker. Its
// fields are meaningful only to the tracker that issued it.
type AckToken struct {
	// Tx is the tracker's sequence number of the delivery's transaction.
	Tx uint64
	// Group is the delivery's completion group, zero for none.
	Group uint64
}

// AckHandle acknowledges one delivery. The zero handle acknowledges nothing
// and returns nil, for records that are not tracked (tests, tools).
type AckHandle struct {
	acker Acker
	token AckToken
}

// NewAckHandle returns a handle that acknowledges tok through a.
func NewAckHandle(a Acker, tok AckToken) AckHandle {
	return AckHandle{acker: a, token: tok}
}

// Ack acknowledges the delivery. Sinks call it exactly once per record.
func (h AckHandle) Ack() error {
	if h.acker == nil {
		return nil
	}
	return h.acker.Ack(h.token)
}

// Token returns the delivery's token.
func (h AckHandle) Token() AckToken { return h.token }

// IsZero reports whether the handle tracks nothing.
func (h AckHandle) IsZero() bool { return h.acker == nil }
