// SPDX-License-Identifier: Apache-2.0

package pipeline

type AckToken struct {
	Tx    uint64
	Group uint64
}

type AckHandle struct {
	acker Acker
	token AckToken
}

func NewAckHandle(a Acker, tok AckToken) AckHandle {
	return AckHandle{acker: a, token: tok}
}

func (h AckHandle) Ack() error {
	if h.acker == nil {
		return nil
	}
	return h.acker.Ack(h.token)
}

func (h AckHandle) Token() AckToken { return h.token }

func (h AckHandle) IsZero() bool { return h.acker == nil }
