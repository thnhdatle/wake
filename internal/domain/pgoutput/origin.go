// SPDX-License-Identifier: Apache-2.0

package pgoutput

import "github.com/thnhdatle/wake/internal/domain/lsn"

// Origin names the replication origin of the current transaction.
type Origin struct {
	CommitLSN lsn.LSN
	Name      string
}

// MessageType implements Message.
func (*Origin) MessageType() MessageType { return MessageOrigin }

func parseOrigin(r *reader) *Origin {
	m := &Origin{CommitLSN: lsn.LSN(r.u64("origin lsn"))}
	m.Name = r.cstring("origin name")
	return m
}
