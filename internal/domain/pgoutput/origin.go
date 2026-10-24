// SPDX-License-Identifier: Apache-2.0

package pgoutput

import "github.com/thnhdatle/wake/internal/domain/lsn"

type Origin struct {
	CommitLSN lsn.LSN
	Name      string
}

func (*Origin) MessageType() MessageType { return MessageOrigin }

func parseOrigin(r *reader) *Origin {
	m := &Origin{CommitLSN: lsn.LSN(r.u64("origin lsn"))}
	m.Name = r.cstring("origin name")
	return m
}
