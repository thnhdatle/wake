// SPDX-License-Identifier: Apache-2.0

package snapshot

const OtherTable = "_other"

type Metrics interface {
	RowsAcked(table string, n uint64)
	Request(table string)
	ChunkRetry(table string)
}

type nopMetrics struct{}

func (nopMetrics) RowsAcked(string, uint64) {}
func (nopMetrics) Request(string)           {}
func (nopMetrics) ChunkRetry(string)        {}
