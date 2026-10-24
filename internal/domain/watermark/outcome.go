// SPDX-License-Identifier: Apache-2.0

package watermark

type Outcome struct {
	Chunk            uint64
	Retry            bool
	RetryXID         uint64
	Keep             []int
	DroppedWindow    int
	DroppedInvisible int
}
