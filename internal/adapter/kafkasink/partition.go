// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kgo"
)

// hashPartition marks a record whose partition the partitioner derives from
// its key; a record with Partition >= 0 keeps that partition (the `t`
// fan-out, and every re-produce of a record that already has one).
const hashPartition = -1

// newPartitioner returns the sink's partitioner: Kafka's default murmur2 key
// hash (the same partition as the Java client for the same key and partition
// count), so every record of one key lands in one partition and keeps its
// order. A record without a key (a forwarded message) hashes its event id,
// carried in its first header. The `t` fan-out sets partitions explicitly.
func newPartitioner() kgo.Partitioner {
	return kgo.BasicConsistentPartitioner(func(string) func(*kgo.Record, int) int {
		return partitionFor
	})
}

func partitionFor(r *kgo.Record, n int) int {
	if r.Partition >= 0 && int(r.Partition) < n {
		return int(r.Partition)
	}
	key := r.Key
	if key == nil && len(r.Headers) > 0 {
		key = r.Headers[0].Value
	}
	return keyPartition(key, n)
}

// keyPartition returns Kafka's default partition of key among n partitions.
func keyPartition(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	return int(murmur2(key)&0x7fffffff) % n
}

// murmur2 is Kafka's Utils.murmur2 (seed 0x9747b28c).
func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	n := len(data)
	h := seed ^ uint32(n) //nolint:gosec // G115: Kafka's algorithm truncates the length to 32 bits
	for len(data) >= 4 {
		k := binary.LittleEndian.Uint32(data)
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		data = data[4:]
	}
	switch len(data) {
	case 3:
		h ^= uint32(data[2]) << 16
		h ^= uint32(data[1]) << 8
		h ^= uint32(data[0])
		h *= m
	case 2:
		h ^= uint32(data[1]) << 8
		h ^= uint32(data[0])
		h *= m
	case 1:
		h ^= uint32(data[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}
