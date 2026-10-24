// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kgo"
)

const hashPartition = -1

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

func keyPartition(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	return int(murmur2(key)&0x7fffffff) % n
}

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
