// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgpackCodec returns a Codec using MessagePack.
func MsgpackCodec[V any]() Codec[V] { return msgpackCodec[V]{} }

type msgpackCodec[V any] struct{}

func (msgpackCodec[V]) Marshal(v V) ([]byte, error) {
	b, err := msgpack.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("msgpack marshal: %w", err)
	}
	return b, nil
}

func (msgpackCodec[V]) Unmarshal(data []byte) (V, error) {
	var v V
	if err := msgpack.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("msgpack unmarshal: %w", err)
	}
	return v, nil
}
