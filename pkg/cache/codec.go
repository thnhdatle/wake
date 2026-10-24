// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"encoding/json"
	"fmt"
)

// Codec converts values to the bytes stored in L2 and back.
type Codec[V any] interface {
	Marshal(v V) ([]byte, error)
	Unmarshal(data []byte) (V, error)
}

// JSONCodec returns a Codec using encoding/json.
func JSONCodec[V any]() Codec[V] { return jsonCodec[V]{} }

type jsonCodec[V any] struct{}

func (jsonCodec[V]) Marshal(v V) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	return b, nil
}

func (jsonCodec[V]) Unmarshal(data []byte) (V, error) {
	var v V
	if err := json.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("json unmarshal: %w", err)
	}
	return v, nil
}
