// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtobufCodec returns a Codec for a generated protobuf message type V
// (a pointer type such as *pb.User).
func ProtobufCodec[V proto.Message]() Codec[V] { return protobufCodec[V]{} }

type protobufCodec[V proto.Message] struct{}

func (protobufCodec[V]) Marshal(v V) ([]byte, error) {
	b, err := proto.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("protobuf marshal: %w", err)
	}
	return b, nil
}

func (protobufCodec[V]) Unmarshal(data []byte) (V, error) {
	var zero V
	v, ok := zero.ProtoReflect().Type().New().Interface().(V)
	if !ok {
		return zero, fmt.Errorf("protobuf unmarshal into %T: %w", zero, errInvalidConfig)
	}
	if err := proto.Unmarshal(data, v); err != nil {
		return zero, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	return v, nil
}
