// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Payload envelope stored in L2 (inside the l2 value record): one flags
// byte followed by the codec bytes, zstd-compressed when flagZstd is set.
const (
	flagNegative byte = 1 << 0
	flagZstd     byte = 1 << 1

	maxDecodedBytes = 64 << 20
)

// envelope seals and opens payloads. The decoder always exists so payloads
// compressed by another instance can be read.
type envelope struct {
	minBytes int
	enc      *zstd.Encoder // nil without compression
	dec      *zstd.Decoder
}

func newEnvelope(minBytes int) (*envelope, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecodedBytes))
	if err != nil {
		return nil, fmt.Errorf("zstd decoder: %w", err)
	}
	e := &envelope{minBytes: minBytes, dec: dec}
	if minBytes > 0 {
		e.enc, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil {
			dec.Close()
			return nil, fmt.Errorf("zstd encoder: %w", err)
		}
	}
	return e, nil
}

func (e *envelope) seal(raw []byte, negative bool) []byte {
	if negative {
		return []byte{flagNegative}
	}
	if e.enc != nil && len(raw) >= e.minBytes {
		return e.enc.EncodeAll(raw, []byte{flagZstd})
	}
	out := make([]byte, 0, len(raw)+1)
	out = append(out, 0)
	return append(out, raw...)
}

func (e *envelope) open(payload []byte) (raw []byte, negative bool, err error) {
	if len(payload) == 0 {
		return nil, false, errMalformedValue
	}
	flags, body := payload[0], payload[1:]
	if flags&flagNegative != 0 {
		return nil, true, nil
	}
	if flags&flagZstd != 0 {
		raw, err = e.dec.DecodeAll(body, nil)
		if err != nil {
			return nil, false, fmt.Errorf("zstd: %w", err)
		}
		return raw, false, nil
	}
	return body, false, nil
}

func (e *envelope) close() {
	if e.enc != nil {
		_ = e.enc.Close() // EncodeAll-only encoder: Close cannot fail
	}
	e.dec.Close()
}
