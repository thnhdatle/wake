// SPDX-License-Identifier: Apache-2.0

package redisl2

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

// millis converts d to the whole milliseconds the scripts use, rounding up
// so that a positive duration never becomes 0 (which means "no expiry").
func millis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// parseUint parses a decimal reply: the scripts return generations and
// epochs as strings so that no Lua number conversion is involved.
func parseUint(v any) (uint64, error) {
	var (
		n   uint64
		err error
	)
	switch x := v.(type) {
	case string:
		n, err = strconv.ParseUint(x, 10, 64)
	case int64:
		if x < 0 {
			err = errors.New("negative")
		}
		n = uint64(x) //nolint:gosec // G115: checked non-negative above
	default:
		err = fmt.Errorf("type %T", v)
	}
	if err != nil {
		return 0, fmt.Errorf("redisl2: reply %v: %w", v, l2.ErrMalformed)
	}
	return n, nil
}

// parseRead decodes the read script's reply: {gen} or {gen, ttl_ms, payload}.
func parseRead(v any) (l2.ReadResult, error) {
	arr, ok := v.([]any)
	if !ok || (len(arr) != 1 && len(arr) != 3) {
		return l2.ReadResult{}, fmt.Errorf("redisl2: read reply %T: %w", v, l2.ErrMalformed)
	}
	gen, err := parseUint(arr[0])
	if err != nil {
		return l2.ReadResult{}, err
	}
	res := l2.ReadResult{Gen: gen}
	if len(arr) == 1 {
		return res, nil
	}
	ttl, err := parseUint(arr[1])
	payload, ok := arr[2].(string)
	if err != nil || !ok {
		return l2.ReadResult{}, fmt.Errorf("redisl2: read reply: %w", l2.ErrMalformed)
	}
	res.Hit = true
	res.TTL = time.Duration(ttl) * time.Millisecond //nolint:gosec // G115: a TTL in ms fits int64
	res.Payload = []byte(payload)
	return res, nil
}

// isNoScript reports a NOSCRIPT reply to EVALSHA (empty script cache).
func isNoScript(err error) bool {
	return errors.Is(err, redis.ErrNoScript) || redis.HasErrorPrefix(err, "NOSCRIPT")
}

// unavailable wraps a failed command for op on key (empty for epoch ops).
func unavailable(op, key string, err error) error {
	if key == "" {
		return fmt.Errorf("redisl2: %s: %w: %w", op, ErrUnavailable, err)
	}
	return fmt.Errorf("redisl2: %s %q: %w: %w", op, key, ErrUnavailable, err)
}
