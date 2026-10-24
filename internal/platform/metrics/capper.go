// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"sync"
)

const (
	labelTable = "table"
	labelSink  = "sink"

	DefaultLabelLimit = 50
	OtherLabel        = "other"
)

// Capper bounds the number of distinct values of a label.
type Capper struct {
	limit    int
	reserved map[string]struct{}

	mu   sync.RWMutex
	seen map[string]struct{}
}

// NewCapper returns a Capper admitting limit distinct values.
func NewCapper(limit int, reserved ...string) *Capper {
	if limit <= 0 {
		limit = DefaultLabelLimit
	}
	c := &Capper{
		limit:    limit,
		reserved: make(map[string]struct{}, len(reserved)),
		seen:     make(map[string]struct{}, limit),
	}
	for _, r := range reserved {
		c.reserved[r] = struct{}{}
	}
	return c
}

func (c *Capper) Label(value string) string {
	value = strings.ToValidUTF8(value, "?")
	if _, ok := c.reserved[value]; ok {
		return value
	}
	c.mu.RLock()
	_, ok := c.seen[value]
	full := len(c.seen) >= c.limit
	c.mu.RUnlock()
	switch {
	case ok:
		return value
	case full:
		return OtherLabel
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[value]; ok {
		return value
	}
	if len(c.seen) >= c.limit {
		return OtherLabel
	}
	c.seen[value] = struct{}{}
	return value
}

func (c *Capper) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.seen)
}
