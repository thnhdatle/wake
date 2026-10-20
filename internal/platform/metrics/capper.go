// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"sync"
)

const (
	// labelTable is the name of the table label.
	labelTable = "table"
	// labelSink is the name of the sink label.
	labelSink = "sink"

	// DefaultLabelLimit is the number of distinct values a label taken from
	// user data keeps before the rest fall into OtherLabel, which bounds the
	// number of series.
	DefaultLabelLimit = 50
	// OtherLabel is the value of every label value beyond the limit.
	OtherLabel = "other"
)

// Capper bounds the cardinality of a label: the first limit distinct values
// pass through, every later new value maps to OtherLabel. A value that was
// admitted keeps passing through. Reserved values always pass through and do
// not count against the limit. A Capper is safe for concurrent use.
type Capper struct {
	limit    int
	reserved map[string]struct{}

	mu   sync.RWMutex // protects seen
	seen map[string]struct{}
}

// NewCapper returns a Capper admitting limit distinct values; limit <= 0
// means DefaultLabelLimit. Reserved values (for example "_other") are never
// capped.
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

// Label returns value if it is reserved or admitted, else OtherLabel. Invalid
// UTF-8 in value is replaced, since Prometheus rejects it in label values.
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

// Len returns the number of admitted values.
func (c *Capper) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.seen)
}
