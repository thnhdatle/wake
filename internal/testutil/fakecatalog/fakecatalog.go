// SPDX-License-Identifier: Apache-2.0

package fakecatalog

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// SlotResult is one scripted Slot answer.
type SlotResult struct {
	Status pipeline.SlotStatus
	Err    error
}

// Catalog is the fake. It is safe for concurrent use. Every method returns
// the context's error, without recording the call, when ctx is already done.
type Catalog struct {
	mu          sync.Mutex // protects every field below
	settings    pipeline.ServerSettings
	settingsErr error
	slot        SlotResult
	script      []SlotResult
	slotCalls   []string
	tables      []pipeline.TableInfo
	tablesErr   error
	pubCalls    [][]string
	now         time.Time
	nowErr      error
	changed     chan struct{} // closed and replaced on every recorded Slot call
}

// New returns a catalog whose answers are zero values: Slot reports a
// missing slot until SetSlot or QueueSlot.
func New() *Catalog { return &Catalog{changed: make(chan struct{})} }

// SetServerSettings sets the ServerSettings result.
func (c *Catalog) SetServerSettings(s pipeline.ServerSettings, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.settings, c.settingsErr = s, err
}

// ServerSettings returns the configured settings.
func (c *Catalog) ServerSettings(ctx context.Context) (pipeline.ServerSettings, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.ServerSettings{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.settings, c.settingsErr
}

// SetSlot sets the Slot answer used once the queued answers are consumed.
func (c *Catalog) SetSlot(st pipeline.SlotStatus, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slot = SlotResult{Status: st, Err: err}
}

// QueueSlot appends answers that Slot returns, one per call, before the
// SetSlot answer.
func (c *Catalog) QueueSlot(results ...SlotResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.script = append(c.script, results...)
}

// Slot records the call and returns the next queued answer, or the SetSlot
// answer.
func (c *Catalog) Slot(ctx context.Context, name string) (pipeline.SlotStatus, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.SlotStatus{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slotCalls = append(c.slotCalls, name)
	close(c.changed)
	c.changed = make(chan struct{})
	res := c.slot
	if len(c.script) > 0 {
		res, c.script = c.script[0], c.script[1:]
	}
	return res.Status, res.Err
}

// SlotCalls returns the slot names of the Slot calls in order.
func (c *Catalog) SlotCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.slotCalls)
}

// WaitSlotCalls waits until Slot was called at least n times and returns the
// calls, or the calls so far and the context's error.
func (c *Catalog) WaitSlotCalls(ctx context.Context, n int) ([]string, error) {
	for {
		c.mu.Lock()
		got, ch := slices.Clone(c.slotCalls), c.changed
		c.mu.Unlock()
		if len(got) >= n {
			return got, nil
		}
		select {
		case <-ctx.Done():
			return got, fmt.Errorf("fakecatalog: wait for %d slot calls, have %d: %w", n, len(got), ctx.Err())
		case <-ch:
		}
	}
}

// SetPublicationTables sets the PublicationTables result.
func (c *Catalog) SetPublicationTables(tables []pipeline.TableInfo, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables, c.tablesErr = slices.Clone(tables), err
}

// PublicationTables records the call and returns a copy of the configured
// tables.
func (c *Catalog) PublicationTables(ctx context.Context, publications []string) ([]pipeline.TableInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pubCalls = append(c.pubCalls, slices.Clone(publications))
	if c.tablesErr != nil {
		return nil, c.tablesErr
	}
	return slices.Clone(c.tables), nil
}

// PublicationCalls returns the publications of each PublicationTables call.
func (c *Catalog) PublicationCalls() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]string, len(c.pubCalls))
	for i, p := range c.pubCalls {
		out[i] = slices.Clone(p)
	}
	return out
}

// SetCurrentTime sets the CurrentTime result.
func (c *Catalog) SetCurrentTime(t time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now, c.nowErr = t, err
}

// CurrentTime returns the configured time.
func (c *Catalog) CurrentTime(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now, c.nowErr
}
