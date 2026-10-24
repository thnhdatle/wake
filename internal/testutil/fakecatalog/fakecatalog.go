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

type SlotResult struct {
	Status pipeline.SlotStatus
	Err    error
}

// Catalog is an in-memory catalog with scriptable answers.
type Catalog struct {
	mu          sync.Mutex
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
	changed     chan struct{}
}

// New returns a catalog whose answers are zero values.
func New() *Catalog { return &Catalog{changed: make(chan struct{})} }

func (c *Catalog) SetServerSettings(s pipeline.ServerSettings, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.settings, c.settingsErr = s, err
}

func (c *Catalog) ServerSettings(ctx context.Context) (pipeline.ServerSettings, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.ServerSettings{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.settings, c.settingsErr
}

func (c *Catalog) SetSlot(st pipeline.SlotStatus, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slot = SlotResult{Status: st, Err: err}
}

func (c *Catalog) QueueSlot(results ...SlotResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.script = append(c.script, results...)
}

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

func (c *Catalog) SlotCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.slotCalls)
}

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

func (c *Catalog) SetPublicationTables(tables []pipeline.TableInfo, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables, c.tablesErr = slices.Clone(tables), err
}

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

func (c *Catalog) PublicationCalls() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]string, len(c.pubCalls))
	for i, p := range c.pubCalls {
		out[i] = slices.Clone(p)
	}
	return out
}

func (c *Catalog) SetCurrentTime(t time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now, c.nowErr = t, err
}

func (c *Catalog) CurrentTime(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now, c.nowErr
}
