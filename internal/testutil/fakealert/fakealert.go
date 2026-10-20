// SPDX-License-Identifier: Apache-2.0

package fakealert

import (
	"context"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Notifier is the fake. It is safe for concurrent use.
type Notifier struct {
	mu     sync.Mutex // protects every field below
	alerts []pipeline.Alert
	err    error
}

// New returns a notifier without alerts.
func New() *Notifier { return &Notifier{} }

// Notify records a, or returns the injected error without recording it.
func (n *Notifier) Notify(_ context.Context, a pipeline.Alert) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err != nil {
		return n.err
	}
	n.alerts = append(n.alerts, a)
	return nil
}

// Fail makes Notify return err; nil restores it.
func (n *Notifier) Fail(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.err = err
}

// Alerts returns the recorded alerts in order.
func (n *Notifier) Alerts() []pipeline.Alert {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.alerts)
}
