// SPDX-License-Identifier: Apache-2.0

package fakealert

import (
	"context"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Notifier is an in-memory alert notifier.
type Notifier struct {
	mu     sync.Mutex
	alerts []pipeline.Alert
	err    error
}

// New returns a notifier without alerts.
func New() *Notifier { return &Notifier{} }

func (n *Notifier) Notify(_ context.Context, a pipeline.Alert) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err != nil {
		return n.err
	}
	n.alerts = append(n.alerts, a)
	return nil
}

func (n *Notifier) Fail(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.err = err
}

func (n *Notifier) Alerts() []pipeline.Alert {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.alerts)
}
