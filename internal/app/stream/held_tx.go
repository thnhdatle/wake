// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// OpenTx registers a transaction with the ack tracker, like Dispatch, but
// returns it unsealed so that the caller can attach completion groups: the
// initial-snapshot pseudo-transaction at the consistent point, opened
// before the reader registers any stream transaction.
func (r *Router) OpenTx(ctx context.Context, commit, end lsn.LSN, readAt time.Time) (pipeline.HeldTx, error) {
	tx, err := r.tracker.Begin(ctx, commit, end, readAt)
	if err != nil {
		return nil, err
	}
	return &heldTx{router: r, tx: tx}, nil
}

// heldTx implements pipeline.HeldTx over the tracker and router.
type heldTx struct {
	router *Router
	tx     TxHandle
}

// NewGroup implements pipeline.HeldTx.
func (h *heldTx) NewGroup(readAt time.Time) (pipeline.TxGroup, error) {
	g, err := h.router.tracker.NewGroup(h.tx, readAt)
	if err != nil {
		return nil, err
	}
	return &txGroup{router: h.router, group: g}, nil
}

// Seal implements pipeline.HeldTx.
func (h *heldTx) Seal() error { return h.router.tracker.Seal(h.tx) }

// txGroup implements pipeline.TxGroup.
type txGroup struct {
	router *Router
	group  Group
}

// Route implements pipeline.TxGroup.
func (g *txGroup) Route(ctx context.Context, rec pipeline.Record) (int, error) {
	return g.router.RouteToGroup(ctx, g.group, rec)
}

// Seal implements pipeline.TxGroup.
func (g *txGroup) Seal() error { return g.router.tracker.SealGroup(g.group) }

// Done implements pipeline.TxGroup.
func (g *txGroup) Done() <-chan struct{} { return g.router.tracker.GroupDone(g.group) }
