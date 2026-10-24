// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func (r *Router) OpenTx(ctx context.Context, commit, end lsn.LSN, readAt time.Time) (pipeline.HeldTx, error) {
	tx, err := r.tracker.Begin(ctx, commit, end, readAt)
	if err != nil {
		return nil, err
	}
	return &heldTx{router: r, tx: tx}, nil
}

type heldTx struct {
	router *Router
	tx     TxHandle
}

func (h *heldTx) NewGroup(readAt time.Time) (pipeline.TxGroup, error) {
	g, err := h.router.tracker.NewGroup(h.tx, readAt)
	if err != nil {
		return nil, err
	}
	return &txGroup{router: h.router, group: g}, nil
}

func (h *heldTx) Seal() error { return h.router.tracker.Seal(h.tx) }

type txGroup struct {
	router *Router
	group  Group
}

func (g *txGroup) Route(ctx context.Context, rec pipeline.Record) (int, error) {
	return g.router.RouteToGroup(ctx, g.group, rec)
}

func (g *txGroup) Seal() error { return g.router.tracker.SealGroup(g.group) }

func (g *txGroup) Done() <-chan struct{} { return g.router.tracker.GroupDone(g.group) }
