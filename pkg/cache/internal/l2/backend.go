// SPDX-License-Identifier: Apache-2.0

package l2

import (
	"context"
	"time"
)

type Config struct {
	Namespace      string
	Channel        string
	BusHealthCheck time.Duration
}

// Backend is the complete L2 contract.
type Backend interface {
	Store
	Locker
	EpochStore
	Bus
}

type Store interface {
	Read(ctx context.Context, req ReadRequest) (ReadResult, error)
	Fill(ctx context.Context, req FillRequest) (bool, error)
	Set(ctx context.Context, req SetRequest) (WriteResult, error)
	Invalidate(ctx context.Context, req InvalidateRequest) (WriteResult, error)
}

type Locker interface {
	Lock(ctx context.Context, epoch uint64, key, token string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, epoch uint64, key, token string) error
}

type EpochStore interface {
	Epoch(ctx context.Context) (uint64, error)
	BumpEpoch(ctx context.Context) (uint64, error)
}

type Bus interface {
	Receive(ctx context.Context) (Event, error)
	Close() error
}

type ReadRequest struct {
	Epoch  uint64
	Key    string
	GenTTL time.Duration
}

type ReadResult struct {
	Gen     uint64
	Hit     bool
	Payload []byte
	TTL     time.Duration
}

type FillRequest struct {
	Epoch     uint64
	Key       string
	Gen       uint64
	Payload   []byte
	TTL       time.Duration
	GenTTL    time.Duration
	LockToken string
}

type SetRequest struct {
	Epoch   uint64
	Key     string
	Payload []byte
	TTL     time.Duration
	GenTTL  time.Duration
}

type InvalidateRequest struct {
	Epoch  uint64
	Key    string
	GenTTL time.Duration
}

type WriteResult struct {
	Gen   uint64
	Epoch uint64
}
