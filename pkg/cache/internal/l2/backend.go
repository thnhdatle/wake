// SPDX-License-Identifier: Apache-2.0

package l2

import (
	"context"
	"time"
)

// Config is what the cache core hands to an L2 factory.
type Config struct {
	// Namespace is the validated cache namespace ([A-Za-z0-9_-]+).
	Namespace string
	// Channel is the invalidation bus channel (default wake:inval:<ns>).
	Channel string
	// BusHealthCheck is the idle interval after which the bus connection is
	// probed; a link that stays silent for two intervals is treated as lost.
	// The core sets it to half of degraded_l1_ttl so that a dead link is
	// detected within degraded_l1_ttl. Zero selects the implementation
	// default; implementations without a network link ignore it.
	BusHealthCheck time.Duration
}

// Backend is the complete L2 contract. Every method that touches per-key
// state runs as one atomic server-side script over keys sharing the {<key>}
// hash tag. All generation arithmetic happens on the server: "TIME" below is
// the server clock in microseconds, never the instance clock.
//
// Generation key rules shared by every script:
//   - a missing generation key is initialized to TIME (microseconds), so a
//     re-created generation never repeats an earlier value;
//   - every script that touches a generation key extends its TTL to at least
//     the GenTTL argument and never shortens it.
type Backend interface {
	Store
	Locker
	EpochStore
	Bus
}

// Store holds generation-checked values.
type Store interface {
	// Read returns the current generation of key (initializing it when
	// missing) and the stored value when value.gen equals that generation and
	// value.expires_at is in the future.
	Read(ctx context.Context, req ReadRequest) (ReadResult, error)
	// Fill writes the value only if the generation still equals req.Gen. When
	// req.LockToken is non-empty and equals the lock key's value it deletes
	// the lock. It publishes a "loaded" message when it stored the value or
	// released the lock.
	Fill(ctx context.Context, req FillRequest) (bool, error)
	// Set increments the generation, writes the value with the new generation
	// and publishes an "invalidate" message carrying it, in one script. It
	// also returns the namespace epoch (see WriteResult).
	Set(ctx context.Context, req SetRequest) (WriteResult, error)
	// Invalidate increments the generation, deletes the value and publishes
	// an "invalidate" message carrying the new generation, in one script. It
	// also returns the namespace epoch (see WriteResult).
	Invalidate(ctx context.Context, req InvalidateRequest) (WriteResult, error)
}

// Locker is the per-key distributed load lock.
type Locker interface {
	// Lock runs SET <lock> token NX PX ttl and reports whether it acquired it.
	Lock(ctx context.Context, epoch uint64, key, token string, ttl time.Duration) (bool, error)
	// Unlock deletes the lock only if it still holds token and then publishes
	// a "loaded" message so waiters re-check L2.
	Unlock(ctx context.Context, epoch uint64, key, token string) error
}

// EpochStore holds the namespace epoch in <ns>:epoch (no TTL).
type EpochStore interface {
	// Epoch returns the epoch, initializing a missing key to TIME
	// (microseconds) with SET NX so it never goes back to an earlier value.
	Epoch(ctx context.Context) (uint64, error)
	// BumpEpoch increments the epoch (initializing it first when missing) and
	// publishes an "epoch" message with the new value.
	BumpEpoch(ctx context.Context) (uint64, error)
}

// Bus is the invalidation bus subscription, created with the backend.
type Bus interface {
	// Receive blocks until the next event. The first event is EventConnected
	// once the subscription is active. A lost connection yields
	// EventDisconnected; messages published while disconnected are lost; the
	// implementation reconnects by itself and then yields EventConnected
	// again. Receive returns ctx.Err() when ctx is done and ErrClosed after
	// Close; it never returns other errors.
	Receive(ctx context.Context) (Event, error)
	// Close releases the subscription and connections owned by the backend.
	Close() error
}

// ReadRequest asks for the value of Key in Epoch.
type ReadRequest struct {
	Epoch  uint64
	Key    string
	GenTTL time.Duration
}

// ReadResult is the outcome of Read.
type ReadResult struct {
	// Gen is the current generation of the key.
	Gen uint64
	// Hit reports a stored value whose generation equals Gen and that has
	// not expired.
	Hit bool
	// Payload is the stored payload on a hit.
	Payload []byte
	// TTL is the remaining lifetime of the value on a hit
	// (expires_at - TIME).
	TTL time.Duration
}

// FillRequest is a generation-checked write by a loader.
type FillRequest struct {
	Epoch   uint64
	Key     string
	Gen     uint64
	Payload []byte
	TTL     time.Duration
	GenTTL  time.Duration
	// LockToken releases the load lock when non-empty and still held.
	LockToken string
}

// SetRequest is an unconditional write that starts a new generation.
type SetRequest struct {
	Epoch   uint64
	Key     string
	Payload []byte
	TTL     time.Duration
	GenTTL  time.Duration
}

// InvalidateRequest starts a new generation without a value.
type InvalidateRequest struct {
	Epoch  uint64
	Key    string
	GenTTL time.Duration
}

// WriteResult is the outcome of Set and Invalidate.
type WriteResult struct {
	// Gen is the new generation.
	Gen uint64
	// Epoch is the namespace epoch read in the same round trip (GET
	// <ns>:epoch, pipelined; not necessarily atomic with the script). The
	// core repeats the write when it differs from the request epoch.
	Epoch uint64
}
