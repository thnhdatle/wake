// SPDX-License-Identifier: Apache-2.0

package redisl2

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

// Backend implements l2.Backend over a go-redis UniversalClient.
type Backend struct {
	client  redis.UniversalClient
	ns      string
	channel string
	scripts scripts
	bus     *bus
}

var _ l2.Backend = (*Backend)(nil)

// New returns a backend for cfg.Namespace on client and starts its bus
// goroutine, which Close stops. It does no I/O: the subscription connects
// in the background and reports EventConnected once active, so New succeeds
// while Redis is unreachable. client stays owned by the caller.
func New(client redis.UniversalClient, cfg l2.Config) (*Backend, error) {
	if client == nil {
		return nil, fmt.Errorf("nil client: %w", errInvalidConfig)
	}
	if cfg.Namespace == "" || cfg.Channel == "" {
		return nil, fmt.Errorf("namespace %q, channel %q: %w", cfg.Namespace, cfg.Channel, errInvalidConfig)
	}
	return &Backend{
		client:  client,
		ns:      cfg.Namespace,
		channel: cfg.Channel,
		scripts: newScripts(),
		bus:     startBus(client, cfg.Channel, cfg.BusHealthCheck),
	}, nil
}

// Read implements l2.Store.
func (b *Backend) Read(ctx context.Context, req l2.ReadRequest) (l2.ReadResult, error) {
	v, g, _ := l2.KeyNames(b.ns, req.Epoch, req.Key)
	val, err := b.scripts.read.Run(ctx, b.client, []string{v, g}, millis(req.GenTTL)).Result()
	if err != nil {
		return l2.ReadResult{}, unavailable("read", req.Key, err)
	}
	return parseRead(val)
}

// Fill implements l2.Store.
func (b *Backend) Fill(ctx context.Context, req l2.FillRequest) (bool, error) {
	v, g, l := l2.KeyNames(b.ns, req.Epoch, req.Key)
	loaded := l2.EncodeMessage(l2.Event{Kind: l2.EventLoaded, Epoch: req.Epoch, Key: req.Key})
	n, err := b.scripts.fill.Run(ctx, b.client, []string{v, g, l},
		strconv.FormatUint(req.Gen, 10), req.Payload, millis(req.TTL), millis(req.GenTTL),
		req.LockToken, b.channel, loaded,
	).Int64()
	if err != nil {
		return false, unavailable("fill", req.Key, err)
	}
	return n == 1, nil
}

// Set implements l2.Store.
func (b *Backend) Set(ctx context.Context, req l2.SetRequest) (l2.WriteResult, error) {
	v, g, _ := l2.KeyNames(b.ns, req.Epoch, req.Key)
	return b.writeWithEpoch(ctx, "set", req.Key, b.scripts.set, []string{v, g},
		req.Payload, millis(req.TTL), millis(req.GenTTL), b.channel, invalidatePrefix(req.Epoch), req.Key)
}

// Invalidate implements l2.Store.
func (b *Backend) Invalidate(ctx context.Context, req l2.InvalidateRequest) (l2.WriteResult, error) {
	v, g, _ := l2.KeyNames(b.ns, req.Epoch, req.Key)
	return b.writeWithEpoch(ctx, "invalidate", req.Key, b.scripts.invalidate, []string{v, g},
		millis(req.GenTTL), b.channel, invalidatePrefix(req.Epoch), req.Key)
}

// invalidatePrefix is the part of "i|<epoch>|<gen>|<key>" known before the
// script runs; the script appends the new generation and the key.
func invalidatePrefix(epoch uint64) string {
	return "i|" + strconv.FormatUint(epoch, 10) + "|"
}

// writeWithEpoch runs a generation-incrementing script and, in the same
// pipeline, GETs the namespace epoch (another slot on Cluster, so not
// atomic with the script). A missing epoch is initialized by Epoch.
func (b *Backend) writeWithEpoch(ctx context.Context, op, key string, s *redis.Script, keys []string, args ...any) (
	l2.WriteResult, error,
) {
	cmd, epochCmd := b.evalWithEpoch(ctx, s, keys, args)
	if err := cmd.Err(); err != nil {
		return l2.WriteResult{}, unavailable(op, key, err)
	}
	gen, err := parseUint(cmd.Val())
	if err != nil {
		return l2.WriteResult{}, err
	}
	res := l2.WriteResult{Gen: gen}
	switch raw, err := epochCmd.Result(); {
	case errors.Is(err, redis.Nil):
		res.Epoch, err = b.Epoch(ctx)
		if err != nil {
			return l2.WriteResult{}, err
		}
	case err != nil:
		return l2.WriteResult{}, unavailable(op+" epoch", key, err)
	default:
		if res.Epoch, err = parseUint(raw); err != nil {
			return l2.WriteResult{}, err
		}
	}
	return res, nil
}

// evalWithEpoch pipelines EVALSHA and GET <ns>:epoch, repeating both with
// EVAL when the script cache is empty (NOSCRIPT means the script did not
// run, so repeating it is safe).
func (b *Backend) evalWithEpoch(ctx context.Context, s *redis.Script, keys []string, args []any) (
	*redis.Cmd, *redis.StringCmd,
) {
	epochKey := l2.EpochKey(b.ns)
	exec := func(eval func(redis.Pipeliner) *redis.Cmd) (*redis.Cmd, *redis.StringCmd) {
		pipe := b.client.Pipeline()
		cmd := eval(pipe)
		epochCmd := pipe.Get(ctx, epochKey)
		_, _ = pipe.Exec(ctx) // each command's own error is inspected by the caller
		return cmd, epochCmd
	}
	cmd, epochCmd := exec(func(p redis.Pipeliner) *redis.Cmd { return s.EvalSha(ctx, p, keys, args...) })
	if isNoScript(cmd.Err()) {
		cmd, epochCmd = exec(func(p redis.Pipeliner) *redis.Cmd { return s.Eval(ctx, p, keys, args...) })
	}
	return cmd, epochCmd
}

// Lock implements l2.Locker with SET <lock> token NX PX ttl.
func (b *Backend) Lock(ctx context.Context, epoch uint64, key, token string, ttl time.Duration) (bool, error) {
	_, _, l := l2.KeyNames(b.ns, epoch, key)
	ok, err := b.client.SetNX(ctx, l, token, time.Duration(millis(ttl))*time.Millisecond).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, unavailable("lock", key, err)
	}
	return ok, nil
}

// Unlock implements l2.Locker.
func (b *Backend) Unlock(ctx context.Context, epoch uint64, key, token string) error {
	_, _, l := l2.KeyNames(b.ns, epoch, key)
	loaded := l2.EncodeMessage(l2.Event{Kind: l2.EventLoaded, Epoch: epoch, Key: key})
	if err := b.scripts.unlock.Run(ctx, b.client, []string{l}, token, b.channel, loaded).Err(); err != nil {
		return unavailable("unlock", key, err)
	}
	return nil
}

// Epoch implements l2.EpochStore.
func (b *Backend) Epoch(ctx context.Context) (uint64, error) {
	val, err := b.scripts.epoch.Run(ctx, b.client, []string{l2.EpochKey(b.ns)}).Result()
	if err != nil {
		return 0, unavailable("epoch", "", err)
	}
	return parseUint(val)
}

// BumpEpoch implements l2.EpochStore.
func (b *Backend) BumpEpoch(ctx context.Context) (uint64, error) {
	val, err := b.scripts.bumpEpoch.Run(ctx, b.client, []string{l2.EpochKey(b.ns)}, b.channel).Result()
	if err != nil {
		return 0, unavailable("bump epoch", "", err)
	}
	return parseUint(val)
}

// Receive implements l2.Bus.
func (b *Backend) Receive(ctx context.Context) (l2.Event, error) { return b.bus.receive(ctx) }

// Close implements l2.Bus: it stops the bus goroutine and closes the
// subscription connection, but not the client. It is idempotent.
func (b *Backend) Close() error {
	b.bus.close()
	return nil
}
