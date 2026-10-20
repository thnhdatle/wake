// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Op codes of a recorded change (CDC event op letters).
const (
	OpCreate = "c"
	OpUpdate = "u"
	OpDelete = "d"
)

// Change is one committed row change recorded by a writer (M3).
type Change struct {
	// Table is "<schema>.<table>".
	Table string
	// Key is the canonical key JSON, see IDKey.
	Key string
	// Ver is the row's ver after the change (the deleted row's ver for d).
	Ver int64
	// Op is OpCreate, OpUpdate or OpDelete.
	Op string
	// Commit is the M1 commit time: host clock when COMMIT returned.
	Commit time.Time
}

// IDKey returns the canonical key JSON of a single bigint key column id.
func IDKey(id int64) string { return `{"id":` + strconv.FormatInt(id, 10) + `}` }

// TxFunc runs one transaction on a dedicated connection and returns the
// changes it committed. It runs in autocommit mode unless it begins a
// transaction itself; the generator stamps Commit when it returns nil.
type TxFunc func(ctx context.Context, conn *pgx.Conn, worker int, rng *rand.Rand) ([]Change, error)

// LoadOptions configure an open-model load.
type LoadOptions struct {
	// Rate is the offered transactions per second (constant arrival rate).
	Rate float64
	// Duration of the arrival schedule; zero runs until Stop.
	Duration time.Duration
	// Conns is the number of dedicated connections (workers).
	Conns int
	// Seed seeds every worker's RNG (worker i uses Seed+i).
	Seed uint64
	// Tx is the transaction body.
	Tx TxFunc
	// Backlog bounds arrivals waiting for a free worker; arrivals beyond it
	// are counted as Dropped (the schedule never waits for completions).
	Backlog int
	// DSN overrides PostgresDSN.
	DSN string
}

// LoadResult summarizes a finished load.
type LoadResult struct {
	Seed      uint64
	Offered   int
	Committed int
	Errors    int
	Dropped   int
	// MaxLag is the largest delay between an arrival's scheduled time and
	// the start of its transaction.
	MaxLag    time.Duration
	Elapsed   time.Duration
	Changes   []Change
	FirstErrs []string
}

// AchievedRate returns committed transactions per second.
func (r *LoadResult) AchievedRate() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Committed) / r.Elapsed.Seconds()
}

// Summary is a one-line description for test details.
func (r *LoadResult) Summary() string {
	return fmt.Sprintf("seed=%d offered=%d committed=%d errors=%d dropped=%d achieved=%.1f tx/s max_lag=%s",
		r.Seed, r.Offered, r.Committed, r.Errors, r.Dropped, r.AchievedRate(), r.MaxLag.Round(time.Millisecond))
}

// Load is a running open-model generator.
type Load struct {
	opts   LoadOptions
	cancel context.CancelFunc
	stop   chan struct{}
	once   sync.Once
	done   chan struct{}

	mu  sync.Mutex
	res LoadResult
}

// StartLoad opens the connections and starts the arrival schedule. Stop or
// the end of Duration ends arrivals; Wait returns once in-flight
// transactions finished.
func StartLoad(ctx context.Context, opts LoadOptions) (*Load, error) {
	if opts.Rate <= 0 || opts.Conns <= 0 || opts.Tx == nil {
		return nil, errors.New("stack: load needs Rate > 0, Conns > 0 and Tx")
	}
	if opts.Backlog <= 0 {
		opts.Backlog = max(int(opts.Rate)*10, 1000)
	}
	dsn := opts.DSN
	if dsn == "" {
		dsn = PostgresDSN
	}
	conns := make([]*pgx.Conn, 0, opts.Conns)
	for range opts.Conns {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			for _, o := range conns {
				_ = o.Close(context.WithoutCancel(ctx))
			}
			return nil, fmt.Errorf("stack: load connect: %w", err)
		}
		conns = append(conns, c)
	}
	lctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l := &Load{opts: opts, cancel: cancel, stop: make(chan struct{}), done: make(chan struct{})}
	l.res.Seed = opts.Seed
	go l.run(lctx, conns)
	return l, nil
}

type arrival struct{ at time.Time }

func (l *Load) run(ctx context.Context, conns []*pgx.Conn) {
	defer close(l.done)
	queue := make(chan arrival, l.opts.Backlog)
	var wg sync.WaitGroup
	start := time.Now()
	for i, c := range conns {
		wg.Go(func() {
			defer func() { _ = c.Close(context.WithoutCancel(ctx)) }()
			rng := rand.New(rand.NewPCG(l.opts.Seed+uint64(i), uint64(i)+1)) //nolint:gosec // reproducible load
			for a := range queue {
				l.execute(ctx, c, i, rng, a)
			}
		})
	}
	l.schedule(ctx, queue, start)
	close(queue)
	wg.Wait()
	l.mu.Lock()
	l.res.Elapsed = time.Since(start)
	l.mu.Unlock()
}

// schedule emits arrivals at the constant rate until Duration, Stop or ctx.
func (l *Load) schedule(ctx context.Context, queue chan<- arrival, start time.Time) {
	interval := time.Duration(float64(time.Second) / l.opts.Rate)
	total := -1 // arrivals at start + n*interval for n < total; -1 until Stop
	if l.opts.Duration > 0 {
		total = int((l.opts.Duration + interval - 1) / interval)
	}
	tick := time.NewTicker(min(interval, 5*time.Millisecond))
	defer tick.Stop()
	for n := 0; ; {
		// Arrivals whose time has come: at <= now.
		due := int(time.Since(start)/interval) + 1
		if total >= 0 {
			due = min(due, total)
		}
		for ; n < due; n++ {
			l.offer(queue, arrival{at: start.Add(time.Duration(n) * interval)})
		}
		if n == total {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-l.stop:
			return
		case <-tick.C:
		}
	}
}

func (l *Load) offer(queue chan<- arrival, a arrival) {
	l.mu.Lock()
	l.res.Offered++
	l.mu.Unlock()
	select {
	case queue <- a:
	default:
		l.mu.Lock()
		l.res.Dropped++
		l.mu.Unlock()
	}
}

func (l *Load) execute(ctx context.Context, c *pgx.Conn, worker int, rng *rand.Rand, a arrival) {
	lag := time.Since(a.at)
	tctx, cancel := context.WithTimeout(ctx, time.Minute)
	changes, err := l.opts.Tx(tctx, c, worker, rng)
	cancel()
	commit := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.res.MaxLag = max(l.res.MaxLag, lag)
	if err != nil {
		l.res.Errors++
		if len(l.res.FirstErrs) < 5 && !errors.Is(err, context.Canceled) {
			l.res.FirstErrs = append(l.res.FirstErrs, err.Error())
		}
		return
	}
	l.res.Committed++
	for _, ch := range changes {
		ch.Commit = commit
		l.res.Changes = append(l.res.Changes, ch)
	}
}

// Stop ends the arrival schedule; in-flight transactions complete.
func (l *Load) Stop() { l.once.Do(func() { close(l.stop) }) }

// Abort cancels in-flight transactions as well.
func (l *Load) Abort() {
	l.Stop()
	l.cancel()
}

// Wait waits for the schedule to end (Duration or Stop) and in-flight
// transactions to finish, then returns the result with changes sorted by
// commit time. With Duration zero, call Stop first.
func (l *Load) Wait() LoadResult {
	<-l.done
	l.cancel()
	l.mu.Lock()
	defer l.mu.Unlock()
	res := l.res
	res.Changes = append([]Change(nil), l.res.Changes...)
	sort.SliceStable(res.Changes, func(i, j int) bool { return res.Changes[i].Commit.Before(res.Changes[j].Commit) })
	return res
}
