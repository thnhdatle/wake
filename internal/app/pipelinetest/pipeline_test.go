// SPDX-License-Identifier: Apache-2.0

package pipelinetest

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/watermark"
	"github.com/thnhdatle/wake/internal/testutil/fakestate"
)

// requestSnapshot is what `wake snapshot table` does.
func requestSnapshot(t *testing.T, db *fakeDB, table string) {
	t.Helper()
	content, err := watermark.Trigger{Table: table}.MarshalJSON()
	require.NoError(t, err)
	_, err = db.EmitMessage(context.Background(), watermark.TriggerPrefix, content)
	require.NoError(t, err)
}

// readEvents returns the `r` events of the records.
func readEvents(recs []pipeline.Record) []pipeline.Record {
	var out []pipeline.Record
	for _, r := range recs {
		if r.Op == change.OpRead {
			out = append(out, r)
		}
	}
	return out
}

// TestPipeline_WatermarkSnapshot_ConcurrentWrites snapshots a table through
// watermarks while a writer changes it, including inside every chunk's
// LOW–HIGH window (updates, key-changing updates and deletes of rows the
// chunk just read): the final sink state must equal the database.
func TestPipeline_WatermarkSnapshot_ConcurrentWrites(t *testing.T) {
	t.Parallel()
	for seed := range uint64(4) {
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			db := newFakeDB(items)
			db.load(items.qualified(), 400)
			w := newWriter(db, items.qualified(), seed)
			// Inside the window: change rows of the chunk just read.
			db.onRead = func(n int) { w.touch(int64(n*25-20), int64(n*25)) }
			p := startProc(t, db, fakestate.New(), procOpts{mode: snapshot.ModeNever, chunkRows: 25, nonce: nonce(1)})
			requestSnapshot(t, db, items.qualified())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 300 {
					w.step()
				}
			}()
			wg.Wait()
			p.waitIdle(t, db)
			assert.Equal(t, db.state(items.qualified()), finalState(t, p.sink.Received(), items.qualified()))
			rows, requests, _ := p.metrics.snapshot()
			assert.Equal(t, []string{items.qualified()}, requests)
			assert.Positive(t, rows[items.qualified()])
			reads := readEvents(p.sink.Received())
			require.NotEmpty(t, reads)
			for _, r := range reads {
				assert.Equal(t, uint64(1), r.ID.Run, "first run_id")
				assert.Equal(t, change.IDKindSnapshot, r.ID.Kind)
			}
		})
	}
}

// TestPipeline_WatermarkSnapshot_RowsCarryHighWatermark checks the CDC event fields
// of watermark `r` events: lsn and ts_ms of the HIGH transaction, no tx_id,
// n = c × chunk_rows + i.
func TestPipeline_WatermarkSnapshot_RowsCarryHighWatermark(t *testing.T) {
	t.Parallel()
	db := newFakeDB(items)
	db.load(items.qualified(), 7)
	p := startProc(t, db, fakestate.New(), procOpts{mode: snapshot.ModeNever, chunkRows: 3, nonce: nonce(1)})
	requestSnapshot(t, db, items.qualified())
	p.waitIdle(t, db)
	reads := readEvents(p.sink.Received())
	require.Len(t, reads, 7)
	ns := make([]uint64, 0, len(reads))
	for _, r := range reads {
		ns = append(ns, r.ID.N)
		var payload map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(r.Payload, &payload))
		assert.NotContains(t, payload, "tx_id")
		assert.JSONEq(t, `"`+r.ID.LSN.String()+`"`, string(payload["lsn"]))
		assert.JSONEq(t, "null", string(payload["before"]))
	}
	// Chunks 0, 1 and 2 of three rows each: n = c*3 + i.
	assert.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6}, ns)
	assert.NotEqual(t, reads[0].ID.LSN, reads[3].ID.LSN, "each chunk has its own HIGH")
	assert.Equal(t, db.state(items.qualified()), finalState(t, p.sink.Received(), items.qualified()))
}

// TestPipeline_WatermarkSnapshot_RetriesInvisibleCommit: a transaction whose
// commit record the stream read before the run started is still invisible
// to the first chunk read, and pg_xact_status reports it committed, so the
// chunk is discarded and re-run.
func TestPipeline_WatermarkSnapshot_RetriesInvisibleCommit(t *testing.T) {
	t.Parallel()
	db := newFakeDB(items)
	db.load(items.qualified(), 10)
	db.onRead = func(n int) {
		if n == 1 {
			db.publish()
		}
	}
	p := startProc(t, db, fakestate.New(), procOpts{mode: snapshot.ModeNever, chunkRows: 100, nonce: nonce(1)})
	db.commitInvisible(op{kind: 'U', table: items.qualified(), id: 5, v: "late"})
	require.NoError(t, p.sink.WaitReceived(context.Background(), 1))
	requestSnapshot(t, db, items.qualified())
	p.waitIdle(t, db)
	_, _, retries := p.metrics.snapshot()
	assert.Equal(t, 1, retries)
	state := finalState(t, p.sink.Received(), items.qualified())
	assert.Equal(t, db.state(items.qualified()), state)
	assert.Equal(t, "late", state[5])
}

// TestPipeline_SnapshotRequests: requests for uncaptured tables and invalid
// requests are counted as "_other" and ignored; a request replayed after a
// restart is not queued again.
func TestPipeline_SnapshotRequests(t *testing.T) {
	t.Parallel()
	db := newFakeDB(items)
	db.load(items.qualified(), 3)
	store := fakestate.New()
	p := startProc(t, db, store, procOpts{mode: snapshot.ModeNever, chunkRows: 10, nonce: nonce(1)})
	requestSnapshot(t, db, "public.missing")
	_, err := db.EmitMessage(context.Background(), watermark.TriggerPrefix, []byte(`{"table":`))
	require.NoError(t, err)
	requestSnapshot(t, db, items.qualified())
	p.waitIdle(t, db)
	_, requests, _ := p.metrics.snapshot()
	assert.ElementsMatch(t, []string{snapshot.OtherTable, snapshot.OtherTable, items.qualified()}, requests)
	assert.Len(t, readEvents(p.sink.Received()), 3)
	p.stop(t)

	// A restart from an older confirmed LSN replays the requests: the state
	// remembers the latest one, so nothing runs again.
	st, ok := store.State()
	require.True(t, ok)
	require.NotZero(t, st.Snapshot.TriggerLSN)
	db.mu.Lock()
	db.confirmed = st.ConsistentPoint
	db.mu.Unlock()
	p2 := startProc(t, db, store, procOpts{mode: snapshot.ModeNever, chunkRows: 10, nonce: nonce(2)})
	p2.waitIdle(t, db)
	assert.Empty(t, readEvents(p2.sink.Received()))
	assert.Equal(t, uint64(1), p2.keeper.State().RunCounter)
}

// TestPipeline_InitialSnapshot copies two tables at the exported snapshot
// with two workers while writers run; the stream is released after every
// `r` event, which carries the consistent point and the clock_timestamp.
func TestPipeline_InitialSnapshot(t *testing.T) {
	t.Parallel()
	db := newFakeDB(items, orders)
	db.load(items.qualified(), 120)
	db.load(orders.qualified(), 80)
	wi, wo := newWriter(db, items.qualified(), 7), newWriter(db, orders.qualified(), 8)
	store := fakestate.New()
	p := startProc(t, db, store, procOpts{mode: snapshot.ModeInitial, chunkRows: 16, parallel: 2, nonce: nonce(1)})
	require.NotNil(t, p.plan.Initial)
	cp := p.plan.Initial.ConsistentPoint
	for range 100 {
		wi.step()
		wo.step()
	}
	p.waitIdle(t, db)
	recs := p.sink.Received()
	for _, table := range []string{items.qualified(), orders.qualified()} {
		assert.Equal(t, db.state(table), finalState(t, recs, table), table)
	}
	reads := readEvents(recs)
	require.Len(t, reads, 200)
	firstStream := len(recs)
	for i, r := range recs {
		if r.Op != change.OpRead {
			firstStream = i
			break
		}
	}
	assert.Equal(t, 200, firstStream, "stream events follow the initial snapshot")
	ids := map[string]bool{}
	for _, r := range reads {
		assert.Equal(t, cp, r.ID.LSN)
		assert.Equal(t, uint64(1), r.ID.Run)
		assert.Equal(t, clockTimestamp.UnixMilli(), r.TsMs)
		assert.False(t, ids[r.ID.String()], "duplicate id %s", r.ID)
		ids[r.ID.String()] = true
	}
	st, _ := store.State()
	assert.Empty(t, st.Snapshot.Runs, "initial run finished")
	assert.Equal(t, cp, st.ConsistentPoint)
	assert.Equal(t, clockTimestamp.UnixMilli(), st.ConsistentPointTsMs)
	rows, _, _ := p.metrics.snapshot()
	assert.Equal(t, uint64(120), rows[items.qualified()])
}

// TestPipeline_InitialSnapshot_CrashResumes kills the process during the
// initial snapshot after some chunks were acknowledged; the restart keeps
// the slot, releases the stream at once and resumes each unfinished table
// from its last acknowledged key through watermarks under the same run_id.
func TestPipeline_InitialSnapshot_CrashResumes(t *testing.T) {
	t.Parallel()
	db := newFakeDB(items, orders)
	db.load(items.qualified(), 100)
	db.load(orders.qualified(), 40)
	store := fakestate.New()
	p1 := startProc(t, db, store, procOpts{
		mode: snapshot.ModeInitial, chunkRows: 10, parallel: 1, nonce: nonce(1), manualAck: true,
	})
	ctx := context.Background()
	// Process 1 emits the whole snapshot but only the first 25 records are
	// acknowledged: chunks 0 and 1 of the first table are complete.
	require.NoError(t, p1.sink.WaitReceived(ctx, 140))
	wi := newWriter(db, items.qualified(), 3)
	for range 50 {
		wi.step()
	}
	_, err := p1.sink.AckNext(25)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		st := p1.keeper.State()
		return len(st.Snapshot.Runs) == 1 && st.Snapshot.Runs[0].Tables[0].Position != nil
	}, testTimeout, pollInterval)
	p1.stop(t)
	st, _ := store.State()
	require.Len(t, st.Snapshot.Runs, 1)
	run := st.Snapshot.Runs[0]
	assert.Equal(t, pipeline.SnapshotKindInitial, run.Kind)
	assert.JSONEq(t, `[20]`, string(run.Tables[0].Position))
	assert.Equal(t, uint64(20), run.Tables[0].Rows)
	assert.Equal(t, st.ConsistentPoint, dbConfirmed(db), "nothing beyond the consistent point confirmed")

	// Process 2: resume.
	p2 := startProc(t, db, store, procOpts{mode: snapshot.ModeInitial, chunkRows: 10, parallel: 1, nonce: nonce(2)})
	assert.Equal(t, snapshot.ActionResume, p2.plan.Decision.Action)
	assert.Nil(t, p2.plan.Initial)
	for range 50 {
		wi.step()
	}
	p2.waitIdle(t, db)
	reads := readEvents(p2.sink.Received())
	require.NotEmpty(t, reads)
	for _, r := range reads {
		assert.Equal(t, run.ID, r.ID.Run, "same run_id")
		assert.NotEqual(t, st.ConsistentPoint, r.ID.LSN, "watermark rows carry their HIGH LSN")
	}
	all := append(p1.sink.Received(), p2.sink.Received()...)
	for _, table := range []string{items.qualified(), orders.qualified()} {
		assert.Equal(t, db.state(table), finalState(t, all, table), table)
	}
	// Rows of the acknowledged chunks are not copied again.
	for _, r := range reads {
		if r.Destination == items.qualified() {
			var payload struct {
				Key struct {
					ID int64 `json:"id"`
				} `json:"key"`
			}
			require.NoError(t, json.Unmarshal(r.Payload, &payload))
			assert.Greater(t, payload.Key.ID, int64(20))
		}
	}
}

func dbConfirmed(db *fakeDB) lsn.LSN {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.confirmed
}
