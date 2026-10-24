// SPDX-License-Identifier: Apache-2.0

package pipelinetest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

var errNoSlot = errors.New("fakedb: slot does not exist")

const pgEpochUnixMs = 946_684_800_000

type dbTable struct {
	oid    uint32
	schema string
	name   string
}

func (t dbTable) qualified() string { return t.schema + "." + t.name }

type rows map[string]map[int64]string

func (r rows) clone() rows {
	c := make(rows, len(r))
	for k, v := range r {
		c[k] = maps.Clone(v)
	}
	return c
}

func (r rows) apply(ops []op) {
	for _, o := range ops {
		t := r[o.table]
		switch o.kind {
		case 'I', 'U':
			t[o.id] = o.v
		case 'K':
			delete(t, o.oldID)
			t[o.id] = o.v
		case 'D':
			delete(t, o.id)
		}
	}
}

type op struct {
	kind  byte
	table string
	id    int64
	oldID int64
	v     string
}

type walRecord struct {
	at     lsn.LSN
	commit lsn.LSN
	data   []byte
}

type pendingTx struct {
	xid uint64
	ops []op
}

type fakeDB struct {
	mu        sync.Mutex
	tables    map[string]dbTable
	visible   rows
	invisible []pendingTx
	wal       []walRecord
	next      lsn.LSN
	nextXID   uint64
	tsMs      int64
	walGrew   chan struct{}

	slotExists bool
	confirmed  lsn.LSN
	exported   map[string]rows
	cursor     int
	started    bool
	reads      int
	onRead     func(n int)
	emitted    []string
}

func newFakeDB(tables ...dbTable) *fakeDB {
	db := &fakeDB{
		tables: map[string]dbTable{}, visible: rows{}, next: 0x1000000, nextXID: 1000, tsMs: 1_800_000_000_000,
		walGrew: make(chan struct{}), exported: map[string]rows{},
	}
	for _, t := range tables {
		db.tables[t.qualified()] = t
		db.visible[t.qualified()] = map[int64]string{}
	}
	return db
}

func (db *fakeDB) load(table string, to int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	for id := int64(1); id <= to; id++ {
		db.visible[table][id] = "v" + strconv.FormatInt(id, 10)
	}
}

func (db *fakeDB) commit(ops ...op) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.commitLocked(ops, true)
}

func (db *fakeDB) commitInvisible(ops ...op) uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.commitLocked(ops, false)
}

func (db *fakeDB) publish() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, p := range db.invisible {
		db.visible.apply(p.ops)
	}
	db.invisible = nil
}

func (db *fakeDB) commitLocked(ops []op, visible bool) uint64 {
	db.nextXID++
	xid := db.nextXID
	db.tsMs++
	msgs := [][]byte{nil}
	sent := map[string]bool{}
	for _, o := range ops {
		t := db.tables[o.table]
		if !sent[o.table] {
			sent[o.table] = true
			msgs = append(msgs, encodeRelation(t))
		}
		msgs = append(msgs, encodeOp(t, o))
	}
	start := db.next
	commitAt := start + lsn.LSN(len(msgs)*0x10)
	msgs[0] = wire{'B'}.u64(uint64(commitAt)).pgTime(db.tsMs).u32(uint32(xid))
	msgs = append(msgs, wire{'C'}.u8(0).u64(uint64(commitAt)).u64(uint64(commitAt+8)).pgTime(db.tsMs))
	for i, m := range msgs {
		db.wal = append(db.wal, walRecord{at: start + lsn.LSN(i*0x10), commit: commitAt, data: m})
	}
	db.next = commitAt + 0x10
	if visible {
		db.visible.apply(ops)
	} else {
		db.invisible = append(db.invisible, pendingTx{xid: xid, ops: ops})
	}
	close(db.walGrew)
	db.walGrew = make(chan struct{})
	return xid
}

func (db *fakeDB) EmitMessage(ctx context.Context, prefix string, content []byte) (lsn.LSN, error) {
	if err := ctx.Err(); err != nil {
		return lsn.InvalidLSN, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.nextXID++
	db.tsMs++
	xid := db.nextXID
	start := db.next
	commitAt := start + 0x20
	msgs := [][]byte{
		wire{'B'}.u64(uint64(commitAt)).pgTime(db.tsMs).u32(uint32(xid)),
		append(wire{'M'}.u8(1).u64(uint64(start+0x10)).cstr(prefix).u32(uint32(len(content))), content...),
		wire{'C'}.u8(0).u64(uint64(commitAt)).u64(uint64(commitAt + 8)).pgTime(db.tsMs),
	}
	for i, m := range msgs {
		db.wal = append(db.wal, walRecord{at: start + lsn.LSN(i*0x10), commit: commitAt, data: m})
	}
	db.next = commitAt + 0x10
	db.emitted = append(db.emitted, prefix)
	close(db.walGrew)
	db.walGrew = make(chan struct{})
	return start + 0x10, nil
}

func (db *fakeDB) walEnd() lsn.LSN {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.wal) == 0 {
		return lsn.InvalidLSN
	}
	return db.wal[len(db.wal)-1].commit + 8
}

func (db *fakeDB) state(table string) map[int64]string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return maps.Clone(db.visible[table])
}

func (db *fakeDB) IdentifySystem(context.Context) (pipeline.SystemInfo, error) {
	return pipeline.SystemInfo{SystemID: "7300000000000000001", Timeline: 1, DBName: "app"}, nil
}

func (db *fakeDB) CreateSlot(_ context.Context, name string) (pipeline.SlotInfo, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.slotExists = true
	db.confirmed = db.next
	snap := "snap-" + strconv.Itoa(len(db.exported)+1)
	db.exported[snap] = db.visible.clone()
	return pipeline.SlotInfo{Name: name, ConsistentPoint: db.next, SnapshotName: snap}, nil
}

func (db *fakeDB) DropSlot(context.Context, string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.slotExists = false
	return nil
}

func (db *fakeDB) StartReplication(_ context.Context, opts pipeline.StartOptions) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.slotExists {
		return errNoSlot
	}
	db.cursor = len(db.wal)
	for i, r := range db.wal {
		if r.commit >= opts.Start {
			db.cursor = i
			break
		}
	}
	db.started = true
	return nil
}

func (db *fakeDB) Receive(ctx context.Context) (pipeline.ReplicationMessage, error) {
	for {
		db.mu.Lock()
		if db.cursor < len(db.wal) {
			r := db.wal[db.cursor]
			db.cursor++
			now := time.UnixMilli(db.tsMs)
			db.mu.Unlock()
			return pipeline.ReplicationMessage{Kind: pipeline.ReplicationMessageXLogData, XLogData: pipeline.XLogData{
				WALStart: r.at, ServerWALEnd: r.at, ServerTime: now, Data: r.data,
			}}, nil
		}
		grew := db.walGrew
		db.mu.Unlock()
		select {
		case <-grew:
		case <-ctx.Done():
			return pipeline.ReplicationMessage{}, ctx.Err()
		}
	}
}

func (db *fakeDB) SendStandbyStatus(_ context.Context, st pipeline.StandbyStatus) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.confirmed = max(db.confirmed, st.Flushed)
	return nil
}

func (db *fakeDB) ServerSettings(context.Context) (pipeline.ServerSettings, error) {
	return pipeline.ServerSettings{ServerVersionNum: 170002, WALLevel: "logical"}, nil
}

func (db *fakeDB) Slot(context.Context, string) (pipeline.SlotStatus, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.slotExists {
		return pipeline.SlotStatus{}, nil
	}
	return pipeline.SlotStatus{Exists: true, Plugin: "pgoutput", ConfirmedFlush: db.confirmed, WALStatus: "reserved"}, nil
}

func (db *fakeDB) PublicationTables(context.Context, []string) ([]pipeline.TableInfo, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make([]pipeline.TableInfo, 0, len(db.tables))
	for _, name := range slices.Sorted(maps.Keys(db.tables)) {
		t := db.tables[name]
		out = append(out, pipeline.TableInfo{Schema: t.schema, Table: t.name, OID: t.oid, HasKey: true})
	}
	return out, nil
}

var clockTimestamp = time.UnixMilli(1_790_000_000_000).UTC()

func (db *fakeDB) CurrentTime(context.Context) (time.Time, error) { return clockTimestamp, nil }

func (db *fakeDB) ReadChunk(_ context.Context, req pipeline.ChunkRequest) (pipeline.Chunk, error) {
	db.mu.Lock()
	c, err := readChunk(db.visible, req)
	xmax := db.nextXID + 1
	snap := pipeline.TxSnapshot{XMin: xmax, XMax: xmax}
	for _, p := range db.invisible {
		snap.XIP = append(snap.XIP, p.xid)
		c.CommittedXIP = append(c.CommittedXIP, p.xid)
	}
	if len(snap.XIP) > 0 {
		snap.XMin = snap.XIP[0]
	}
	c.Snapshot = snap
	db.reads++
	n, hook := db.reads, db.onRead
	db.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return c, err
}

func (db *fakeDB) OpenExported(_ context.Context, name string) (pipeline.ChunkSession, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	r, ok := db.exported[name]
	if !ok {
		return nil, fmt.Errorf("fakedb: no exported snapshot %q", name)
	}
	return session{rows: r}, nil
}

type session struct{ rows rows }

func (s session) ReadChunk(_ context.Context, req pipeline.ChunkRequest) (pipeline.Chunk, error) {
	return readChunk(s.rows, req)
}

func (session) Close(context.Context) error { return nil }

func readChunk(r rows, req pipeline.ChunkRequest) (pipeline.Chunk, error) {
	after := int64(-1)
	if req.After != nil {
		var pos []int64
		if err := json.Unmarshal(req.After, &pos); err != nil || len(pos) != 1 {
			return pipeline.Chunk{}, fmt.Errorf("fakedb: position %s", req.After)
		}
		after = pos[0]
	}
	t := r[req.Schema+"."+req.Table]
	c := pipeline.Chunk{Last: req.After, Done: true}
	for _, id := range slices.Sorted(maps.Keys(t)) {
		if id <= after {
			continue
		}
		if len(c.Rows) == req.Limit {
			c.Done = false
			break
		}
		idJSON := []byte(strconv.FormatInt(id, 10))
		c.Rows = append(c.Rows, pipeline.ChunkRow{
			Key:   change.Row{{Name: "id", Value: idJSON}},
			After: change.Row{{Name: "id", Value: idJSON}, {Name: "v", Value: change.AppendString(nil, t[id])}},
		})
		c.Last = json.RawMessage("[" + string(idJSON) + "]")
	}
	return c, nil
}

type wire []byte

func (w wire) u8(v byte) wire     { return append(w, v) }
func (w wire) u16(v uint16) wire  { return binary.BigEndian.AppendUint16(w, v) }
func (w wire) u32(v uint32) wire  { return binary.BigEndian.AppendUint32(w, v) }
func (w wire) u64(v uint64) wire  { return binary.BigEndian.AppendUint64(w, v) }
func (w wire) cstr(s string) wire { return append(append(w, s...), 0) }

func (w wire) pgTime(tsMs int64) wire {
	return w.u64(uint64((tsMs - pgEpochUnixMs) * 1000)) //nolint:gosec // G115: wire Int64 as uint64
}

func (w wire) text(vals ...string) wire {
	w = w.u16(uint16(len(vals))) //nolint:gosec // G115: two columns
	for _, v := range vals {
		w = append(w.u8('t').u32(uint32(len(v))), v...) //nolint:gosec // G115: small values
	}
	return w
}

func encodeRelation(t dbTable) []byte {
	return wire{'R'}.u32(t.oid).cstr(t.schema).cstr(t.name).u8('d').u16(2).
		u8(1).cstr("id").u32(schema.OIDInt4).u32(0xFFFFFFFF).
		u8(0).cstr("v").u32(schema.OIDText).u32(0xFFFFFFFF)
}

func encodeOp(t dbTable, o op) []byte {
	id, v := strconv.FormatInt(o.id, 10), o.v
	switch o.kind {
	case 'I':
		return wire{'I'}.u32(t.oid).u8('N').text(id, v)
	case 'U':
		return wire{'U'}.u32(t.oid).u8('N').text(id, v)
	case 'K':
		oldID := strconv.FormatInt(o.oldID, 10)
		old := wire{'U'}.u32(t.oid).u8('K').u16(2).u8('t').u32(uint32(len(oldID))) //nolint:gosec // small
		old = append(old, oldID...)
		return old.u8('n').u8('N').text(id, v)
	default:
		w := wire{'D'}.u32(t.oid).u8('K').u16(2).u8('t').u32(uint32(len(id))) //nolint:gosec // small
		return append(w, id...).u8('n')
	}
}
