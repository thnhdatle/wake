// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"encoding/binary"
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const spillFormat = 1

func appendSpillRecord(dst []byte, rec *pipeline.Record) []byte {
	dst = append(dst, spillFormat, byte(rec.Kind))
	dst = appendSpillString(dst, string(rec.Op))
	dst = binary.BigEndian.AppendUint64(dst, uint64(rec.ID.LSN))
	dst = append(dst, byte(rec.ID.Kind))
	dst = binary.AppendUvarint(dst, rec.ID.Run)
	dst = binary.AppendUvarint(dst, rec.ID.N)
	dst = appendSpillString(dst, rec.Source.DB)
	dst = appendSpillString(dst, rec.Source.Schema)
	dst = appendSpillString(dst, rec.Source.Table)
	dst = appendSpillString(dst, rec.Destination)
	if rec.Key == nil {
		dst = binary.AppendUvarint(dst, 0)
	} else {
		dst = binary.AppendUvarint(dst, uint64(len(rec.Key))+1)
		dst = append(dst, rec.Key...)
	}
	barrier := byte(0)
	if rec.Barrier {
		barrier = 1
	}
	dst = append(dst, barrier)
	dst = binary.AppendUvarint(dst, uint64(len(rec.Payload)))
	dst = append(dst, rec.Payload...)
	dst = binary.AppendUvarint(dst, rec.SchemaVersion)
	return binary.AppendVarint(dst, rec.TsMs)
}

func appendSpillString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

type spillReader struct {
	buf []byte
	err error
}

func (r *spillReader) fail(what string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrSpillCorrupt, what)
	}
}

func (r *spillReader) byte(what string) byte {
	if r.err != nil || len(r.buf) < 1 {
		r.fail(what)
		return 0
	}
	b := r.buf[0]
	r.buf = r.buf[1:]
	return b
}

func (r *spillReader) u64(what string) uint64 {
	if r.err != nil || len(r.buf) < 8 {
		r.fail(what)
		return 0
	}
	v := binary.BigEndian.Uint64(r.buf)
	r.buf = r.buf[8:]
	return v
}

func (r *spillReader) uvarint(what string) uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.fail(what)
		return 0
	}
	r.buf = r.buf[n:]
	return v
}

func (r *spillReader) varint(what string) int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.buf)
	if n <= 0 {
		r.fail(what)
		return 0
	}
	r.buf = r.buf[n:]
	return v
}

func (r *spillReader) bytes(n uint64, what string) []byte {
	if r.err != nil || n > uint64(len(r.buf)) {
		r.fail(what)
		return nil
	}
	b := make([]byte, n)
	copy(b, r.buf)
	r.buf = r.buf[n:]
	return b
}

func (r *spillReader) string(what string) string {
	n := r.uvarint(what)
	return string(r.bytes(n, what))
}

func decodeSpillRecord(data []byte) (pipeline.Record, error) {
	r := &spillReader{buf: data}
	if f := r.byte("format"); r.err == nil && f != spillFormat {
		return pipeline.Record{}, fmt.Errorf("%w: format %d", ErrSpillCorrupt, f)
	}
	var rec pipeline.Record
	rec.Kind = pipeline.RecordKind(r.byte("kind"))
	rec.Op = change.Op(r.string("op"))
	rec.ID.LSN = lsn.LSN(r.u64("id lsn"))
	rec.ID.Kind = change.IDKind(r.byte("id kind"))
	rec.ID.Run = r.uvarint("id run")
	rec.ID.N = r.uvarint("id n")
	rec.Source.DB = r.string("source db")
	rec.Source.Schema = r.string("source schema")
	rec.Source.Table = r.string("source table")
	rec.Destination = r.string("destination")
	if n := r.uvarint("key"); n > 0 {
		rec.Key = r.bytes(n-1, "key")
	}
	rec.Barrier = r.byte("barrier") == 1
	rec.Payload = r.bytes(r.uvarint("payload"), "payload")
	rec.SchemaVersion = r.uvarint("schema version")
	rec.TsMs = r.varint("ts_ms")
	if r.err == nil && len(r.buf) > 0 {
		r.fail("trailing bytes")
	}
	if r.err != nil {
		return pipeline.Record{}, r.err
	}
	return rec, nil
}

func recordSize(rec *pipeline.Record) int64 {
	const overhead = 128
	return int64(len(rec.Payload)+len(rec.Key)+len(rec.Destination)) + overhead
}
