// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	headerSize = 4
	bufSize    = 64 << 10
	// MaxRecordSize is the largest record a frame can hold.
	MaxRecordSize = math.MaxUint32
)

// File is one spill file. Append, Replay, Size and Remove are safe for
// concurrent use; Replay reads the frames appended before it started and
// does not hold the file lock while it calls fn.
type File struct {
	path  string
	store *Store

	mu      sync.Mutex // protects every field below
	f       *os.File
	w       *bufio.Writer
	written int64 // bytes in the file including frame headers
	size    int64 // record bytes appended, the value of Size
	removed bool
}

var _ pipeline.SpillFile = (*File)(nil)

func newFile(f *os.File, path string, store *Store) *File {
	return &File{path: path, store: store, f: f, w: bufio.NewWriterSize(f, bufSize)}
}

// invalidate closes the file after the store deleted it.
func (f *File) invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.removed {
		f.removed = true
		_ = f.f.Close()
	}
}

// Path returns the file's path.
func (f *File) Path() string { return f.path }

// Append writes rec as one frame. The bytes may stay buffered until Replay.
func (f *File) Append(ctx context.Context, rec []byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("append to %s: %w", f.path, err)
	}
	if uint64(len(rec)) > MaxRecordSize {
		return fmt.Errorf("%w: %d bytes", ErrRecordTooLarge, len(rec))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removed {
		return ErrRemoved
	}
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(rec))) //nolint:gosec // bounded by MaxRecordSize above
	if _, err := f.w.Write(hdr[:]); err != nil {
		return fmt.Errorf("append to %s: %w", f.path, err)
	}
	if _, err := f.w.Write(rec); err != nil {
		return fmt.Errorf("append to %s: %w", f.path, err)
	}
	f.written += headerSize + int64(len(rec))
	f.size += int64(len(rec))
	return nil
}

// Replay flushes pending frames and calls fn with every record in append
// order, stopping at the first error from fn, which it returns. The slice
// passed to fn is only valid during the call. A truncated or oversized frame
// returns ErrCorrupt.
func (f *File) Replay(ctx context.Context, fn func(rec []byte) error) error {
	f.mu.Lock()
	if f.removed {
		f.mu.Unlock()
		return ErrRemoved
	}
	err := f.w.Flush()
	limit, fh := f.written, f.f
	f.mu.Unlock()
	if err != nil {
		return fmt.Errorf("flush %s: %w", f.path, err)
	}

	r := bufio.NewReaderSize(io.NewSectionReader(fh, 0, limit), bufSize)
	var buf []byte
	remaining := limit
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("replay %s: %w", f.path, err)
		}
		var rec []byte
		var err error
		rec, buf, err = readFrame(r, buf, remaining)
		if err != nil {
			return fmt.Errorf("replay %s: %w", f.path, err)
		}
		remaining -= headerSize + int64(len(rec))
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// readFrame reads one frame, reusing buf for the record, given the bytes
// that remain in the file.
func readFrame(r io.Reader, buf []byte, remaining int64) (rec, grown []byte, err error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, buf, frameError("frame header", err)
	}
	n := int64(binary.BigEndian.Uint32(hdr[:]))
	if n > remaining-headerSize {
		return nil, buf, fmt.Errorf("%w: frame of %d bytes exceeds the %d bytes left",
			ErrCorrupt, n, remaining-headerSize)
	}
	if int64(cap(buf)) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, buf, frameError("frame body", err)
	}
	return buf, buf, nil
}

// frameError reports a short read as corruption (the file is shorter than
// its own bookkeeping) and any other read error as is.
func frameError(what string, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: truncated %s", ErrCorrupt, what)
	}
	return fmt.Errorf("read %s: %w", what, err)
}

// Size returns the record bytes appended so far, without frame headers.
func (f *File) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.size
}

// Remove closes and deletes the file. A second call returns ErrRemoved.
func (f *File) Remove() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removed {
		return ErrRemoved
	}
	f.removed = true
	f.store.forget(f)
	closeErr := f.f.Close()
	rmErr := os.Remove(f.path)
	if rmErr != nil && !os.IsNotExist(rmErr) {
		return fmt.Errorf("remove spill file %s: %w", f.path, rmErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close spill file %s: %w", f.path, closeErr)
	}
	return nil
}
