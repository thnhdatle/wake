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
	headerSize    = 4
	bufSize       = 64 << 10
	MaxRecordSize = math.MaxUint32
)

// File is one spill file of length-prefixed records.
type File struct {
	path  string
	store *Store

	mu      sync.Mutex
	f       *os.File
	w       *bufio.Writer
	written int64
	size    int64
	removed bool
}

var _ pipeline.SpillFile = (*File)(nil)

func newFile(f *os.File, path string, store *Store) *File {
	return &File{path: path, store: store, f: f, w: bufio.NewWriterSize(f, bufSize)}
}

func (f *File) invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.removed {
		f.removed = true
		_ = f.f.Close()
	}
}

func (f *File) Path() string { return f.path }

// Append writes one record to the file.
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

// Replay calls fn for each record in order.
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

func frameError(what string, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: truncated %s", ErrCorrupt, what)
	}
	return fmt.Errorf("read %s: %w", what, err)
}

func (f *File) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.size
}

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
