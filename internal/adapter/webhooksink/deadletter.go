// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// DeadLetterPath returns the file a poison batch is written to:
// <dir>/<delivery id>.jsonl.
func DeadLetterPath(dir, deliveryID string) string {
	return filepath.Join(dir, deliveryID+".jsonl")
}

// writeDeadLetter writes one payload per line to a temporary file, fsyncs
// it, renames it to DeadLetterPath and fsyncs the directory, so a crash
// leaves either the complete file or none (a stale *.tmp at most).
func writeDeadLetter(dir, deliveryID string, recs []pipeline.Record) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	final := DeadLetterPath(dir, deliveryID)
	tmp := final + ".tmp"
	if err := writeSynced(tmp, recs); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	return syncDir(dir)
}

func writeSynced(path string, recs []pipeline.Record) error {
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	w := bufio.NewWriter(f)
	for i := range recs {
		_, _ = w.Write(recs[i].Payload) // a write error sticks and Flush returns it
		_ = w.WriteByte('\n')
	}
	err = w.Flush()
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	// Some file systems cannot fsync a directory; the rename is then as
	// durable as they allow.
	if err != nil && !errors.Is(err, os.ErrInvalid) {
		return fmt.Errorf("%w: %w", ErrDeadLetter, err)
	}
	return nil
}
