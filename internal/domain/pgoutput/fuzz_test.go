// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
)

func fuzzRelations(f *testing.F) [][]byte {
	f.Helper()
	var out [][]byte
	paths, err := filepath.Glob(filepath.Join(recordedDir, "*.hex"))
	if err != nil {
		f.Fatal(err)
	}
	var all [][]byte
	for _, p := range paths {
		all = append(all, readRecorded(f, p)...)
	}
	for _, msgs := range synthesizedScenarios() {
		all = append(all, msgs...)
	}
	for _, m := range all {
		if len(m) > 0 && (m[0] == 'R' || m[0] == 'Y') {
			out = append(out, m)
		}
	}
	return out
}

func FuzzDecode(f *testing.F) {
	relations := fuzzRelations(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		dec := pgoutput.NewDecoder()
		for _, r := range relations {
			if _, err := dec.Decode(r); err != nil {
				t.Fatalf("seed relation: %v", err)
			}
		}
		m, err := dec.Decode(data)
		if err != nil {
			var ue *pgoutput.UnsupportedMessageError
			if !errors.Is(err, pgoutput.ErrMalformed) && !errors.As(err, &ue) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		img, err := dec.RowImages(m)
		if err != nil {
			return
		}
		ev := change.Event{Op: img.Op, Key: img.Key, Before: img.Before, After: img.After, Unchanged: img.Unchanged}
		if out := ev.AppendJSON(nil); !json.Valid(out) {
			t.Fatalf("invalid event JSON %s", out)
		}
	})
}
