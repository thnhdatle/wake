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

// fuzzRelations returns every Relation and Type message of the recorded and
// synthesized inputs, so fuzzed row messages reach RowImages.
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

// FuzzDecode feeds arbitrary bytes to a Decoder that knows the relations of
// the golden inputs and converts row messages to images and CDC events. It
// must never panic; errors must be classified, and produced events must be
// valid JSON. Seeds: testdata/fuzz/FuzzDecode (every recorded and
// synthesized message, regenerated with -update).
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
