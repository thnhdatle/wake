// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/schema"
)

func FuzzAppendJSON(f *testing.F) {
	f.Add(schema.OIDInt8, []byte("9223372036854775807"))
	f.Add(schema.OIDTextArray, []byte(`{"a,b","c\"d",NULL}`))
	f.Add(schema.OIDTSTZRange, []byte(`["2026-01-01 00:00:00+00",infinity)`))
	f.Add(schema.OIDInterval, []byte("1 year 2 mons -3 days +04:05:06.5"))
	f.Fuzz(func(t *testing.T, oid uint32, text []byte) {
		got, err := schema.AppendJSON([]byte("x"), oid, text)
		if err != nil {
			if !errors.Is(err, schema.ErrInvalidValue) || string(got) != "x" {
				t.Fatalf("error %v with output %q", err, got)
			}
			return
		}
		if !json.Valid(got[1:]) {
			t.Fatalf("invalid JSON %q for type %d text %q", got[1:], oid, text)
		}
	})
}

func TestFuzzAppendJSON_SeedCorpus(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir(filepath.Join("testdata", "fuzz", "FuzzAppendJSON"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 30, "seed corpus is incomplete")
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join("testdata", "fuzz", "FuzzAppendJSON", e.Name()))
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(string(data), "go test fuzz v1\nuint32("), e.Name())
	}
}
