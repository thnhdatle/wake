// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const fixtureDir = "../../testdata/contracts/cdc"

// fixture returns the bytes of one contract fixture without the trailing newline.
func fixture(tb testing.TB, name string) []byte {
	tb.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, name))
	require.NoError(tb, err)
	for len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	return b
}

// fixtureNames lists all contract fixture file names.
func fixtureNames(tb testing.TB) []string {
	tb.Helper()
	paths, err := filepath.Glob(filepath.Join(fixtureDir, "*.json"))
	require.NoError(tb, err)
	require.NotEmpty(tb, paths)
	names := make([]string, len(paths))
	for i, p := range paths {
		names[i] = filepath.Base(p)
	}
	return names
}

// utoa formats n in decimal.
func utoa(n uint64) string {
	return strconv.FormatUint(n, 10)
}
