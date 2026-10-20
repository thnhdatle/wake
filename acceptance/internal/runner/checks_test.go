// SPDX-License-Identifier: Apache-2.0

package runner_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/runner"
)

func entries() []*harness.Entry {
	return []*harness.Entry{
		{ID: "X-01", Test: "TestAcceptance_X01_A"},
		{ID: "X-02", Test: "BenchmarkAcceptance_X02_B"},
	}
}

func writeSrc(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
}

func TestCheckCriteria(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "all mapped",
			files: map[string]string{
				"a_test.go": "package a\nfunc TestAcceptance_X01_A(t *testing.T) {}\n",
				"b_test.go": "package a\nfunc BenchmarkAcceptance_X02_B(b *testing.B) {}\n",
			},
		},
		{
			name:  "missing both",
			files: map[string]string{"a_test.go": "package a\n"},
			want: []string{
				"X-01: expected exactly one test named TestAcceptance_X01_A, found 0",
				"X-02: expected exactly one test named BenchmarkAcceptance_X02_B, found 0",
			},
		},
		{
			name: "duplicate and orphan",
			files: map[string]string{
				"a_test.go": "package a\nfunc TestAcceptance_X01_A(t *testing.T) {}\nfunc TestAcceptance_X01_A(t *testing.T) {}\n",
				"b_test.go": "package a\nfunc BenchmarkAcceptance_X02_B(b *testing.B) {}\n" +
					"func TestAcceptance_X03_C(t *testing.T) {}\n",
			},
			want: []string{
				"X-01: expected exactly one test named TestAcceptance_X01_A, found 2",
				"TestAcceptance_X03_C has no criterion in criteria.yaml",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			for n, body := range tc.files {
				writeSrc(t, dir, n, body)
			}
			got, err := runner.CheckCriteria(dir, entries())
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestCheckCriteria_NameWithoutID(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeSrc(t, dir, "a_test.go", "package a\nfunc TestAcceptance_Other(t *testing.T) {}\n")
	got, err := runner.CheckCriteria(dir, []*harness.Entry{{ID: "X-01", Test: "TestAcceptance_Other"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"X-01: test name TestAcceptance_Other does not contain the id"}, got)
}

func TestCheckSkips(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		src  string
		want int
	}{
		{"clean", "package a\nfunc TestX(t *testing.T) { t.Fatal(1) }\n", 0},
		{"t.Skip", "package a\nfunc TestX(t *testing.T) {\n\tt.Skip(\"x\")\n}\n", 1},
		{"Skipf", "package a\nfunc TestX(t *testing.T) { t.Skipf(\"x\") }\n", 1},
		{"SkipNow", "package a\nfunc TestX(t *testing.T) { t.SkipNow() }\n", 1},
		{"Short", "package a\nvar _ = testing.Short()\n", 1},
		{"python markers", "// pytest.mark.skip\n// pytest.skip\n// xfail\n", 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeSrc(t, dir, "a_test.go", tc.src)
			// nested files are not scanned
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "internal"), 0o750))
			writeSrc(t, filepath.Join(dir, "internal"), "x.go", "package x\n// t.Skip xfail\n")
			got, err := runner.CheckSkips(dir)
			require.NoError(t, err)
			assert.Len(t, got, tc.want)
		})
	}
}
