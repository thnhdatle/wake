// SPDX-License-Identifier: Apache-2.0

package spill_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/spill"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakespill"
)

func newStore(t *testing.T) (*spill.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spill")
	st, err := spill.New(dir)
	require.NoError(t, err)
	return st, dir
}

func create(t *testing.T, st pipeline.SpillStore, name string) pipeline.SpillFile {
	t.Helper()
	f, err := st.Create(context.Background(), name)
	require.NoError(t, err)
	return f
}

func appendAll(t *testing.T, f pipeline.SpillFile, recs ...[]byte) {
	t.Helper()
	for _, r := range recs {
		require.NoError(t, f.Append(context.Background(), r))
	}
}

func replayAll(t *testing.T, f pipeline.SpillFile) [][]byte {
	t.Helper()
	var got [][]byte
	require.NoError(t, f.Replay(context.Background(), func(rec []byte) error {
		got = append(got, bytes.Clone(rec))
		return nil
	}))
	return got
}

func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestNew_EmptyDir(t *testing.T) {
	t.Parallel()
	_, err := spill.New("")
	require.ErrorIs(t, err, spill.ErrInvalidDir)
}

func TestStore_Create_Name(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		file    string
		wantErr bool
	}{
		{"transaction name", "0000000001977818-760", false},
		{"dots inside", "a.b", false},
		{"empty", "", true},
		{"dot", ".", true},
		{"dot dot", "..", true},
		{"slash", "a/b", true},
		{"parent traversal", "../x", true},
		{"backslash", `a\b`, true},
		{"absolute", "/etc/passwd", true},
		{"nul", "a\x00b", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, dir := newStore(t)
			f, err := st.Create(context.Background(), tc.file)
			if tc.wantErr {
				require.ErrorIs(t, err, spill.ErrInvalidName)
				assert.Nil(t, f)
				return
			}
			require.NoError(t, err)
			_, statErr := os.Stat(filepath.Join(dir, tc.file+".spill"))
			require.NoError(t, statErr)
		})
	}
}

func TestStore_Create_FileAndDir(t *testing.T) {
	t.Parallel()
	st, dir := newStore(t)
	assert.Equal(t, dir, st.Dir())
	f := create(t, st, "a")
	sf, ok := f.(*spill.File)
	require.True(t, ok)
	assert.Equal(t, filepath.Join(dir, "a.spill"), sf.Path())
	assert.Zero(t, f.Size())
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(sf.Path())
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
	}
}

func TestStore_Create_Errors(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	_, err := st.Create(canceled(), "a")
	require.ErrorIs(t, err, context.Canceled)

	blocker := filepath.Join(t.TempDir(), "blocker")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	bad, err := spill.New(filepath.Join(blocker, "spill"))
	require.NoError(t, err)
	_, err = bad.Create(context.Background(), "a")
	require.Error(t, err)
	require.Error(t, bad.RemoveAll(context.Background()))
}

func TestStore_Create_TruncatesExisting(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "a")
	appendAll(t, f, []byte("old"))
	require.NoError(t, f.Replay(context.Background(), func([]byte) error { return nil }))

	g := create(t, st, "a")
	assert.Empty(t, replayAll(t, g))
}

func TestFile_AppendReplay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		recs [][]byte
	}{
		{"none", nil},
		{"one", [][]byte{[]byte("abc")}},
		{"empty records", [][]byte{{}, []byte("x"), {}}},
		{"binary with prefix-like bytes", [][]byte{{0, 0, 0, 5}, {0xff, 0xff, 0xff, 0xff}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := newStore(t)
			f := create(t, st, "tx")
			appendAll(t, f, tc.recs...)
			var want int64
			for _, r := range tc.recs {
				want += int64(len(r))
			}
			assert.Equal(t, want, f.Size())
			got := replayAll(t, f)
			require.Len(t, got, len(tc.recs))
			for i := range tc.recs {
				assert.True(t, bytes.Equal(tc.recs[i], got[i]), "record %d", i)
			}
		})
	}
}

func TestFile_LargeFrame(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "big")
	big := bytes.Repeat([]byte{0xab, 0xcd, 0xef}, 3<<20)
	appendAll(t, f, []byte("before"), big, []byte("after"))
	got := replayAll(t, f)
	require.Len(t, got, 3)
	assert.Equal(t, []byte("before"), got[0])
	assert.True(t, bytes.Equal(big, got[1]))
	assert.Equal(t, []byte("after"), got[2])
	assert.Equal(t, int64(len(big)+len("before")+len("after")), f.Size())
}

func TestFile_ManyFrames(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "many")
	const n = 20_000
	for i := range n {
		appendAll(t, f, fmt.Appendf(nil, "record-%d", i))
	}
	var count int
	require.NoError(t, f.Replay(context.Background(), func(rec []byte) error {
		assert.Equal(t, fmt.Sprintf("record-%d", count), string(rec))
		count++
		return nil
	}))
	assert.Equal(t, n, count)
}

func TestFile_ReplayRepeatableAndAppendAfter(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "a")
	appendAll(t, f, []byte("1"), []byte("2"))
	assert.Len(t, replayAll(t, f), 2)
	assert.Len(t, replayAll(t, f), 2)
	appendAll(t, f, []byte("3"))
	assert.Equal(t, [][]byte{[]byte("1"), []byte("2"), []byte("3")}, replayAll(t, f))
}

func TestFile_Replay_StopsOnCallbackError(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "a")
	appendAll(t, f, []byte("1"), []byte("2"), []byte("3"))
	var seen int
	err := f.Replay(context.Background(), func([]byte) error {
		seen++
		if seen == 2 {
			return assert.AnError
		}
		return nil
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 2, seen)
}

func TestFile_Replay_CanceledContext(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "a")
	appendAll(t, f, []byte("1"))
	err := f.Replay(canceled(), func([]byte) error {
		require.Fail(t, "fn must not run")
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, f.Append(canceled(), []byte("x")), context.Canceled)
	assert.Equal(t, int64(1), f.Size())
}

func TestFile_Replay_Corrupt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{"body truncated", func(t *testing.T, path string) {
			t.Helper()
			fi, err := os.Stat(path)
			require.NoError(t, err)
			require.NoError(t, os.Truncate(path, fi.Size()-2))
		}},
		{"header truncated", func(t *testing.T, path string) {
			t.Helper()
			fi, err := os.Stat(path)
			require.NoError(t, err)
			require.NoError(t, os.Truncate(path, fi.Size()-int64(len("last"))-2))
		}},
		{"file emptied", func(t *testing.T, path string) {
			t.Helper()
			require.NoError(t, os.Truncate(path, 0))
		}},
		{"length prefix beyond file", func(t *testing.T, path string) {
			t.Helper()
			fh, err := os.OpenFile(path, os.O_WRONLY, 0o600) //nolint:gosec // test path
			require.NoError(t, err)
			defer func() { require.NoError(t, fh.Close()) }()
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], 1<<31)
			_, err = fh.WriteAt(hdr[:], 0)
			require.NoError(t, err)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, dir := newStore(t)
			f := create(t, st, "a")
			appendAll(t, f, []byte("first"), []byte("last"))
			require.Len(t, replayAll(t, f), 2)
			tc.damage(t, filepath.Join(dir, "a.spill"))

			err := f.Replay(context.Background(), func([]byte) error { return nil })
			require.ErrorIs(t, err, spill.ErrCorrupt)
		})
	}
}

func TestFile_Remove(t *testing.T) {
	t.Parallel()
	st, dir := newStore(t)
	f := create(t, st, "a")
	appendAll(t, f, []byte("x"))
	require.NoError(t, f.Remove())
	_, err := os.Stat(filepath.Join(dir, "a.spill"))
	require.ErrorIs(t, err, os.ErrNotExist)

	require.ErrorIs(t, f.Remove(), spill.ErrRemoved)
	require.ErrorIs(t, f.Append(context.Background(), []byte("y")), spill.ErrRemoved)
	require.ErrorIs(t, f.Replay(context.Background(), func([]byte) error { return nil }), spill.ErrRemoved)
}

func TestFile_AfterRemoveAll(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "a")
	require.NoError(t, st.RemoveAll(context.Background()))
	require.ErrorIs(t, f.Append(context.Background(), []byte("x")), spill.ErrRemoved)
	require.ErrorIs(t, f.Remove(), spill.ErrRemoved)
}

func TestFile_Remove_DeletedBehindItsBack(t *testing.T) {
	t.Parallel()
	st, dir := newStore(t)
	f := create(t, st, "a")
	require.NoError(t, os.Remove(filepath.Join(dir, "a.spill")))
	require.NoError(t, f.Remove())
}

func TestStore_RemoveAll(t *testing.T) {
	t.Parallel()
	st, dir := newStore(t)
	appendAll(t, create(t, st, "a"), []byte("x"))
	create(t, st, "0000000001977818-760")
	keep := filepath.Join(dir, "keep.txt")
	require.NoError(t, os.WriteFile(keep, []byte("k"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dir.spill"), 0o700))

	require.NoError(t, st.RemoveAll(context.Background()))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.ElementsMatch(t, []string{"keep.txt", "dir.spill"}, names)
}

func TestStore_RemoveAll_CreatesDir(t *testing.T) {
	t.Parallel()
	st, dir := newStore(t)
	require.NoError(t, st.RemoveAll(context.Background()))
	fi, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, fi.IsDir())
	require.NoError(t, st.RemoveAll(context.Background()), "idempotent")
}

func TestStore_RemoveAll_CanceledContext(t *testing.T) {
	t.Parallel()
	st, dir := newStore(t)
	create(t, st, "a")
	require.ErrorIs(t, st.RemoveAll(canceled()), context.Canceled)
	_, err := os.Stat(filepath.Join(dir, "a.spill"))
	require.NoError(t, err)
}

func TestStore_ConcurrentIndependentFiles(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	const files, recs = 8, 500
	var wg sync.WaitGroup
	for i := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := st.Create(context.Background(), fmt.Sprintf("tx-%d", i))
			if !assert.NoError(t, err) {
				return
			}
			for j := range recs {
				assert.NoError(t, f.Append(context.Background(), fmt.Appendf(nil, "%d:%d", i, j)))
			}
			var j int
			assert.NoError(t, f.Replay(context.Background(), func(rec []byte) error {
				assert.Equal(t, fmt.Sprintf("%d:%d", i, j), string(rec))
				j++
				return nil
			}))
			assert.Equal(t, recs, j)
			assert.NoError(t, f.Remove())
		}()
	}
	wg.Wait()
}

func TestFile_ConcurrentAppendWhileReplay(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	f := create(t, st, "a")
	appendAll(t, f, []byte("seed"))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			assert.NoError(t, f.Append(context.Background(), []byte("more")))
		}
	}()
	for range 20 {
		require.NoError(t, f.Replay(context.Background(), func([]byte) error { return nil }))
	}
	wg.Wait()
	assert.Len(t, replayAll(t, f), 201)
}

func TestParityWithFake(t *testing.T) {
	t.Parallel()
	disk, _ := newStore(t)
	for name, st := range map[string]pipeline.SpillStore{"real": disk, "fake": fakespill.New()} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			f := create(t, st, "0000000001977818-760")
			assert.Zero(t, f.Size())
			appendAll(t, f, []byte("one"), []byte{}, []byte("three"))
			assert.Equal(t, int64(8), f.Size())
			assert.Equal(t, [][]byte{[]byte("one"), {}, []byte("three")}, replayAll(t, f))

			var seen int
			err := f.Replay(ctx, func([]byte) error { seen++; return assert.AnError })
			require.ErrorIs(t, err, assert.AnError)
			assert.Equal(t, 1, seen)

			require.ErrorIs(t, f.Append(canceled(), []byte("x")), context.Canceled)
			assert.Equal(t, int64(8), f.Size())
			_, err = st.Create(canceled(), "z")
			require.ErrorIs(t, err, context.Canceled)

			require.NoError(t, f.Remove())
			require.Error(t, f.Remove())
			require.Error(t, f.Append(ctx, []byte("x")))

			g := create(t, st, "b")
			require.NoError(t, st.RemoveAll(ctx))
			require.Error(t, g.Append(ctx, []byte("x")), "removed by RemoveAll")
		})
	}
}
