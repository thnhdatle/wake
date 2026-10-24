// SPDX-License-Identifier: Apache-2.0

package fakespill_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakespill"
)

var (
	_ pipeline.SpillStore = (*fakespill.Store)(nil)
	_ pipeline.SpillFile  = (*fakespill.File)(nil)
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func replayAll(t *testing.T, f pipeline.SpillFile) []string {
	t.Helper()
	var got []string
	require.NoError(t, f.Replay(context.Background(), func(rec []byte) error {
		got = append(got, string(rec))
		return nil
	}))
	return got
}

func TestStore_Create_TracksFiles(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	assert.Empty(t, s.Files())

	for _, name := range []string{"b", "a"} {
		f, err := s.Create(context.Background(), name)
		require.NoError(t, err)
		assert.Zero(t, f.Size())
	}
	assert.Equal(t, []string{"a", "b"}, s.Files())
	created, removed := s.Counts()
	assert.Equal(t, 2, created)
	assert.Zero(t, removed)
}

func TestStore_Create_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(s *fakespill.Store)
		ctx   context.Context //nolint:containedctx // table input
		want  error
	}{
		{"canceled context", func(*fakespill.Store) {}, canceled(), context.Canceled},
		{"injected failure", func(s *fakespill.Store) { s.FailCreate(assert.AnError) }, context.Background(), assert.AnError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := fakespill.New()
			tc.setup(s)
			f, err := s.Create(tc.ctx, "x")
			require.ErrorIs(t, err, tc.want)
			assert.Nil(t, f)
			assert.Empty(t, s.Files())
			created, _ := s.Counts()
			assert.Zero(t, created)
		})
	}
}

func TestStore_FailCreate_NilRestores(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	s.FailCreate(assert.AnError)
	_, err := s.Create(context.Background(), "x")
	require.ErrorIs(t, err, assert.AnError)
	s.FailCreate(nil)
	_, err = s.Create(context.Background(), "x")
	require.NoError(t, err)
}

func TestStore_RemoveAll_DeletesEveryFile(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	s.Seed("b", []byte("x"))
	assert.Zero(t, s.RemoveAlls())

	require.NoError(t, s.RemoveAll(context.Background()))
	assert.Equal(t, 1, s.RemoveAlls())
	assert.Empty(t, s.Files())
	require.ErrorIs(t, f.Append(context.Background(), []byte("y")), fakespill.ErrRemoved)
	require.ErrorIs(t, f.Remove(), fakespill.ErrRemoved)
	_, removed := s.Counts()
	assert.Zero(t, removed, "Counts counts only Remove")
}

func TestStore_RemoveAll_CanceledContext(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	s.Seed("a")
	require.ErrorIs(t, s.RemoveAll(canceled()), context.Canceled)
	assert.Zero(t, s.RemoveAlls())
	assert.Equal(t, []string{"a"}, s.Files())
}

func TestStore_Seed_AddsFileWithoutCreate(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	rec := []byte("abc")
	s.Seed("crashed", rec, []byte("de"))
	s.Seed("empty")
	rec[0] = 'z'
	assert.Equal(t, []string{"crashed", "empty"}, s.Files())
	created, removed := s.Counts()
	assert.Zero(t, created, "seeded files are not created")
	assert.Zero(t, removed)

	require.NoError(t, s.RemoveAll(context.Background()))
	assert.Empty(t, s.Files())
}

func TestFile_Append_StoresCopyAndSize(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	rec := []byte("abc")
	require.NoError(t, f.Append(context.Background(), rec))
	rec[0] = 'z'
	require.NoError(t, f.Append(context.Background(), []byte("de")))
	assert.EqualValues(t, 5, f.Size())
	assert.Equal(t, []string{"abc", "de"}, replayAll(t, f))
}

func TestFile_Append_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(t *testing.T, s *fakespill.Store, f pipeline.SpillFile)
		ctx   context.Context //nolint:containedctx // table input
		want  error
	}{
		{"canceled context", func(*testing.T, *fakespill.Store, pipeline.SpillFile) {}, canceled(), context.Canceled},
		{
			"removed file",
			func(t *testing.T, _ *fakespill.Store, f pipeline.SpillFile) {
				t.Helper()
				require.NoError(t, f.Remove())
			},
			context.Background(), fakespill.ErrRemoved,
		},
		{
			"injected failure",
			func(_ *testing.T, s *fakespill.Store, _ pipeline.SpillFile) { s.FailAppend(assert.AnError) },
			context.Background(), assert.AnError,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := fakespill.New()
			f, err := s.Create(context.Background(), "a")
			require.NoError(t, err)
			tc.setup(t, s, f)
			require.ErrorIs(t, f.Append(tc.ctx, []byte("x")), tc.want)
			assert.Zero(t, f.Size())
		})
	}
}

func TestStore_FailAppend_NilRestores(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	s.FailAppend(assert.AnError)
	require.ErrorIs(t, f.Append(context.Background(), []byte("x")), assert.AnError)
	s.FailAppend(nil)
	require.NoError(t, f.Append(context.Background(), []byte("x")))
	assert.EqualValues(t, 1, f.Size())
}

func TestFile_Replay_OrderAndScratchCopy(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	for _, r := range []string{"one", "two", "three"} {
		require.NoError(t, f.Append(context.Background(), []byte(r)))
	}

	var kept [][]byte
	var seen []string
	require.NoError(t, f.Replay(context.Background(), func(rec []byte) error {
		seen = append(seen, string(rec))
		kept = append(kept, rec)
		return nil
	}))
	assert.Equal(t, []string{"one", "two", "three"}, seen)
	for _, k := range kept {
		for _, b := range k {
			assert.Zero(t, b, "scratch is cleared after the callback")
		}
	}
	assert.Equal(t, seen, replayAll(t, f))
}

func TestFile_Replay_Empty(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	assert.Empty(t, replayAll(t, f))
}

func TestFile_Replay_CallbackErrorStops(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	for _, r := range []string{"one", "two", "three"} {
		require.NoError(t, f.Append(context.Background(), []byte(r)))
	}
	calls := 0
	err = f.Replay(context.Background(), func([]byte) error {
		calls++
		if calls == 2 {
			return assert.AnError
		}
		return nil
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 2, calls)
}

func TestFile_Replay_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(t *testing.T, f pipeline.SpillFile) error
		want error
	}{
		{"canceled context", func(_ *testing.T, f pipeline.SpillFile) error {
			return f.Replay(canceled(), func([]byte) error { return nil })
		}, context.Canceled},
		{"removed file", func(t *testing.T, f pipeline.SpillFile) error {
			t.Helper()
			require.NoError(t, f.Remove())
			return f.Replay(context.Background(), func([]byte) error { return nil })
		}, fakespill.ErrRemoved},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := fakespill.New()
			f, err := s.Create(context.Background(), "a")
			require.NoError(t, err)
			require.NoError(t, f.Append(context.Background(), []byte("x")))
			require.ErrorIs(t, tc.run(t, f), tc.want)
		})
	}
}

func TestFile_Replay_CanceledMidway(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	for _, r := range []string{"one", "two"} {
		require.NoError(t, f.Append(context.Background(), []byte(r)))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err = f.Replay(ctx, func([]byte) error {
		calls++
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}

func TestFile_Remove_Twice(t *testing.T) {
	t.Parallel()
	s := fakespill.New()
	f, err := s.Create(context.Background(), "a")
	require.NoError(t, err)
	require.NoError(t, f.Remove())
	assert.Empty(t, s.Files())
	created, removed := s.Counts()
	assert.Equal(t, 1, created)
	assert.Equal(t, 1, removed)

	err = f.Remove()
	require.ErrorIs(t, err, fakespill.ErrRemoved)
	_, removed = s.Counts()
	assert.Equal(t, 1, removed)
}
