// SPDX-License-Identifier: Apache-2.0

package statefile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

var errInjected = errors.New("injected failure")

func TestStore_Save_CrashLeavesOldFile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		inject func(o *fileOps)
	}{
		{"fsync of temp file fails", func(o *fileOps) {
			o.syncFile = func(*os.File) error { return errInjected }
		}},
		{"rename fails", func(o *fileOps) {
			o.rename = func(string, string) error { return errInjected }
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.json")
			st, err := New(path)
			require.NoError(t, err)
			old := pipeline.NewState("wake_app", "1")
			old.RunCounter = 1
			require.NoError(t, st.Save(ctx, old))

			tc.inject(&st.ops)
			next := old
			next.RunCounter = 2
			require.ErrorIs(t, st.Save(ctx, next), errInjected)

			got, err := st.Load(ctx)
			require.NoError(t, err)
			assert.Equal(t, uint64(1), got.RunCounter, "old file must survive")
			entries, err := os.ReadDir(filepath.Dir(path))
			require.NoError(t, err)
			assert.Len(t, entries, 1, "temporary file must be removed")
		})
	}
}

func TestStore_Save_DirSyncFailureAfterRename(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := New(filepath.Join(t.TempDir(), "state.json"))
	require.NoError(t, err)
	st.ops.syncDir = func(string) error { return errInjected }

	s := pipeline.NewState("wake_app", "1")
	require.ErrorIs(t, st.Save(ctx, s), errInjected)
	// The rename happened, so the new content is readable, never a mix.
	got, err := st.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, "wake_app", got.SlotName)
}

func TestSyncDir_MissingDir(t *testing.T) {
	t.Parallel()
	require.Error(t, syncDir(filepath.Join(t.TempDir(), "absent")))
	require.NoError(t, syncDir(t.TempDir()))
}
