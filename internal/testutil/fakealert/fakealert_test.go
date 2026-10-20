// SPDX-License-Identifier: Apache-2.0

package fakealert_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakealert"
)

func TestNotifier_Notify(t *testing.T) {
	t.Parallel()
	n := fakealert.New()
	a := pipeline.Alert{Rule: "max_ack_age", State: pipeline.AlertStateFiring, Value: 400, Threshold: 300}
	require.NoError(t, n.Notify(context.Background(), a))
	n.Fail(assert.AnError)
	require.ErrorIs(t, n.Notify(context.Background(), a), assert.AnError)
	n.Fail(nil)
	assert.Equal(t, []pipeline.Alert{a}, n.Alerts())
}
