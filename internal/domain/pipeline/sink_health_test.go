// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestSinkHealth(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		state   pipeline.SinkState
		str     string
		healthy bool
		fatal   bool
	}{
		{name: "healthy", state: pipeline.SinkStateHealthy, str: "healthy", healthy: true},
		{name: "retrying", state: pipeline.SinkStateRetrying, str: "retrying"},
		{name: "fatal", state: pipeline.SinkStateFatal, str: "fatal", fatal: true},
		{name: "unknown", state: pipeline.SinkState(9), str: "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := pipeline.SinkHealth{State: tc.state, Code: pipeline.CodeSinkFatal, Since: time.Unix(1, 0)}
			assert.Equal(t, tc.str, tc.state.String())
			assert.Equal(t, tc.healthy, h.IsHealthy())
			assert.Equal(t, tc.fatal, h.IsFatal())
		})
	}
}
