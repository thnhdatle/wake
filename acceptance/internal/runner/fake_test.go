// SPDX-License-Identifier: Apache-2.0

package runner_test

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/thnhdatle/wake/acceptance/internal/runner"
)

// fakeExec records commands and answers them through a handler.
type fakeExec struct {
	mu      sync.Mutex
	calls   []string
	handler func(c runner.Cmd) (runner.Output, error)
}

func (f *fakeExec) Run(_ context.Context, c runner.Cmd) (runner.Output, error) {
	f.mu.Lock()
	f.calls = append(f.calls, c.String())
	f.mu.Unlock()
	if f.handler == nil {
		return runner.Output{}, nil
	}
	return f.handler(c)
}

func (f *fakeExec) called(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

var errBoom = errors.New("boom")
