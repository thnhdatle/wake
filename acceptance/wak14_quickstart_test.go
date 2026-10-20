// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
)

// wak14Timeout bounds the whole script: clone, README commands (bounded by
// duration_s_max inside the script) and teardown.
const wak14Timeout = 15 * time.Minute

var wak14Duration = regexp.MustCompile(`quickstart-test: duration_s=(\d+) exit=(\d+)`)

// TestAcceptance_WAK14_Quickstart implements WAK-14: Quickstart: README brings up Postgres, Kafka and Wake;
// an insert appears in the topic.
func TestAcceptance_WAK14_Quickstart(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-14"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	th := readThresholds(t, id, c, "duration_s_max", "success_min")
	root, err := stack.Root()
	failIf(t, id, err, "repository root")

	// scripts/quickstart_test.sh clones HEAD into a temp dir, removes a
	// running stack of the compose project, runs the README commands
	// verbatim with duration_s_max as their limit and removes the stack.
	ctx, cancel := context.WithTimeout(context.Background(), wak14Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(root, "scripts", "quickstart_test.sh"))
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "QUICKSTART_TIMEOUT="+strconv.FormatInt(th["duration_s_max"], 10))
	start := time.Now()
	out, runErr := cmd.CombinedOutput()
	total := time.Since(start)
	t.Logf("%s", out)

	text := string(out)
	duration := int64(-1)
	if m := wak14Duration.FindStringSubmatch(text); m != nil {
		duration, _ = strconv.ParseInt(m[1], 10, 64)
	}
	success := 0
	if runErr == nil && strings.Contains(text, "quickstart-test: PASS") {
		success = 1
	}
	pass := int64(success) >= th["success_min"] && duration >= 0 && duration <= th["duration_s_max"]
	details := fmt.Sprintf("README quickstart run verbatim in a fresh clone of HEAD: README commands %ds "+
		"(limit %ds), script total %s including clone and teardown, exit %v", duration, th["duration_s_max"],
		total.Round(time.Second), runErr)
	if success == 0 {
		details += "; output tail: " + tail(text, 1500)
	}
	harness.Finish(t, harness.Result{
		ID:       id,
		Measured: map[string]any{"duration_s": duration, "success": success, "script_total_s": total.Seconds()},
		Pass:     pass,
		Details:  details,
	})
}

// tail returns the last n bytes of s.
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
