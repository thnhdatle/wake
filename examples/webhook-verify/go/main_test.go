// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const secret = "whsec_example"

type fixture struct {
	dir, body, tampered, signature, old string
}

func newFixture(t *testing.T, now time.Time) fixture {
	t.Helper()
	dir := t.TempDir()
	body := []byte(`[{"id":"0/16B3748-0","source":{"db":"app","schema":"public","table":"users"},"op":"c",` +
		`"ts_ms":1760600000000,"lsn":"0/16B3748","key":{"id":42},"before":null,` +
		`"after":{"id":42,"email":"a@example.com","note":"<b>&amp; ünïcode"}},` +
		`{"type":"schema_change","id":"0/16C0000-s0"}]`)
	f := fixture{
		dir:       dir,
		body:      filepath.Join(dir, "body.json"),
		tampered:  filepath.Join(dir, "tampered.json"),
		signature: webhooksink.Sign([]byte(secret), now.Unix(), body),
		old:       webhooksink.Sign([]byte(secret), now.Add(-10*time.Minute).Unix(), body),
	}
	require.NoError(t, os.WriteFile(f.body, body, 0o600))
	require.NoError(t, os.WriteFile(f.tampered, bytes.Replace(body, []byte("42"), []byte("43"), 1), 0o600))
	return f
}

func getenv(k string) string {
	if k == secretEnv {
		return secret
	}
	return ""
}

func TestRun(t *testing.T) {
	t.Parallel()
	now := time.Now()
	f := newFixture(t, now)
	body, err := os.ReadFile(f.body)
	require.NoError(t, err)
	cases := []struct {
		name  string
		args  []string
		stdin []byte
		noEnv bool
		code  int
		out   string
	}{
		{name: "valid", args: []string{"--signature", f.signature, f.body}, code: 0, out: "valid\n"},
		{name: "valid from stdin", args: []string{"--signature", f.signature, "-"}, stdin: body, code: 0, out: "valid\n"},
		{name: "tampered", args: []string{"--signature", f.signature, f.tampered}, code: 1, out: "invalid: "},
		{name: "expired", args: []string{"--signature", f.old, f.body}, code: 1, out: "invalid: "},
		{name: "wide tolerance", args: []string{"--signature", f.old, "--tolerance", "900", f.body}, code: 0, out: "valid\n"},
		{name: "malformed", args: []string{"--signature", "v1", f.body}, code: 1, out: "invalid: "},
		{name: "repeated t", args: []string{"--signature", "t=1,t=2,v1=00", f.body}, code: 1, out: "invalid: "},
		{name: "missing v1", args: []string{"--signature", "t=1", f.body}, code: 1, out: "invalid: "},
		{name: "no signature", args: []string{f.body}, code: 2},
		{name: "bad flag", args: []string{"--nope"}, code: 2},
		{name: "no secret", args: []string{"--signature", f.signature, f.body}, noEnv: true, code: 2},
		{name: "missing file", args: []string{"--signature", f.signature, filepath.Join(f.dir, "nope")}, code: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ge := getenv
			if tc.noEnv {
				ge = func(string) string { return "" }
			}
			var stdout, stderr bytes.Buffer
			code := run(tc.args, ge, bytes.NewReader(tc.stdin), &stdout, &stderr, now)
			assert.Equal(t, tc.code, code, "stderr: %s", stderr.String())
			assert.True(t, strings.HasPrefix(stdout.String(), tc.out), "stdout: %q", stdout.String())
		})
	}
}

func TestVerifiers_AgreeWithSink(t *testing.T) {
	t.Parallel()
	f := newFixture(t, time.Now())
	verifiers := map[string]func(t *testing.T, sig, body string) (string, int){
		"go": func(_ *testing.T, sig, body string) (string, int) {
			var stdout, stderr bytes.Buffer
			code := run([]string{"--signature", sig, body}, getenv, nil, &stdout, &stderr, time.Now())
			return stdout.String(), code
		},
		"python": script("python3", "-I", filepath.Join("..", "verify.py")),
		"node":   script("node", filepath.Join("..", "verify.js")),
	}
	for name, v := range verifiers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, code := v(t, f.signature, f.body)
			assert.Equal(t, "valid\n", out)
			assert.Equal(t, 0, code)
			out, code = v(t, f.signature, f.tampered)
			assert.True(t, strings.HasPrefix(out, "invalid: "), out)
			assert.Equal(t, 1, code)
			out, code = v(t, f.old, f.body)
			assert.True(t, strings.HasPrefix(out, "invalid: "), out)
			assert.Equal(t, 1, code)
		})
	}
}

func script(interp string, args ...string) func(t *testing.T, sig, body string) (string, int) {
	return func(t *testing.T, sig, body string) (string, int) {
		t.Helper()
		if _, err := exec.LookPath(interp); err != nil {
			t.Skipf("%s not installed", interp)
		}
		cmd := exec.CommandContext(t.Context(), interp, append(args, "--signature", sig, body)...)
		cmd.Env = append(os.Environ(), secretEnv+"="+secret)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		code := 0
		if err != nil {
			var ee *exec.ExitError
			require.ErrorAs(t, err, &ee, "stderr: %s", stderr.String())
			code = ee.ExitCode()
		}
		require.Empty(t, stderr.String(), "%s exit %s", interp, strconv.Itoa(code))
		return stdout.String(), code
	}
}
