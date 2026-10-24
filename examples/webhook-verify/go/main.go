// SPDX-License-Identifier: Apache-2.0

// Command verify checks the Wake-Signature header of a Wake webhook request against its raw body.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultTolerance = 5 * time.Minute

const secretEnv = "WAKE_WEBHOOK_SECRET" //nolint:gosec // G101: an environment variable name, not a secret

type invalidError string

func (e invalidError) Error() string { return string(e) }

// verify checks the Wake-Signature header against the raw body and the secret.
func verify(secret []byte, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var ts string
	var sigs []string
	for part := range strings.SplitSeq(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		switch {
		case !ok:
			return invalidError(fmt.Sprintf("malformed element %q", part))
		case k == "t" && ts != "":
			return invalidError("repeated t")
		case k == "t":
			ts = v
		case k == "v1":
			sigs = append(sigs, v)
		}
	}
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || len(sigs) == 0 {
		return invalidError("t and v1 are required")
	}
	if d := now.Sub(time.Unix(t, 0)); d > tolerance || d < -tolerance {
		return invalidError(fmt.Sprintf("timestamp outside the %s tolerance", tolerance))
	}
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(ts + "."))
	h.Write(body)
	want := h.Sum(nil)
	for _, s := range sigs {
		if got, err := hex.DecodeString(s); err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return invalidError("no matching v1 signature")
}

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr, time.Now()))
}

func run(args []string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer, now time.Time) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sig := fs.String("signature", "", "Wake-Signature header value")
	tolerance := fs.Int("tolerance", int(defaultTolerance/time.Second), "accepted clock difference in seconds")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *sig == "" || fs.NArg() != 1 {
		_, _ = fmt.Fprintln(stderr, "usage: verify --signature <header> [--tolerance <seconds>] <body-file|->")
		return 2
	}
	secret := getenv(secretEnv)
	if secret == "" {
		_, _ = fmt.Fprintf(stderr, "%s is not set\n", secretEnv)
		return 2
	}
	body, err := readBody(fs.Arg(0), stdin)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "read body: %v\n", err)
		return 2
	}
	if err := verify([]byte(secret), *sig, body, now, time.Duration(*tolerance)*time.Second); err != nil {
		_, _ = fmt.Fprintf(stdout, "invalid: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "valid")
	return 0
}

func readBody(path string, stdin io.Reader) ([]byte, error) {
	if path == "-" {
		b, err := io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("stdin: %w", err)
		}
		return b, nil
	}
	b, err := os.ReadFile(path) //nolint:gosec // G304: the user names the file to verify
	if err != nil {
		return nil, fmt.Errorf("file: %w", err)
	}
	return b, nil
}
