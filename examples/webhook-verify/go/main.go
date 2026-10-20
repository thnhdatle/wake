// SPDX-License-Identifier: Apache-2.0

// Command verify checks the Wake-Signature header of a Wake webhook request
// against its raw body. Copy the verify function into your receiver.
//
//	WAKE_WEBHOOK_SECRET=... go run ./examples/webhook-verify/go --signature "t=...,v1=..." body.json
//
// It prints "valid" and exits 0, or prints "invalid: <reason>" and exits 1;
// usage errors exit 2.
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

// defaultTolerance is the accepted difference between the signature time
// and the receiver's clock; it bounds replays of a captured request.
const defaultTolerance = 5 * time.Minute

// secretEnv names the environment variable holding the shared secret (the
// variable Wake's webhook.secret_env names).
const secretEnv = "WAKE_WEBHOOK_SECRET" //nolint:gosec // G101: an environment variable name, not a secret

// invalidError describes why a signature is rejected.
type invalidError string

func (e invalidError) Error() string { return string(e) }

// verify checks header "t=<unix>,v1=<hex>" against the raw body: the HMAC-
// SHA256 of "<t>.<body>" with secret, compared in constant time; any of
// several v1 values may match (secret rotation); t must lie within
// tolerance of now.
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

// run is main with its dependencies injected; it returns the exit code.
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
