#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs the README steps: signs a test body with openssl and checks it with
# the Go, Python and Node verifiers, then checks a tampered body.
set -euo pipefail
cd "$(dirname "$0")/../.."

export WAKE_WEBHOOK_SECRET=whsec_example
BODY='[{"id":"0/16B3748-0","op":"c","key":{"id":42}}]'
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
printf '%s' "$BODY" >"$tmp/body.json"
T=$(date +%s)
SIG="t=$T,v1=$(printf '%s.%s' "$T" "$BODY" | openssl dgst -sha256 -hmac "$WAKE_WEBHOOK_SECRET" | sed 's/^.* //')"
printf '%s' "$BODY" | sed 's/42/43/' >"$tmp/tampered.json"

go build -o "$tmp/verify-go" ./examples/webhook-verify/go
for v in "$tmp/verify-go" "python3 examples/webhook-verify/verify.py" "node examples/webhook-verify/verify.js"; do
	name=$(basename "${v%% *}")
	[ "$name" = verify-go ] && name=go
	echo "$name body: $($v --signature "$SIG" "$tmp/body.json")"
	echo "$name tampered: $($v --signature "$SIG" "$tmp/tampered.json" || true)"
done
