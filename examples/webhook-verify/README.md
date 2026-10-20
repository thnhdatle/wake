# Verifying Wake webhook signatures

Check a Wake webhook signature in Go, Python or Node.

| Header | Value |
|---|---|
| `Wake-Signature` | `t=<unix seconds>,v1=<hex HMAC-SHA256(secret, t + "." + raw body)>` |
| `Wake-Delivery-Id` | batch ULID, same across retries |

The secret is in the variable named by `webhook.secret_env`. A receiver must:

1. compute the HMAC over the **raw** body, before any JSON parsing;
2. compare it with every `v1` value in constant time (rotation sends several);
3. reject a `t` more than the tolerance (default 5 minutes) from its clock.

Deduplicate events by `id`. Answer 2xx once stored. 408, 429 and 5xx make Wake retry. Any
other status is a poison response (`webhook.max_attempts`, `webhook.dead_letter_dir`).

All three verifiers take:

```text
verify --signature <Wake-Signature value> [--tolerance <seconds>] <body-file|->
```

They read `WAKE_WEBHOOK_SECRET`. They print `valid` (exit 0) or `invalid: <reason>` (exit 1).
Usage errors exit 2. Copy the `verify` function into your receiver.

## Make a signed test body

From the repository root, with `openssl`:

```sh
export WAKE_WEBHOOK_SECRET=whsec_example
BODY='[{"id":"0/16B3748-0","op":"c","key":{"id":42}}]'
printf '%s' "$BODY" > /tmp/wake-body.json
T=$(date +%s)
SIG="t=$T,v1=$(printf '%s.%s' "$T" "$BODY" | openssl dgst -sha256 -hmac "$WAKE_WEBHOOK_SECRET" | sed 's/^.* //')"
```

## Go

```sh
go run ./examples/webhook-verify/go --signature "$SIG" /tmp/wake-body.json
```

```text
valid
```

## Python (3.10+, standard library only)

```sh
python3 examples/webhook-verify/verify.py --signature "$SIG" /tmp/wake-body.json
```

```text
valid
```

## Node (18+, standard library only)

```sh
node examples/webhook-verify/verify.js --signature "$SIG" /tmp/wake-body.json
```

```text
valid
```

## A tampered body

```sh
printf '%s' "$BODY" | sed 's/42/43/' | python3 examples/webhook-verify/verify.py --signature "$SIG" -
```

```text
invalid: no matching v1 signature
```

Go and Node print the same and exit 1. An expired signature prints
`invalid: timestamp outside the … tolerance`.

## Tests

```sh
go test ./examples/webhook-verify/...   # all three verifiers, valid, tampered, expired
examples/webhook-verify/run.sh          # the steps above; make examples compares with expected_output.txt
```

The test skips an interpreter that is not installed.
