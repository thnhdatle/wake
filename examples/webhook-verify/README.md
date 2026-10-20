# Verifying Wake webhook signatures

Every request of a Wake `webhook` sink carries two headers:

| Header | Value |
|---|---|
| `Wake-Signature` | `t=<unix seconds>,v1=<hex HMAC-SHA256(secret, t + "." + raw body)>` |
| `Wake-Delivery-Id` | ULID of the batch, unchanged across retries of the same batch |

The secret is the value of the environment variable named by
`webhook.secret_env`. Wake signs each attempt anew, so `t` is the time of the
attempt. A receiver must:

1. compute the HMAC over the **raw** body bytes, before any JSON parsing or
   re-encoding;
2. compare it with every `v1` value in constant time (several `v1` values may
   appear while a secret is rotated);
3. reject a `t` more than a tolerance (default 5 minutes) away from its own
   clock, which bounds replays of a captured request.

Delivery is at-least-once: deduplicate events by their `id` field (and whole
batches by `Wake-Delivery-Id` if you like). Answer 2xx once the batch is
stored; 408, 429 and 5xx make Wake retry; any other status is a poison
response (see `webhook.max_attempts` and `webhook.dead_letter_dir`).

The three verifiers below share one command line and output:

```text
verify --signature <Wake-Signature value> [--tolerance <seconds>] <body-file|->
```

They read the secret from `WAKE_WEBHOOK_SECRET`, print `valid` and exit 0, or
print `invalid: <reason>` and exit 1; usage errors exit 2. Copy the `verify`
function of your language into your receiver.

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

`go/main.go` is a `main` package inside the Wake module (standard library
only).

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

The Go and Node verifiers print the same line and also exit 1 (`go run`
additionally reports `exit status 1` on stderr). A signature older than the
tolerance prints `invalid: timestamp outside the … tolerance`.

## Tests

```sh
go test ./examples/webhook-verify/...
examples/webhook-verify/run.sh
```

`run.sh` runs the steps above for all three verifiers and prints one line
per check (`make examples` compares it with `expected_output.txt`).

The test signs a body with the sink's own signing function
(`webhooksink.Sign`) and checks that the Go, Python and Node verifiers print
`valid` for it and `invalid: …` for a tampered body and for an expired
signature. It skips an interpreter that is not installed.
