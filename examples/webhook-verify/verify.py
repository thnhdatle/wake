#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify the Wake-Signature header of a Wake webhook request (stdlib only).

Usage:
    WAKE_WEBHOOK_SECRET=... python3 verify.py --signature "t=...,v1=..." body.json

Prints "valid" and exits 0, or prints "invalid: <reason>" and exits 1; usage
errors exit 2. Copy the verify function into your receiver and pass it the raw
request body, before any JSON parsing.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import re
import sys
import time

DEFAULT_TOLERANCE = 300  # seconds; bounds replays of a captured request
SECRET_ENV = "WAKE_WEBHOOK_SECRET"


class InvalidSignatureError(ValueError):
    """The signature does not match, is malformed or is outside the tolerance."""


def verify(
    secret: bytes,
    header: str,
    body: bytes,
    now: float | None = None,
    tolerance: int = DEFAULT_TOLERANCE,
) -> None:
    """Check header "t=<unix>,v1=<hex>" against the raw body.

    The HMAC-SHA256 of "<t>.<body>" with secret is compared in constant time;
    any of several v1 values may match (secret rotation); t must lie within
    tolerance seconds of now. Raises InvalidSignatureError otherwise.
    """
    ts = ""
    sigs: list[str] = []
    for part in header.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            raise InvalidSignatureError(f"malformed element {part!r}")
        if key == "t":
            if ts:
                raise InvalidSignatureError("repeated t")
            ts = value
        elif key == "v1":
            sigs.append(value)
    if not re.fullmatch(r"-?[0-9]+", ts) or not sigs:
        raise InvalidSignatureError("t and v1 are required")
    current = time.time() if now is None else now
    if abs(current - int(ts)) > tolerance:
        raise InvalidSignatureError(f"timestamp outside the {tolerance}s tolerance")
    want = hmac.new(secret, ts.encode() + b"." + body, hashlib.sha256).digest()
    for s in sigs:
        try:
            got = bytes.fromhex(s)
        except ValueError:
            continue
        if hmac.compare_digest(want, got):
            return
    raise InvalidSignatureError("no matching v1 signature")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--signature", required=True, help="Wake-Signature header value")
    parser.add_argument("--tolerance", type=int, default=DEFAULT_TOLERANCE, help="seconds")
    parser.add_argument("body", help="file with the raw request body, or - for stdin")
    args = parser.parse_args(argv)
    secret = os.environ.get(SECRET_ENV, "")
    if not secret:
        print(f"{SECRET_ENV} is not set", file=sys.stderr)
        return 2
    if args.body == "-":
        body = sys.stdin.buffer.read()
    else:
        with open(args.body, "rb") as f:
            body = f.read()
    try:
        verify(secret.encode(), args.signature, body, tolerance=args.tolerance)
    except InvalidSignatureError as e:
        print(f"invalid: {e}")
        return 1
    print("valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
