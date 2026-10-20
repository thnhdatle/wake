#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
//
// Verify the Wake-Signature header of a Wake webhook request (stdlib only).
//
// Usage:
//   WAKE_WEBHOOK_SECRET=... node verify.js --signature "t=...,v1=..." body.json
//
// Prints "valid" and exits 0, or prints "invalid: <reason>" and exits 1; usage
// errors exit 2. Copy the verify function into your receiver and pass it the
// raw request body (a Buffer), before any JSON parsing.
'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');

const DEFAULT_TOLERANCE = 300; // seconds; bounds replays of a captured request
const SECRET_ENV = 'WAKE_WEBHOOK_SECRET';

// verify checks header "t=<unix>,v1=<hex>" against the raw body: the
// HMAC-SHA256 of "<t>.<body>" with secret, compared in constant time; any of
// several v1 values may match (secret rotation); t must lie within tolerance
// seconds of now. It throws an Error otherwise.
function verify(secret, header, body, now = Date.now() / 1000, tolerance = DEFAULT_TOLERANCE) {
  let ts = '';
  const sigs = [];
  for (const part of header.split(',')) {
    const el = part.trim();
    const i = el.indexOf('=');
    if (i < 0) {
      throw new Error(`malformed element ${JSON.stringify(part)}`);
    }
    const key = el.slice(0, i);
    const value = el.slice(i + 1);
    if (key === 't') {
      if (ts !== '') {
        throw new Error('repeated t');
      }
      ts = value;
    } else if (key === 'v1') {
      sigs.push(value);
    }
  }
  if (!/^-?\d+$/.test(ts) || sigs.length === 0) {
    throw new Error('t and v1 are required');
  }
  if (Math.abs(now - Number(ts)) > tolerance) {
    throw new Error(`timestamp outside the ${tolerance}s tolerance`);
  }
  const want = crypto.createHmac('sha256', secret).update(`${ts}.`).update(body).digest();
  const ok = sigs.some((s) => {
    const got = Buffer.from(s, 'hex');
    return got.length === want.length && crypto.timingSafeEqual(got, want);
  });
  if (!ok) {
    throw new Error('no matching v1 signature');
  }
}

function main(argv) {
  let signature = '';
  let tolerance = DEFAULT_TOLERANCE;
  const rest = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--signature') {
      signature = argv[++i] ?? '';
    } else if (argv[i] === '--tolerance') {
      tolerance = Number(argv[++i]);
    } else {
      rest.push(argv[i]);
    }
  }
  if (signature === '' || rest.length !== 1 || !Number.isFinite(tolerance)) {
    console.error('usage: verify.js --signature <header> [--tolerance <seconds>] <body-file|->');
    return 2;
  }
  const secret = process.env[SECRET_ENV] ?? '';
  if (secret === '') {
    console.error(`${SECRET_ENV} is not set`);
    return 2;
  }
  const body = fs.readFileSync(rest[0] === '-' ? 0 : rest[0]);
  try {
    verify(secret, signature, body, Date.now() / 1000, tolerance);
  } catch (e) {
    console.log(`invalid: ${e.message}`);
    return 1;
  }
  console.log('valid');
  return 0;
}

module.exports = { verify };

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2));
}
