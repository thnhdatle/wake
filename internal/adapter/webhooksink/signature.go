// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Header names of a webhook request.
const (
	SignatureHeader  = "Wake-Signature"
	DeliveryIDHeader = "Wake-Delivery-Id"
)

// DefaultTolerance is the default window for the signature timestamp that
// receivers should accept.
const DefaultTolerance = 5 * time.Minute

// Sign returns the Wake-Signature header value for body signed at Unix time
// t: "t=<t>,v1=<hex HMAC-SHA256(secret, t + "." + body)>".
func Sign(secret []byte, t int64, body []byte) string {
	ts := strconv.FormatInt(t, 10)
	return "t=" + ts + ",v1=" + hex.EncodeToString(mac(secret, ts, body))
}

func mac(secret []byte, ts string, body []byte) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(ts))
	h.Write([]byte{'.'})
	h.Write(body)
	return h.Sum(nil)
}

// Verify checks a Wake-Signature header against the raw body: one t, at
// least one v1 (any matching v1 is accepted, so receivers can rotate
// secrets), the HMAC compared in constant time, and |now - t| within
// tolerance. A non-positive tolerance disables the time check. It returns an
// error wrapping ErrInvalidSignature.
func Verify(secret []byte, header string, body []byte, now time.Time, tolerance time.Duration) error {
	ts, sigs, err := parseSignature(header)
	if err != nil {
		return err
	}
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: bad timestamp %q", ErrInvalidSignature, ts)
	}
	if tolerance > 0 {
		age := now.Sub(time.Unix(t, 0))
		if age > tolerance || age < -tolerance {
			return fmt.Errorf("%w: timestamp outside the %s tolerance", ErrInvalidSignature, tolerance)
		}
	}
	want := mac(secret, ts, body)
	for _, s := range sigs {
		got, err := hex.DecodeString(s)
		if err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return fmt.Errorf("%w: no matching v1 signature", ErrInvalidSignature)
}

func parseSignature(header string) (ts string, sigs []string, err error) {
	for part := range strings.SplitSeq(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return "", nil, fmt.Errorf("%w: malformed element %q", ErrInvalidSignature, part)
		}
		switch k {
		case "t":
			if ts != "" {
				return "", nil, fmt.Errorf("%w: repeated t", ErrInvalidSignature)
			}
			ts = v
		case "v1":
			sigs = append(sigs, v)
		}
	}
	if ts == "" || len(sigs) == 0 {
		return "", nil, fmt.Errorf("%w: t and v1 are required", ErrInvalidSignature)
	}
	return ts, sigs, nil
}
