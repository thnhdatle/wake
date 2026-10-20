// SPDX-License-Identifier: Apache-2.0

package webhooksink_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
)

func TestSign_Format(t *testing.T) {
	t.Parallel()
	body := []byte(`[{"id":"0/16B3748-0"}]`)
	got := webhooksink.Sign([]byte("secret"), 1760600000, body)
	assert.Regexp(t, `^t=1760600000,v1=[0-9a-f]{64}$`, got)

	h := hmac.New(sha256.New, []byte("secret"))
	h.Write([]byte("1760600000." + string(body)))
	assert.Equal(t, "t=1760600000,v1="+hex.EncodeToString(h.Sum(nil)), got)
}

func TestVerify(t *testing.T) {
	t.Parallel()
	secret := []byte("secret")
	body := []byte(`[{"id":"0/1-0"}]`)
	now := time.Unix(1760600000, 0)
	valid := webhooksink.Sign(secret, now.Unix(), body)
	other := webhooksink.Sign([]byte("other"), now.Unix(), body)
	ts := strconv.FormatInt(now.Unix(), 10)
	cases := []struct {
		name      string
		header    string
		body      string
		now       time.Time
		tolerance time.Duration
		ok        bool
	}{
		{name: "valid", header: valid, body: string(body), now: now, tolerance: time.Minute, ok: true},
		{
			name: "valid with spaces", header: "t=" + ts + ", v1=" + valid[len("t="+ts+",v1="):], body: string(body),
			now: now, tolerance: time.Minute, ok: true,
		},
		{
			name: "rotated secrets", header: other + "," + valid[len("t="+ts+","):], body: string(body), now: now,
			tolerance: time.Minute, ok: true,
		},
		{name: "tampered body", header: valid, body: `[{"id":"0/1-1"}]`, now: now, tolerance: time.Minute},
		{name: "wrong secret", header: other, body: string(body), now: now, tolerance: time.Minute},
		{
			name: "too old", header: valid, body: string(body), now: now.Add(6 * time.Minute),
			tolerance: webhooksink.DefaultTolerance,
		},
		{
			name: "from the future", header: valid, body: string(body), now: now.Add(-6 * time.Minute),
			tolerance: webhooksink.DefaultTolerance,
		},
		{name: "old but tolerance disabled", header: valid, body: string(body), now: now.Add(time.Hour), ok: true},
		{name: "missing v1", header: "t=" + ts, body: string(body), now: now},
		{name: "missing t", header: valid[len("t="+ts+","):], body: string(body), now: now},
		{name: "repeated t", header: "t=" + ts + "," + valid, body: string(body), now: now},
		{name: "malformed element", header: valid + ",junk", body: string(body), now: now},
		{name: "bad timestamp", header: "t=x,v1=00", body: string(body), now: now},
		{name: "non-hex v1", header: "t=" + ts + ",v1=zz", body: string(body), now: now},
		{name: "empty", header: "", body: string(body), now: now},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := webhooksink.Verify(secret, tc.header, []byte(tc.body), tc.now, tc.tolerance)
			if tc.ok {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, webhooksink.ErrInvalidSignature)
		})
	}
}
