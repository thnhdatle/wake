// SPDX-License-Identifier: Apache-2.0

package natstest

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func decode(t *testing.T, token, issuer string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	pub, err := nkeys.FromPublicKey(issuer)
	require.NoError(t, err)
	require.NoError(t, pub.Verify([]byte(parts[0]+"."+parts[1]), sig))
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims map[string]any
	require.NoError(t, json.Unmarshal(payload, &claims))
	return claims
}

func TestTrust_CredsFileParsesAndVerifies(t *testing.T) {
	t.Parallel()
	tr := newTrust(t)
	accPub, err := tr.account.PublicKey()
	require.NoError(t, err)
	path := tr.userCreds(t, t.TempDir(), "wake", tr.account)
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	userJWT, err := nkeys.ParseDecoratedJWT(data)
	require.NoError(t, err)
	claims := decode(t, userJWT, accPub)
	assert.Equal(t, accPub, claims["iss"])
	assert.Equal(t, "user", claims["nats"].(map[string]any)["type"]) //nolint:forcetypeassert // our own JSON
	kp, err := nkeys.ParseDecoratedUserNKey(data)
	require.NoError(t, err)
	userPub, err := kp.PublicKey()
	require.NoError(t, err)
	assert.Equal(t, userPub, claims["sub"])

	opPub := mustIssuer(t, tr.operatorJWT)
	opClaims := decode(t, tr.operatorJWT, opPub)
	assert.Equal(t, opPub, opClaims["sub"], "the operator JWT is self-signed")
	assert.Equal(t, tr.sysPub, opClaims["nats"].(map[string]any)["system_account"]) //nolint:forcetypeassert // own JSON
	accClaims := decode(t, tr.accountJWT, opPub)
	limits := accClaims["nats"].(map[string]any)["limits"].(map[string]any) //nolint:forcetypeassert // own JSON
	assert.InDelta(t, float64(unlimited), limits["disk_storage"], 0, "JetStream is enabled for the account")

	conf := tr.serverConfig()
	assert.Contains(t, conf, "resolver: MEMORY")
	assert.Contains(t, conf, tr.accountPub+": ")
}

func mustIssuer(t *testing.T, token string) string {
	t.Helper()
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	require.NoError(t, err)
	var claims struct {
		Iss string `json:"iss"`
	}
	require.NoError(t, json.Unmarshal(payload, &claims))
	return claims.Iss
}

func TestConfig(t *testing.T) {
	t.Parallel()
	n := &NATS{}
	conf := n.config(options{tls: true, maxPayload: 4096})
	assert.Contains(t, conf, "jetstream {")
	assert.Contains(t, conf, "max_payload: 4096\n")
	assert.Contains(t, conf, `cert_file: "/etc/wake/server.pem"`)
	assert.NotContains(t, conf, "operator")
	assert.Equal(t, DefaultImage, image())
}
