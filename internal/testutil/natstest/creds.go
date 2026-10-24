// SPDX-License-Identifier: Apache-2.0

package natstest

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
)

const jwtHeader = `{"typ":"JWT","alg":"ed25519-nkey"}`

const unlimited = -1

const (
	claimType = "type"
	claimSub  = "sub"
	claimPub  = "pub"
	claimVer  = "version"
)

type trust struct {
	operatorJWT string
	sysPub      string
	sysJWT      string
	accountPub  string
	accountJWT  string
	account     nkeys.KeyPair
}

func newTrust(tb testing.TB) *trust {
	tb.Helper()
	op := mustKey(tb, nkeys.CreateOperator)
	sys := mustKey(tb, nkeys.CreateAccount)
	acc := mustKey(tb, nkeys.CreateAccount)
	t := &trust{sysPub: publicKey(tb, sys), accountPub: publicKey(tb, acc), account: acc}
	opPub := publicKey(tb, op)
	t.operatorJWT = sign(tb, op, opPub, "wake-test-operator", map[string]any{
		claimType: "operator", claimVer: 2, "system_account": t.sysPub,
	})
	t.sysJWT = sign(tb, op, t.sysPub, "SYS", accountClaims(false))
	t.accountJWT = sign(tb, op, t.accountPub, "wake", accountClaims(true))
	return t
}

func accountClaims(jetstream bool) map[string]any {
	limits := map[string]any{
		"subs": unlimited, "data": unlimited, "payload": unlimited, "imports": unlimited,
		"exports": unlimited, "wildcards": true, "conn": unlimited, "leaf": unlimited,
	}
	if jetstream {
		limits["mem_storage"] = unlimited
		limits["disk_storage"] = unlimited
		limits["streams"] = unlimited
		limits["consumer"] = unlimited
	}
	return map[string]any{
		claimType: "account", claimVer: 2, "limits": limits,
		"default_permissions": map[string]any{claimPub: map[string]any{}, claimSub: map[string]any{}},
	}
}

func (t *trust) userCreds(tb testing.TB, dir, name string, signer nkeys.KeyPair) string {
	tb.Helper()
	user := mustKey(tb, nkeys.CreateUser)
	userJWT := sign(tb, signer, publicKey(tb, user), name, map[string]any{
		claimType: "user", claimVer: 2, claimPub: map[string]any{}, claimSub: map[string]any{},
		"subs": unlimited, "data": unlimited, "payload": unlimited,
	})
	seed, err := user.Seed()
	if err != nil {
		tb.Fatalf("natstest: user seed: %v", err)
	}
	creds := fmt.Sprintf("-----BEGIN NATS USER JWT-----\n%s\n------END NATS USER JWT------\n\n"+
		"************************* IMPORTANT *************************\n"+
		"NKEY Seed printed below can be used to sign and prove identity.\n\n"+
		"-----BEGIN USER NKEY SEED-----\n%s\n------END USER NKEY SEED------\n", userJWT, seed)
	path := filepath.Join(dir, name+".creds")
	if err := os.WriteFile(path, []byte(creds), 0o600); err != nil {
		tb.Fatalf("natstest: write creds: %v", err)
	}
	return path
}

func (t *trust) serverConfig() string {
	return fmt.Sprintf("operator: %q\nsystem_account: %s\nresolver: MEMORY\n"+
		"resolver_preload: {\n  %s: %q\n  %s: %q\n}\n",
		t.operatorJWT, t.sysPub, t.sysPub, t.sysJWT, t.accountPub, t.accountJWT)
}

func sign(tb testing.TB, issuer nkeys.KeyPair, subject, name string, natsClaims map[string]any) string {
	tb.Helper()
	claims := map[string]any{
		"jti": randomID(tb), "iat": time.Now().Unix(), "iss": publicKey(tb, issuer),
		"name": name, claimSub: subject, "nats": natsClaims,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		tb.Fatalf("natstest: claims: %v", err)
	}
	enc := base64.RawURLEncoding
	signing := enc.EncodeToString([]byte(jwtHeader)) + "." + enc.EncodeToString(payload)
	sig, err := issuer.Sign([]byte(signing))
	if err != nil {
		tb.Fatalf("natstest: sign: %v", err)
	}
	return signing + "." + enc.EncodeToString(sig)
}

func randomID(tb testing.TB) string {
	tb.Helper()
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		tb.Fatalf("natstest: random: %v", err)
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
}

func mustKey(tb testing.TB, create func() (nkeys.KeyPair, error)) nkeys.KeyPair {
	tb.Helper()
	kp, err := create()
	if err != nil {
		tb.Fatalf("natstest: create key: %v", err)
	}
	return kp
}

func publicKey(tb testing.TB, kp nkeys.KeyPair) string {
	tb.Helper()
	pub, err := kp.PublicKey()
	if err != nil {
		tb.Fatalf("natstest: public key: %v", err)
	}
	return pub
}
