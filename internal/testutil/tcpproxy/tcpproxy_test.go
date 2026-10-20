// SPDX-License-Identifier: Apache-2.0

package tcpproxy_test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/testutil/tcpproxy"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// echoServer answers every line with the same line prefixed by tag.
func echoServer(t *testing.T, tag string) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if _, err := c.Write([]byte(tag + line)); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); <-done })
	return ln.Addr().String()
}

func roundTrip(t *testing.T, addr string) (string, error) {
	t.Helper()
	d := net.Dialer{Timeout: time.Second}
	c, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		return "", err
	}
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(2*time.Second)))
	if _, err := c.Write([]byte("hi\n")); err != nil {
		return "", err
	}
	return bufio.NewReader(c).ReadString('\n')
}

func TestProxy_ForwardsRetargetsAndFailsDuringOutage(t *testing.T) {
	t.Parallel()
	a, b := echoServer(t, "a:"), echoServer(t, "b:")
	p := tcpproxy.Start(t, "")
	assert.Positive(t, p.Port())

	_, err := roundTrip(t, p.Addr())
	require.Error(t, err, "no target yet")

	p.SetTarget(a)
	got, err := roundTrip(t, p.Addr())
	require.NoError(t, err)
	assert.Equal(t, "a:hi\n", got)

	d := net.Dialer{Timeout: time.Second}
	held, err := d.DialContext(t.Context(), "tcp", p.Addr())
	require.NoError(t, err)
	defer held.Close()
	_, err = held.Write([]byte("x\n"))
	require.NoError(t, err)
	_, err = bufio.NewReader(held).ReadString('\n')
	require.NoError(t, err)

	p.Down()
	require.NoError(t, held.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = held.Read(make([]byte, 1))
	require.Error(t, err, "open connections close on Down")
	_, err = roundTrip(t, p.Addr())
	require.Error(t, err, "new connections fail during the outage")

	p.Up()
	p.SetTarget(b)
	got, err = roundTrip(t, p.Addr())
	require.NoError(t, err)
	assert.Equal(t, "b:hi\n", got)
	p.Close()
	p.Close()
}
