// SPDX-License-Identifier: Apache-2.0

package tcpproxy

import (
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

const dialTimeout = 5 * time.Second

// Proxy forwards every TCP connection accepted on a loopback port to the
// current target. It is safe for concurrent use.
type Proxy struct {
	ln net.Listener
	wg sync.WaitGroup

	mu     sync.Mutex // protects every field below
	target string
	down   bool
	closed bool
	conns  map[net.Conn]struct{}
}

// Start listens on 127.0.0.1 with a free port and forwards to target, which
// may be empty until SetTarget (connections are closed meanwhile). The
// proxy closes when the test ends.
func Start(tb testing.TB, target string) *Proxy {
	tb.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(tb.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("tcpproxy: listen: %v", err)
	}
	p := &Proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	p.wg.Go(p.accept)
	tb.Cleanup(p.Close)
	return p
}

// Addr returns the proxy's "127.0.0.1:<port>".
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Port returns the proxy's port.
func (p *Proxy) Port() int {
	_, port, _ := net.SplitHostPort(p.Addr())
	n, _ := strconv.Atoi(port)
	return n
}

// SetTarget changes where new connections go; open connections keep their
// target.
func (p *Proxy) SetTarget(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = addr
}

// Down simulates an outage: it closes every open connection and closes new
// ones at once until Up.
func (p *Proxy) Down() {
	p.mu.Lock()
	p.down = true
	conns := p.conns
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

// Up ends an outage.
func (p *Proxy) Up() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = false
}

// Close stops the proxy and waits for its goroutines.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	_ = p.ln.Close()
	p.Down()
	p.wg.Wait()
}

func (p *Proxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Go(func() { p.serve(c) })
	}
}

func (p *Proxy) serve(client net.Conn) {
	p.mu.Lock()
	target, down := p.target, p.down || p.closed
	p.mu.Unlock()
	if down || target == "" {
		_ = client.Close()
		return
	}
	d := net.Dialer{Timeout: dialTimeout}
	server, err := d.Dial("tcp", target)
	if err != nil {
		_ = client.Close()
		return
	}
	if !p.track(client, server) {
		return
	}
	var pipes sync.WaitGroup
	pipes.Go(func() { pipe(server, client) })
	pipes.Go(func() { pipe(client, server) })
	pipes.Wait()
	p.mu.Lock()
	delete(p.conns, client)
	delete(p.conns, server)
	p.mu.Unlock()
}

// track registers a connection pair unless an outage began meanwhile.
func (p *Proxy) track(client, server net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.down || p.closed {
		_ = client.Close()
		_ = server.Close()
		return false
	}
	p.conns[client] = struct{}{}
	p.conns[server] = struct{}{}
	return true
}

// pipe copies until either side fails, then closes both, so the peer of a
// closed connection sees it closed too.
func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}
