// SPDX-License-Identifier: Apache-2.0

// Package tcpproxy is a loopback TCP forwarder for integration tests. A
// broker advertises the proxy's fixed address, so the test can restart the
// container (whose mapped host port changes) by retargeting the proxy, and
// can simulate an outage by dropping every connection.
//
//	p := tcpproxy.Start(t, "")
//	// start the container advertising p.Addr(), then:
//	p.SetTarget(mappedHostPort)
//	p.Down() // outage: open connections close, new ones are refused
//	p.Up()
package tcpproxy
