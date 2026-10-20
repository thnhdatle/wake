// SPDX-License-Identifier: Apache-2.0

// Package l2test is the behavioral conformance suite of the l2 contract. It
// runs the same tests against every l2.Backend implementation: the in-memory
// model (pkg/cache/internal/l2fake, unit tests) and Redis
// (pkg/cache/redisl2, integration tests against a single node, a Cluster
// and a TLS server). Only tests import it.
//
// Main types: Harness (one deployment: backends plus raw server access and
// the server clock) and Run.
//
//	l2test.Run(t, func(t *testing.T) l2test.Harness { return newFakeHarness(t) })
package l2test
