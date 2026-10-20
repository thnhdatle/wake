// SPDX-License-Identifier: Apache-2.0

// Package clock provides the injected time source used by app and adapter
// code: Clock with Now, After and NewTicker, the real
// implementation System, and Fake, a manually advanced clock for tests.
//
//	c := clock.NewFake(time.Unix(0, 0))
//	ch := c.After(time.Second)
//	c.Advance(time.Second) // ch now fires
package clock
