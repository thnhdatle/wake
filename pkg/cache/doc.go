// SPDX-License-Identifier: Apache-2.0

// Package cache is a two-tier read-through cache with an in-process L1 and a shared Redis L2.
// Delete invalidates keys, typically driven by pkg/cache/cdcsource.
package cache
