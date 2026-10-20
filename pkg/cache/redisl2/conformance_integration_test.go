// SPDX-License-Identifier: Apache-2.0

//go:build integration

package redisl2_test

import (
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/thnhdatle/wake/internal/testutil/redistest"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2/l2test"
)

// The l2 contract suite, the same one the l2 fake passes, against a single
// node, a 3-master Cluster and a TLS-only node.

func TestConformance_SingleNode(t *testing.T) {
	t.Parallel()
	srv := redistest.StartSingle(t)
	h := newRedisHarness(srv.Client(t), func(name string) redis.UniversalClient {
		opt := srv.Options()
		opt.ClientName = name
		return redis.NewClient(opt)
	})
	l2test.Run(t, func(*testing.T) l2test.Harness { return h })
}

func TestConformance_Cluster(t *testing.T) {
	t.Parallel()
	c := redistest.StartCluster(t)
	h := newRedisHarness(c.Client(t), func(name string) redis.UniversalClient {
		opt := c.Options()
		opt.ClientName = name
		return redis.NewClusterClient(opt)
	})
	l2test.Run(t, func(*testing.T) l2test.Harness { return h })
}

func TestConformance_TLS(t *testing.T) {
	t.Parallel()
	srv := redistest.StartTLS(t)
	h := newRedisHarness(srv.Client(t), func(name string) redis.UniversalClient {
		opt := srv.Options(t)
		opt.ClientName = name
		return redis.NewClient(opt)
	})
	l2test.Run(t, func(*testing.T) l2test.Harness { return h })
}
