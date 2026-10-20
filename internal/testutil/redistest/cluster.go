// SPDX-License-Identifier: Apache-2.0

package redistest

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// clusterPorts are the node ports inside the container; each node's
// cluster bus uses port+10000 on the container's loopback.
var clusterPorts = [...]string{"7000", "7001", "7002"}

const clusterReady = "WAKE-CLUSTER-READY"

// clusterScript starts three cluster-enabled nodes announcing 127.0.0.1,
// creates a 3-master cluster without replicas and waits until every node
// reports cluster_state:ok.
const clusterScript = `set -e
for p in 7000 7001 7002; do
  redis-server --port $p --cluster-enabled yes --cluster-config-file /tmp/nodes-$p.conf \
    --cluster-node-timeout 5000 --cluster-announce-ip 127.0.0.1 --save '' --appendonly no \
    --dir /tmp --daemonize yes --logfile /tmp/redis-$p.log
done
for p in 7000 7001 7002; do
  until redis-cli -p $p ping 2>/dev/null | grep -q PONG; do sleep 0.1; done
done
redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 --cluster-replicas 0 --cluster-yes
for p in 7000 7001 7002; do
  until redis-cli -p $p cluster info | grep -q cluster_state:ok; do sleep 0.1; done
done
echo ` + clusterReady + `
exec tail -f /dev/null
`

// Cluster is a 3-master Redis Cluster in one container.
type Cluster struct {
	ctr *testcontainers.DockerContainer
	// hostAddr maps a node port inside the container to its published
	// host:port; written once by StartCluster.
	hostAddr map[string]string
}

// StartCluster starts the cluster; it is terminated when tb ends.
func StartCluster(tb testing.TB) *Cluster {
	tb.Helper()
	exposed := make([]string, 0, len(clusterPorts))
	for _, p := range clusterPorts {
		exposed = append(exposed, p+"/tcp")
	}
	ctr := startContainer(tb,
		testcontainers.WithExposedPorts(exposed...),
		testcontainers.WithEntrypoint("sh", "-c", clusterScript),
		testcontainers.WithWaitStrategy(wait.ForLog(clusterReady).WithStartupTimeout(startTimeout)),
	)
	c := &Cluster{ctr: ctr, hostAddr: make(map[string]string, len(clusterPorts))}
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	for _, p := range clusterPorts {
		addr, err := endpoint(ctx, ctr, p+"/tcp")
		if err != nil {
			tb.Fatalf("redistest: cluster endpoint %s: %v", p, err)
		}
		c.hostAddr[p] = addr
	}
	return c
}

// dial maps an announced node address (127.0.0.1:700x) to its host port.
func (c *Cluster) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	target, ok := c.hostAddr[port]
	if !ok {
		return nil, fmt.Errorf("redistest: unknown cluster node %s", addr)
	}
	var d net.Dialer
	return d.DialContext(ctx, network, target)
}

// Options returns cluster client options (short timeouts, no retries)
// seeded with the announced node addresses and the mapping Dialer.
func (c *Cluster) Options() *redis.ClusterOptions {
	base := clientOptions(c.dial)
	addrs := make([]string, 0, len(clusterPorts))
	for _, p := range clusterPorts {
		addrs = append(addrs, "127.0.0.1:"+p)
	}
	return &redis.ClusterOptions{
		Addrs:         addrs,
		Dialer:        base.Dialer,
		DialTimeout:   base.DialTimeout,
		DialerRetries: base.DialerRetries,
		ReadTimeout:   base.ReadTimeout,
		WriteTimeout:  base.WriteTimeout,
		MaxRetries:    base.MaxRetries,
	}
}

// Client returns a cluster client built from Options; it is closed when tb
// ends.
func (c *Cluster) Client(tb testing.TB) *redis.ClusterClient {
	tb.Helper()
	rdb := redis.NewClusterClient(c.Options())
	tb.Cleanup(func() { _ = rdb.Close() })
	waitPing(tb, rdb)
	return rdb
}
