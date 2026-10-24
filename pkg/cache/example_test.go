// SPDX-License-Identifier: Apache-2.0

package cache_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/thnhdatle/wake/pkg/cache"
)

type User struct {
	ID   int64  `json:"id" msgpack:"id"`
	Name string `json:"name" msgpack:"name"`
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (m *manualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *manualClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (m *manualClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

func loadUser(id int64) cache.LoadFunc[User] {
	return func(context.Context) (User, error) {
		return User{ID: id, Name: "Ada"}, nil
	}
}

func ExampleNew() {
	users, err := cache.New[User]("users", cache.JSONCodec[User](), cache.WithTTL(10*time.Minute, time.Minute))
	if err != nil {
		panic(err)
	}
	defer users.Close()
	u, err := users.GetOrLoad(context.Background(), "user:42", loadUser(42))
	fmt.Println(u.Name, err)
	// Output: Ada <nil>
}

func ExampleCache_GetOrLoad() {
	users, _ := cache.New[User]("users", cache.JSONCodec[User]())
	defer users.Close()
	ctx := context.Background()
	calls := 0
	load := func(context.Context) (User, error) {
		calls++
		return User{ID: 1, Name: "Ada"}, nil
	}
	_, _ = users.GetOrLoad(ctx, "user:1", load)
	u, _ := users.GetOrLoad(ctx, "user:1", load)
	fmt.Println(u.Name, calls)
	// Output: Ada 1
}

func ExampleCache_Get() {
	users, _ := cache.New[User]("users", cache.JSONCodec[User]())
	defer users.Close()
	_, ok, err := users.Get(context.Background(), "user:1")
	fmt.Println(ok, err)
	// Output: false <nil>
}

func ExampleCache_Set() {
	users, _ := cache.New[User]("users", cache.JSONCodec[User]())
	defer users.Close()
	ctx := context.Background()
	_ = users.Set(ctx, "user:1", User{ID: 1, Name: "Grace"})
	u, ok, _ := users.Get(ctx, "user:1")
	fmt.Println(u.Name, ok)
	// Output: Grace true
}

func ExampleCache_Delete() {
	users, _ := cache.New[User]("users", cache.JSONCodec[User]())
	defer users.Close()
	ctx := context.Background()
	_ = users.Set(ctx, "user:1", User{ID: 1})
	err := users.Delete(ctx, "user:1", "user:email:ada@example.com")
	if errors.Is(err, cache.ErrDeferred) {
		fmt.Println("retry later: other instances are not invalidated yet")
	}
	_, ok, _ := users.Get(ctx, "user:1")
	fmt.Println(ok, err)
	// Output: false <nil>
}

func ExampleCache_Close() {
	users, _ := cache.New[User]("users", cache.JSONCodec[User]())
	fmt.Println(users.Close())
	// Output: <nil>
}

func ExampleJSONCodec() {
	c := cache.JSONCodec[User]()
	b, _ := c.Marshal(User{ID: 1, Name: "Ada"})
	fmt.Println(string(b))
	// Output: {"id":1,"name":"Ada"}
}

func ExampleMsgpackCodec() {
	c := cache.MsgpackCodec[User]()
	b, _ := c.Marshal(User{ID: 1, Name: "Ada"})
	u, _ := c.Unmarshal(b)
	fmt.Println(u.Name, u.ID)
	// Output: Ada 1
}

func ExampleProtobufCodec() {
	c := cache.ProtobufCodec[*wrapperspb.StringValue]()
	b, _ := c.Marshal(wrapperspb.String("Ada"))
	v, _ := c.Unmarshal(b)
	fmt.Println(v.GetValue())
	// Output: Ada
}

func ExampleWithL1() {
	_, err := cache.New[User]("users", cache.JSONCodec[User](), cache.WithL1(50_000, 256<<20))
	fmt.Println(err)
	// Output: <nil>
}

func ExampleWithTTL() {
	clk := &manualClock{now: time.Unix(0, 0)}
	users, _ := cache.New[User]("users", cache.JSONCodec[User](), cache.WithClock(clk),
		cache.WithTTL(time.Minute, 10*time.Second))
	defer users.Close()
	ctx := context.Background()
	_, _ = users.GetOrLoad(ctx, "user:1", loadUser(1))
	clk.Advance(time.Minute)
	_, ok, _ := users.Get(ctx, "user:1")
	fmt.Println(ok)
	// Output: false
}

func ExampleWithNegativeTTL() {
	users, _ := cache.New[User]("users", cache.JSONCodec[User](), cache.WithNegativeTTL(30*time.Second))
	defer users.Close()
	calls := 0
	missing := func(context.Context) (User, error) {
		calls++
		return User{}, cache.ErrNotFound
	}
	_, err := users.GetOrLoad(context.Background(), "user:404", missing)
	_, err2 := users.GetOrLoad(context.Background(), "user:404", missing)
	fmt.Println(errors.Is(err, cache.ErrNotFound), errors.Is(err2, cache.ErrNotFound), calls)
	// Output: true true 1
}

func ExampleWithStaleIfError() {
	clk := &manualClock{now: time.Unix(0, 0)}
	users, _ := cache.New[User]("users", cache.JSONCodec[User](), cache.WithClock(clk),
		cache.WithTTL(10*time.Second, 0), cache.WithStaleIfError(5*time.Minute))
	defer users.Close()
	ctx := context.Background()
	_, _ = users.GetOrLoad(ctx, "user:1", loadUser(1))
	clk.Advance(time.Minute)
	u, err := users.GetOrLoad(ctx, "user:1", func(context.Context) (User, error) {
		return User{}, errors.New("database unreachable")
	})
	fmt.Println(u.Name, err)
	// Output: Ada <nil>
}

func ExampleWithL2() {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"localhost:6379"}})
	defer rdb.Close()

	users, err := cache.New[User]("users", cache.JSONCodec[User](),
		cache.WithL2(rdb),
		cache.WithTTL(10*time.Minute, time.Minute),
		cache.WithDegradedL1TTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer users.Close()

	ctx := context.Background()
	u, err := users.GetOrLoad(ctx, "user:42", loadUser(42))
	fmt.Println(u.Name, err)
	if err := users.Delete(ctx, "user:42"); errors.Is(err, cache.ErrDeferred) {
		fmt.Println("Redis unreachable: invalidation queued for replay")
	}
}

func ExampleWithInvalidationBus() {
	_, err := cache.New[User]("users", cache.JSONCodec[User](), cache.WithInvalidationBus("myapp:inval:users"))
	fmt.Println(err)
	// Output: <nil>
}

func ExampleWithLockTimeout() {
	_, err := cache.New[User]("users", cache.JSONCodec[User](), cache.WithLockTimeout(2*time.Second))
	fmt.Println(err)
	// Output: <nil>
}

func ExampleWithDegradedL1TTL() {
	_, err := cache.New[User]("users", cache.JSONCodec[User](), cache.WithDegradedL1TTL(time.Second))
	fmt.Println(err)
	// Output: <nil>
}

func ExampleWithMaxPendingInvalidations() {
	_, err := cache.New[User]("users", cache.JSONCodec[User](), cache.WithMaxPendingInvalidations(0))
	fmt.Println(err)
	// Output: <nil>
}

func ExampleWithCompression() {
	users, _ := cache.New[string]("docs", cache.JSONCodec[string](), cache.WithCompression(1024))
	defer users.Close()
	ctx := context.Background()
	_ = users.Set(ctx, "doc:1", strings.Repeat("lorem ipsum ", 1000))
	v, _, _ := users.Get(ctx, "doc:1")
	fmt.Println(len(v))
	// Output: 12000
}

func ExampleWithMetrics() {
	reg := prometheus.NewRegistry()
	users, _ := cache.New[User]("users", cache.JSONCodec[User](), cache.WithMetrics(reg))
	defer users.Close()
	_, _, _ = users.Get(context.Background(), "user:1")
	families, _ := reg.Gather()
	for _, f := range families {
		if f.GetName() == "wake_cache_misses_total" {
			fmt.Println(f.GetName(), f.GetMetric()[0].GetCounter().GetValue())
		}
	}
	// Output: wake_cache_misses_total 1
}

func ExampleWithClock() {
	clk := &manualClock{now: time.Unix(0, 0)}
	users, _ := cache.New[User]("users", cache.JSONCodec[User](), cache.WithClock(clk))
	defer users.Close()
	_ = users.Set(context.Background(), "user:1", User{ID: 1}, cache.WithSetTTL(time.Second))
	clk.Advance(time.Second)
	_, ok, _ := users.Get(context.Background(), "user:1")
	fmt.Println(ok)
	// Output: false
}

func ExampleWithSetTTL() {
	users, _ := cache.New[User]("users", cache.JSONCodec[User]())
	defer users.Close()
	err := users.Set(context.Background(), "user:1", User{ID: 1}, cache.WithSetTTL(30*time.Second))
	fmt.Println(err)
	// Output: <nil>
}
