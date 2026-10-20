// SPDX-License-Identifier: Apache-2.0

package l1_test

import (
	"fmt"
	"time"

	"github.com/thnhdatle/wake/pkg/cache/l1"
)

// fixedClock is a Clock frozen at a given instant.
type fixedClock struct{ now time.Time }

func (f *fixedClock) Now() time.Time { return f.now }

func ExampleNew() {
	c, err := l1.New[string](10_000, 64<<20)
	if err != nil {
		panic(err)
	}
	c.Set("user:1", l1.Entry[string]{Value: "Ada", Size: 3, TTL: time.Minute})
	v, ok := c.Get("user:1")
	fmt.Println(v, ok)
	// Output: Ada true
}

func ExampleWithShards() {
	c, _ := l1.New[int](100, 1<<20, l1.WithShards(4))
	c.Set("a", l1.Entry[int]{Value: 1})
	fmt.Println(c.Len())
	// Output: 1
}

func ExampleWithClock() {
	clk := &fixedClock{now: time.Unix(0, 0)}
	c, _ := l1.New[int](100, 1<<20, l1.WithClock(clk))
	c.Set("a", l1.Entry[int]{Value: 1, TTL: time.Second})
	clk.now = clk.now.Add(time.Second)
	_, ok := c.Get("a")
	fmt.Println(ok)
	// Output: false
}

func ExampleWithJitter() {
	clk := &fixedClock{now: time.Unix(0, 0)}
	c, _ := l1.New[int](100, 1<<20, l1.WithClock(clk), l1.WithJitter(10*time.Second, 1))
	c.Set("a", l1.Entry[int]{Value: 1, TTL: time.Minute}) // lives between 50 s and 60 s
	clk.now = clk.now.Add(50 * time.Second)
	_, fresh := c.Get("a")
	clk.now = clk.now.Add(10 * time.Second)
	_, stillFresh := c.Get("a")
	fmt.Println(fresh, stillFresh)
	// Output: true false
}

func ExampleCache_Token() {
	c, _ := l1.New[string](100, 1<<20)
	tok := c.Token("user:1") // before reading the lower tier
	c.Delete("user:1")       // an invalidation arrives meanwhile
	stored := c.SetIfUnchanged(tok, "user:1", l1.Entry[string]{Value: "old"})
	fmt.Println(stored)
	// Output: false
}

func ExampleCache_SetIfUnchanged() {
	c, _ := l1.New[string](100, 1<<20)
	tok := c.Token("user:1")
	stored := c.SetIfUnchanged(tok, "user:1", l1.Entry[string]{Value: "Ada", Size: 3})
	v, _ := c.Get("user:1")
	fmt.Println(stored, v)
	// Output: true Ada
}

func ExampleCache_Get() {
	c, _ := l1.New[string](100, 1<<20)
	_, ok := c.Get("missing")
	fmt.Println(ok)
	// Output: false
}

func ExampleCache_GetMaxAge() {
	clk := &fixedClock{now: time.Unix(0, 0)}
	c, _ := l1.New[string](100, 1<<20, l1.WithClock(clk))
	c.Set("a", l1.Entry[string]{Value: "v", TTL: time.Hour})
	clk.now = clk.now.Add(10 * time.Second)
	_, ok := c.GetMaxAge("a", 5*time.Second)
	fmt.Println(ok)
	// Output: false
}

func ExampleCache_GetStale() {
	clk := &fixedClock{now: time.Unix(0, 0)}
	c, _ := l1.New[string](100, 1<<20, l1.WithClock(clk))
	c.Set("a", l1.Entry[string]{Value: "v", TTL: time.Second})
	clk.now = clk.now.Add(30 * time.Second)
	_, fresh := c.Get("a")
	v, stale := c.GetStale("a", time.Minute)
	fmt.Println(fresh, v, stale)
	// Output: false v true
}

func ExampleCache_Set() {
	c, _ := l1.New[string](100, 1<<20)
	c.Set("a", l1.Entry[string]{Value: "v2", Version: 2})
	ok := c.Set("a", l1.Entry[string]{Value: "v1", Version: 1}) // older version
	v, _ := c.Get("a")
	fmt.Println(ok, v)
	// Output: false v2
}

func ExampleCache_Invalidate() {
	c, _ := l1.New[string](100, 1<<20)
	c.Set("a", l1.Entry[string]{Value: "v", Version: 7})
	c.Invalidate("a", 7) // the entry is at least as new: kept
	_, kept := c.Get("a")
	c.Invalidate("a", 8)
	_, after := c.Get("a")
	fmt.Println(kept, after)
	// Output: true false
}

func ExampleCache_Delete() {
	c, _ := l1.New[string](100, 1<<20)
	c.Set("a", l1.Entry[string]{Value: "v"})
	c.Delete("a")
	fmt.Println(c.Len())
	// Output: 0
}

func ExampleCache_Flush() {
	c, _ := l1.New[string](100, 1<<20)
	c.Set("a", l1.Entry[string]{Value: "v"})
	c.Set("b", l1.Entry[string]{Value: "w"})
	c.Flush()
	fmt.Println(c.Len())
	// Output: 0
}

func ExampleCache_Len() {
	c, _ := l1.New[string](100, 1<<20)
	c.Set("a", l1.Entry[string]{Value: "v"})
	fmt.Println(c.Len())
	// Output: 1
}

func ExampleCache_Bytes() {
	c, _ := l1.New[string](100, 1<<20)
	c.Set("a", l1.Entry[string]{Value: "hello", Size: 5})
	fmt.Println(c.Bytes())
	// Output: 5
}
