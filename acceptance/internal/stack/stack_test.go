// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/thnhdatle/wake/pkg/event"
)

// ev builds a CDC data event of public.t with key id and column ver.
func ev(t *testing.T, id, op string, key, ver int64) *event.Event {
	t.Helper()
	img := fmt.Sprintf(`{"id":%d,"ver":%d}`, key, ver)
	before, after := "null", img
	switch op {
	case "u":
		before = fmt.Sprintf(`{"id":%d}`, key)
	case "d":
		before, after = img, "null"
	}
	raw := fmt.Sprintf(`{"id":%q,"source":{"db":"app","schema":"public","table":"t"},"op":%q,"ts_ms":1,`+
		`"lsn":"0/1","tx_id":1,"key":{"id":%d},"before":%s,"after":%s,"schema_version":1}`, id, op, key, before, after)
	e, err := event.Decode([]byte(raw))
	require.NoError(t, err)
	return e
}

func ch(op string, key, ver int64) Change {
	return Change{Table: "public.t", Key: IDKey(key), Ver: ver, Op: op}
}

func TestFindLost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		changes       []Change
		events        func(t *testing.T) []*event.Event
		wantLost      int
		wantUnmatched int
	}{
		{
			name:    "every change delivered, duplicates ignored",
			changes: []Change{ch(OpCreate, 1, 1), ch(OpUpdate, 1, 2), ch(OpDelete, 1, 2)},
			events: func(t *testing.T) []*event.Event {
				t.Helper()
				return []*event.Event{
					ev(t, "0/10-0", "c", 1, 1), ev(t, "0/20-0", "u", 1, 2), ev(t, "0/20-0", "u", 1, 2),
					ev(t, "0/30-0", "d", 1, 2),
				}
			},
		},
		{
			name:    "a missing ver is lost",
			changes: []Change{ch(OpCreate, 1, 1), ch(OpUpdate, 1, 2), ch(OpUpdate, 1, 3)},
			events: func(t *testing.T) []*event.Event {
				t.Helper()
				return []*event.Event{ev(t, "0/10-0", "c", 1, 1), ev(t, "0/30-0", "u", 1, 3)}
			},
			wantLost: 1,
		},
		{
			name:    "a snapshot read counts as a create; unrelated events are unmatched",
			changes: []Change{ch(OpCreate, 2, 1)},
			events: func(t *testing.T) []*event.Event {
				t.Helper()
				return []*event.Event{ev(t, "0/10-r1-0", "r", 2, 1), ev(t, "0/11-0", "c", 3, 1)}
			},
			wantUnmatched: 1,
		},
		{
			name:    "a duplicate id cannot stand in for a second change",
			changes: []Change{ch(OpUpdate, 1, 2), ch(OpUpdate, 1, 2)},
			events: func(t *testing.T) []*event.Event {
				t.Helper()
				return []*event.Event{ev(t, "0/20-0", "u", 1, 2), ev(t, "0/20-0", "u", 1, 2)}
			},
			wantLost: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := FindLost(tc.changes, tc.events(t), "ver")

			require.NoError(t, err)
			assert.Len(t, res.Lost, tc.wantLost)
			assert.Equal(t, tc.wantUnmatched, res.Unmatched)
			assert.Equal(t, len(tc.changes), res.Recorded)
		})
	}
}

func TestOrderViolations(t *testing.T) {
	t.Parallel()
	events := []*event.Event{
		ev(t, "0/10-0", "c", 1, 1),
		ev(t, "0/30-0", "u", 1, 3),
		ev(t, "0/10-0", "c", 1, 1), // duplicate id: dropped, not a violation
		ev(t, "0/20-0", "u", 1, 2), // lower seq than 0/30 for key 1
		ev(t, "0/15-0", "c", 2, 1), // other key: independent
		ev(t, "0/30-1", "u", 1, 4),
	}

	res, err := OrderViolations(events)

	require.NoError(t, err)
	assert.Equal(t, OrderResult{
		Checked: 5, Duplicates: 1, Violations: 1,
		Examples: []string{`public.t {"id":1}: 0/20-0 after 0/30-0`},
	}, res)
	assert.Equal(t, 1, Duplicates(events))
}

func TestIntColumn(t *testing.T) {
	t.Parallel()
	row := event.Row{"a": []byte(" 42 "), "b": []byte(`"7"`), "c": []byte("null"), "d": []byte(`"x"`)}
	tests := []struct {
		col    string
		want   int64
		wantOK bool
	}{
		{"a", 42, true}, {"b", 7, true}, {"c", 0, false}, {"d", 0, false}, {"missing", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.col, func(t *testing.T) {
			t.Parallel()
			got, ok := IntColumn(row, tc.col)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestReceiverDeck_ExactMix(t *testing.T) {
	t.Parallel()
	r := &Receiver{opts: ReceiverOptions{FailurePct: 30, SlowPct: 5, Seed: 1}}
	r.rng = newTestRand(1)
	counts := map[Fault]int{}
	for range 3 * deckSize {
		counts[r.chooseFault()]++
	}

	failures := counts[Fault500] + counts[Fault503] + counts[Fault429] + counts[FaultReset]
	assert.Equal(t, 90, failures)
	assert.Equal(t, 15, counts[FaultSlow])
	assert.Equal(t, 195, counts[FaultNone])
	for _, f := range failureFaults {
		assert.InDelta(t, 22, counts[f], 1, "failure kinds are mixed evenly: %s", f)
	}
}

func TestOverrideYAML(t *testing.T) {
	t.Parallel()
	out := overrideYAML(`/tmp/a b/config.yaml`, map[string]string{"Z": "2", "A": `x"y`})
	var doc struct {
		Services map[string]struct {
			Volumes     []string          `yaml:"volumes"`
			ExtraHosts  []string          `yaml:"extra_hosts"`
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(out, &doc))

	w := doc.Services["wake"]
	assert.Equal(t, []string{"/tmp/a b/config.yaml:/etc/wake/config.yaml:ro", "wake-data:/var/lib/wake"}, w.Volumes)
	assert.Equal(t, []string{"host.docker.internal:host-gateway"}, w.ExtraHosts)
	assert.Equal(t, map[string]string{"A": `x"y`, "Z": "2"}, w.Environment)
	assert.Less(t, strings.Index(string(out), "A:"), strings.Index(string(out), "Z:"), "env sorted")
}

func TestSourceNameAndModule(t *testing.T) {
	t.Parallel()
	name, err := sourceName("source:\n  name: wak07\n")
	require.NoError(t, err)
	assert.Equal(t, "wak07", name)
	_, err = sourceName("sinks: []\n")
	require.Error(t, err)
	assert.True(t, isWakeModule([]byte("// x\nmodule github.com/thnhdatle/wake\n\ngo 1.27\n")))
	assert.False(t, isWakeModule([]byte("module example.com/other\n")))
}

func newTestRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // deterministic test
}

func TestLoadSchedule_OffersRateTimesDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		rate     float64
		duration time.Duration
		want     int
	}{
		{name: "exact multiple", rate: 200, duration: 250 * time.Millisecond, want: 50},
		{name: "partial last interval", rate: 30, duration: 110 * time.Millisecond, want: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := &Load{opts: LoadOptions{Rate: tc.rate, Duration: tc.duration}, stop: make(chan struct{})}
			queue := make(chan arrival, 1000)

			l.schedule(context.Background(), queue, time.Now())

			assert.Equal(t, tc.want, l.res.Offered)
			assert.Len(t, queue, tc.want)
			assert.Zero(t, l.res.Dropped)
		})
	}
}
