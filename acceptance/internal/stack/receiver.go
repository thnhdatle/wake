// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
)

// EnvReceiverBind overrides the address test receivers listen on (default
// 127.0.0.1, which Docker Desktop forwards host.docker.internal to; Linux
// Docker with host-gateway needs 0.0.0.0).
const EnvReceiverBind = "WAKE_ACCEPTANCE_RECEIVER_BIND"

// Fault is the response the receiver chose for one request.
type Fault string

// Faults injected by the receiver. FaultSlow answers 200 after
// ReceiverOptions.SlowDelay; the others are retryable failures.
const (
	FaultNone  Fault = "ok"
	FaultSlow  Fault = "slow"
	Fault500   Fault = "500"
	Fault503   Fault = "503"
	Fault429   Fault = "429"
	FaultReset Fault = "reset"
)

// failureFaults are the retryable failures, chosen uniformly.
var failureFaults = []Fault{Fault500, Fault503, Fault429, FaultReset}

// ReceiverOptions configure a Receiver.
type ReceiverOptions struct {
	// Secret verifies Wake-Signature when non-empty.
	Secret []byte
	// FailurePct of requests get a retryable failure (500, 503, 429 or a
	// connection reset, in equal shares); exact over every 100 requests.
	FailurePct float64
	// SlowPct of requests are answered 200 after SlowDelay.
	SlowPct float64
	// SlowDelay is the delay of slow responses.
	SlowDelay time.Duration
	// Seed seeds the fault choice.
	Seed uint64
}

// Delivery is one request the receiver accepted with 2xx.
type Delivery struct {
	// Seq is the acceptance order across all requests.
	Seq int
	// DeliveryID is the Wake-Delivery-Id header.
	DeliveryID string
	// At is when the request was accepted.
	At time.Time
	// Items are the elements of the JSON array body (the raw body when it is
	// not an array, e.g. an alert).
	Items []json.RawMessage
}

// ReceiverStats counts requests by outcome.
type ReceiverStats struct {
	Requests        int
	Accepted        int
	Faults          map[Fault]int
	SignatureValid  int
	SignatureFailed int
	SignatureErrors []string
	BadBodies       int
}

// Receiver is an HTTP endpoint for the Wake container (webhook sink or
// lag.alert_webhook). It records accepted bodies in acceptance order.
type Receiver struct {
	opts ReceiverOptions
	srv  *http.Server
	port int

	mu         sync.Mutex
	rng        *rand.Rand
	deck       []Fault
	decks      int
	deliveries []Delivery
	stats      ReceiverStats
	notify     chan struct{}
}

// StartReceiver listens on 127.0.0.1 (see EnvReceiverBind) on a free port
// and serves until cleanup.
func StartReceiver(tb testing.TB, opts ReceiverOptions) *Receiver {
	tb.Helper()
	bind := os.Getenv(EnvReceiverBind)
	if bind == "" {
		bind = "127.0.0.1"
	}
	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", net.JoinHostPort(bind, "0"))
	if err != nil {
		tb.Fatalf("stack: receiver listen: %v", err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		tb.Fatalf("stack: receiver address %v is not TCP", ln.Addr())
	}
	r := &Receiver{
		opts:   opts,
		port:   addr.Port,
		rng:    rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x9e3779b97f4a7c15)), //nolint:gosec // test fault injection
		stats:  ReceiverStats{Faults: map[Fault]int{}},
		notify: make(chan struct{}, 1),
	}
	r.srv = &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = r.srv.Serve(ln) }()
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.srv.Shutdown(ctx)
	})
	return r
}

// URL returns the URL the Wake container uses: http://host.docker.internal:<port>/<path>.
func (r *Receiver) URL(path string) string {
	return "http://" + net.JoinHostPort(HostFromContainer, strconv.Itoa(r.port)) + "/" + path
}

// HostURL returns the URL on the host loopback interface.
func (r *Receiver) HostURL(path string) string {
	return "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(r.port)) + "/" + path
}

// deckSize is the number of requests over which the fault mix is exact.
const deckSize = 100

// chooseFault draws from a shuffled deck of deckSize outcomes holding
// round(FailurePct) failures (500, 503, 429 and reset in turn), round(SlowPct)
// slow responses and successes, so every deckSize requests carry exactly the
// configured mix.
func (r *Receiver) chooseFault() Fault {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.deck) == 0 {
		r.deck = r.newDeck()
	}
	f := r.deck[len(r.deck)-1]
	r.deck = r.deck[:len(r.deck)-1]
	return f
}

func (r *Receiver) newDeck() []Fault {
	deck := make([]Fault, 0, deckSize)
	failures := int(math.Round(r.opts.FailurePct * deckSize / 100))
	slow := int(math.Round(r.opts.SlowPct * deckSize / 100))
	for i := range failures {
		deck = append(deck, failureFaults[(r.decks+i)%len(failureFaults)])
	}
	for range slow {
		deck = append(deck, FaultSlow)
	}
	for len(deck) < deckSize {
		deck = append(deck, FaultNone)
	}
	r.decks++
	r.rng.Shuffle(len(deck), func(i, j int) { deck[i], deck[j] = deck[j], deck[i] })
	return deck
}

// ServeHTTP implements http.Handler.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(io.LimitReader(req.Body, 64<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	r.checkSignature(req, body)
	fault := r.chooseFault()
	r.mu.Lock()
	r.stats.Requests++
	r.stats.Faults[fault]++
	r.mu.Unlock()
	switch fault {
	case Fault500:
		http.Error(w, "injected", http.StatusInternalServerError)
		return
	case Fault503:
		http.Error(w, "injected", http.StatusServiceUnavailable)
		return
	case Fault429:
		http.Error(w, "injected", http.StatusTooManyRequests)
		return
	case FaultReset:
		resetConn(w)
		return
	case FaultSlow:
		time.Sleep(r.opts.SlowDelay)
	case FaultNone:
	}
	r.accept(req, body)
	w.WriteHeader(http.StatusOK)
}

// resetConn closes the connection with SO_LINGER 0 (TCP RST).
func resetConn(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack", http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

func (r *Receiver) checkSignature(req *http.Request, body []byte) {
	if len(r.opts.Secret) == 0 {
		return
	}
	err := webhooksink.Verify(r.opts.Secret, req.Header.Get(webhooksink.SignatureHeader), body, time.Now(),
		webhooksink.DefaultTolerance)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.stats.SignatureValid++
		return
	}
	r.stats.SignatureFailed++
	if len(r.stats.SignatureErrors) < 10 {
		r.stats.SignatureErrors = append(r.stats.SignatureErrors, err.Error())
	}
}

func (r *Receiver) accept(req *http.Request, body []byte) {
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		items = []json.RawMessage{append(json.RawMessage(nil), body...)}
	}
	r.mu.Lock()
	if !json.Valid(body) {
		r.stats.BadBodies++
	}
	r.stats.Accepted++
	r.deliveries = append(r.deliveries, Delivery{
		Seq: len(r.deliveries), DeliveryID: req.Header.Get(webhooksink.DeliveryIDHeader), At: time.Now(), Items: items,
	})
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// Deliveries returns a copy of the accepted requests in acceptance order.
func (r *Receiver) Deliveries() []Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Delivery(nil), r.deliveries...)
}

// Items returns every element of every accepted request in acceptance order.
func (r *Receiver) Items() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]byte
	for _, d := range r.deliveries {
		for _, it := range d.Items {
			out = append(out, it)
		}
	}
	return out
}

// Stats returns a copy of the counters.
func (r *Receiver) Stats() ReceiverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats
	s.Faults = make(map[Fault]int, len(r.stats.Faults))
	for k, v := range r.stats.Faults {
		s.Faults[k] = v
	}
	s.SignatureErrors = append([]string(nil), r.stats.SignatureErrors...)
	return s
}

// ErrTimeout is returned by WaitItems when the condition is not met in time.
var ErrTimeout = errors.New("stack: timeout")

// WaitItems waits until cond holds for the accepted items or ctx ends.
func (r *Receiver) WaitItems(ctx context.Context, cond func(items [][]byte) bool) error {
	for {
		if cond(r.Items()) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrTimeout, ctx.Err())
		case <-r.notify:
		case <-time.After(500 * time.Millisecond):
		}
	}
}
