// SPDX-License-Identifier: Apache-2.0

package l2

import (
	"fmt"
	"strconv"
	"strings"
)

// EventKind identifies a bus event.
type EventKind int

const (
	// EventConnected reports an active subscription (first or after a
	// reconnect). Messages may have been lost before it.
	EventConnected EventKind = iota + 1
	// EventDisconnected reports a lost subscription.
	EventDisconnected
	// EventInvalidate carries an invalidation: Epoch, Gen (the new
	// generation) and Key.
	EventInvalidate
	// EventLoaded reports that a load of Key in Epoch finished.
	EventLoaded
	// EventEpoch carries a new namespace Epoch.
	EventEpoch
)

// Event is one bus event.
type Event struct {
	Kind  EventKind
	Epoch uint64
	Gen   uint64
	Key   string
}

// Wire format of bus messages, built by the scripts with string
// concatenation (numbers formatted as exact decimals, never %g):
//
//	i|<epoch>|<gen>|<key>   invalidate
//	l|<epoch>|<key>         loaded
//	e|<epoch>               epoch changed
//
// The key is last, so it may contain '|'.
const (
	tagInvalidate = "i"
	tagLoaded     = "l"
	tagEpoch      = "e"
	sep           = "|"
)

// EncodeMessage returns the wire form of a message event. It panics on
// connection events, which are never published.
func EncodeMessage(e Event) string {
	epoch := strconv.FormatUint(e.Epoch, 10)
	switch e.Kind {
	case EventInvalidate:
		return tagInvalidate + sep + epoch + sep + strconv.FormatUint(e.Gen, 10) + sep + e.Key
	case EventLoaded:
		return tagLoaded + sep + epoch + sep + e.Key
	case EventEpoch:
		return tagEpoch + sep + epoch
	case EventConnected, EventDisconnected:
	}
	panic(fmt.Sprintf("l2: event kind %d is not a message", e.Kind))
}

// DecodeMessage parses the wire form of a bus message.
func DecodeMessage(s string) (Event, error) {
	tag, rest, ok := strings.Cut(s, sep)
	if !ok {
		return Event{}, fmt.Errorf("decode message %q: %w", s, ErrMalformed)
	}
	switch tag {
	case tagInvalidate:
		return decodeInvalidate(s, rest)
	case tagLoaded:
		epochText, key, found := strings.Cut(rest, sep)
		epoch, err := strconv.ParseUint(epochText, 10, 64)
		if !found || err != nil {
			return Event{}, fmt.Errorf("decode message %q: %w", s, ErrMalformed)
		}
		return Event{Kind: EventLoaded, Epoch: epoch, Key: key}, nil
	case tagEpoch:
		epoch, err := strconv.ParseUint(rest, 10, 64)
		if err != nil {
			return Event{}, fmt.Errorf("decode message %q: %w", s, ErrMalformed)
		}
		return Event{Kind: EventEpoch, Epoch: epoch}, nil
	}
	return Event{}, fmt.Errorf("decode message %q: %w", s, ErrMalformed)
}

func decodeInvalidate(s, rest string) (Event, error) {
	epochText, rest, ok1 := strings.Cut(rest, sep)
	genText, key, ok2 := strings.Cut(rest, sep)
	epoch, err1 := strconv.ParseUint(epochText, 10, 64)
	gen, err2 := strconv.ParseUint(genText, 10, 64)
	if !ok1 || !ok2 || err1 != nil || err2 != nil {
		return Event{}, fmt.Errorf("decode message %q: %w", s, ErrMalformed)
	}
	return Event{Kind: EventInvalidate, Epoch: epoch, Gen: gen, Key: key}, nil
}
