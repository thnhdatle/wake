// SPDX-License-Identifier: Apache-2.0

package l2

import (
	"fmt"
	"strconv"
	"strings"
)

type EventKind int

const (
	EventConnected EventKind = iota + 1
	EventDisconnected
	EventInvalidate
	EventLoaded
	EventEpoch
)

type Event struct {
	Kind  EventKind
	Epoch uint64
	Gen   uint64
	Key   string
}

const (
	tagInvalidate = "i"
	tagLoaded     = "l"
	tagEpoch      = "e"
	sep           = "|"
)

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
