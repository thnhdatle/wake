// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/thnhdatle/wake/internal/domain/change"
)

const Prefix = "wake.watermark"

const (
	memberNonce        = "nonce"
	memberRun          = "run"
	memberTable        = "table"
	memberChunk        = "chunk"
	memberMark         = "mark"
	memberSnapshot     = "snapshot"
	memberCommittedXIP = "committed_xip"
)

type Mark string

const (
	MarkLow  Mark = "low"
	MarkHigh Mark = "high"
)

// Payload is the content of a watermark message.
type Payload struct {
	Nonce        string
	Run          uint64
	Table        string
	Chunk        uint64
	Mark         Mark
	Snapshot     Snapshot
	CommittedXIP []uint64
}

// ParsePayload decodes a watermark payload.
func ParsePayload(b []byte) (Payload, error) {
	var p Payload
	if err := p.UnmarshalJSON(b); err != nil {
		return Payload{}, err
	}
	return p, nil
}

func (p *Payload) IsOwn(nonce string) bool { return p.Nonce == nonce }

func (p *Payload) IsFor(nonce string, run uint64, table string) bool {
	return p.IsOwn(nonce) && p.Run == run && p.Table == table
}

func (p *Payload) Validate() error {
	if !validNonce(p.Nonce) {
		return fmt.Errorf("%w: nonce %q is not a ULID", ErrInvalidPayload, p.Nonce)
	}
	if _, _, ok := SplitTable(p.Table); !ok {
		return fmt.Errorf("%w: table %q", ErrInvalidPayload, p.Table)
	}
	switch p.Mark {
	case MarkLow:
		if !p.Snapshot.IsZero() || p.CommittedXIP != nil {
			return fmt.Errorf("%w: low watermark with snapshot", ErrInvalidPayload)
		}
		return nil
	case MarkHigh:
		return p.validateHigh()
	default:
		return fmt.Errorf("%w: mark %q", ErrInvalidPayload, p.Mark)
	}
}

func (p *Payload) validateHigh() error {
	if err := p.Snapshot.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	for i, x := range p.CommittedXIP {
		if i > 0 && x <= p.CommittedXIP[i-1] {
			return fmt.Errorf("%w: committed_xip not increasing", ErrInvalidPayload)
		}
		if _, found := slices.BinarySearch(p.Snapshot.XIP, x); !found {
			return fmt.Errorf("%w: committed xid %d not in xip", ErrInvalidPayload, x)
		}
	}
	return nil
}

func (p Payload) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p.appendJSON(make([]byte, 0, 128)), nil
}

func (p *Payload) appendJSON(dst []byte) []byte {
	dst = append(dst, `{"nonce":`...)
	dst = change.AppendString(dst, p.Nonce)
	dst = append(dst, `,"run":`...)
	dst = strconv.AppendUint(dst, p.Run, 10)
	dst = append(dst, `,"table":`...)
	dst = change.AppendString(dst, p.Table)
	dst = append(dst, `,"chunk":`...)
	dst = strconv.AppendUint(dst, p.Chunk, 10)
	dst = append(dst, `,"mark":`...)
	dst = change.AppendString(dst, string(p.Mark))
	if p.Mark == MarkHigh {
		dst = append(dst, `,"snapshot":"`...)
		dst = p.Snapshot.AppendText(dst)
		dst = append(dst, `","committed_xip":[`...)
		for i, x := range p.CommittedXIP {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = strconv.AppendUint(dst, x, 10)
		}
		dst = append(dst, ']')
	}
	return append(dst, '}')
}

func (p *Payload) UnmarshalJSON(b []byte) error {
	var (
		v    Payload
		seen = make(map[string]bool, 7)
	)
	err := decodeObject(b, func(name string, dec *json.Decoder) error {
		seen[name] = true
		return v.decodeMember(name, dec)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	for _, name := range []string{memberNonce, memberRun, memberTable, memberChunk, memberMark} {
		if !seen[name] {
			return fmt.Errorf("%w: missing %q", ErrInvalidPayload, name)
		}
	}
	switch high := v.Mark == MarkHigh; {
	case high && (!seen[memberSnapshot] || !seen[memberCommittedXIP]):
		return fmt.Errorf("%w: high watermark without snapshot or committed_xip", ErrInvalidPayload)
	case !high && (seen[memberSnapshot] || seen[memberCommittedXIP]):
		return fmt.Errorf("%w: low watermark with snapshot or committed_xip", ErrInvalidPayload)
	}
	if err := v.Validate(); err != nil {
		return err
	}
	*p = v
	return nil
}

func (p *Payload) decodeMember(name string, dec *json.Decoder) error {
	var err error
	switch name {
	case memberNonce:
		p.Nonce, err = decodeString(dec)
	case memberRun:
		p.Run, err = decodeUint(dec)
	case memberTable:
		p.Table, err = decodeString(dec)
	case memberChunk:
		p.Chunk, err = decodeUint(dec)
	case memberMark:
		var m string
		m, err = decodeString(dec)
		p.Mark = Mark(m)
	case memberSnapshot:
		var s string
		if s, err = decodeString(dec); err == nil {
			p.Snapshot, err = ParseSnapshot(s)
		}
	case memberCommittedXIP:
		p.CommittedXIP, err = decodeUints(dec)
	default:
		err = errField
	}
	return err
}
