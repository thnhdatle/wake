// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/schema"
)

var knownOIDs = []uint32{
	schema.OIDInt2, schema.OIDInt4, schema.OIDInt8, schema.OIDOID, schema.OIDFloat4, schema.OIDFloat8,
	schema.OIDBool, schema.OIDBytea, schema.OIDJSON, schema.OIDJSONB, schema.OIDNumeric, schema.OIDText,
	schema.OIDDate, schema.OIDTime, schema.OIDTimeTZ, schema.OIDTimestamp, schema.OIDTimestampTZ,
	schema.OIDInterval, schema.OIDInt4Array, schema.OIDTextArray, schema.OIDBoxArray, schema.OIDJSONBArray,
	schema.OIDInt4Range, schema.OIDNumRange, schema.OIDTSTZRange, schema.OIDDateRange,
	schema.OIDInt8Multirange, schema.OIDTSTZRangeArray, 16387,
}

func TestAppendJSON_ValidJSONOrUnchanged(t *testing.T) {
	t.Parallel()
	alphabet := []rune(`{}[](),;"\ -+.:0123456789abcdefxPYMDTHSNULemptyinfBC` + "\t")
	rapid.Check(t, func(t *rapid.T) {
		oid := rapid.SampledFrom(knownOIDs).Draw(t, "oid")
		text := rapid.StringOfN(rapid.SampledFrom(alphabet), 0, 40, -1).Draw(t, "text")
		got, err := schema.AppendJSON([]byte("x"), oid, []byte(text))
		if err != nil {
			require.ErrorIs(t, err, schema.ErrInvalidValue)
			assert.Equal(t, "x", string(got))
			return
		}
		assert.True(t, json.Valid(got[1:]), "invalid JSON %q for %q", got[1:], text)
	})
}

func TestAppendJSON_IntegersExact(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		v := rapid.Int64().Draw(t, "v")
		text := strconv.FormatInt(v, 10)
		got, err := schema.AppendJSON(nil, schema.OIDInt8, []byte(text))
		require.NoError(t, err)
		assert.Equal(t, text, string(got))
		var n json.Number
		require.NoError(t, json.Unmarshal(got, &n))
		back, err := n.Int64()
		require.NoError(t, err)
		assert.Equal(t, v, back)
	})
}

func TestAppendJSON_ByteaBase64(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		raw := rapid.SliceOfN(rapid.Byte(), 0, 200).Draw(t, "raw")
		got, err := schema.AppendJSON(nil, schema.OIDBytea, []byte(`\x`+hex.EncodeToString(raw)))
		require.NoError(t, err)
		assert.Equal(t, `"`+base64.StdEncoding.EncodeToString(raw)+`"`, string(got))
		got, err = schema.AppendJSON(nil, schema.OIDBytea, escapeBytea(raw))
		require.NoError(t, err)
		assert.Equal(t, `"`+base64.StdEncoding.EncodeToString(raw)+`"`, string(got))
	})
}

func escapeBytea(raw []byte) []byte {
	var b bytes.Buffer
	for _, c := range raw {
		switch {
		case c == '\\':
			b.WriteString(`\\`)
		case c < 0x20 || c > 0x7e:
			fmt.Fprintf(&b, `\%03o`, c)
		default:
			b.WriteByte(c)
		}
	}
	return b.Bytes()
}

func pgArrayElement(s *string) string {
	if s == nil {
		return "NULL"
	}
	v := *s
	plain := v != "" && !strings.EqualFold(v, "NULL") && !strings.ContainsAny(v, "{},\"\\ \t\n\r\v\f")
	if plain {
		return v
	}
	v = strings.ReplaceAll(v, `\`, `\\`)
	return `"` + strings.ReplaceAll(v, `"`, `\"`) + `"`
}

func TestAppendJSON_TextArrayRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		rows := rapid.IntRange(1, 3).Draw(t, "rows")
		cols := rapid.IntRange(0, 4).Draw(t, "cols")
		want := make([][]*string, rows)
		parts := make([]string, rows)
		for i := range want {
			want[i] = make([]*string, cols)
			elems := make([]string, cols)
			for j := range want[i] {
				if !rapid.Bool().Draw(t, "null") {
					s := rapid.String().Draw(t, "s")
					want[i][j] = &s
				}
				elems[j] = pgArrayElement(want[i][j])
			}
			parts[i] = "{" + strings.Join(elems, ",") + "}"
		}
		text := "{" + strings.Join(parts, ",") + "}"
		got, err := schema.AppendJSON(nil, schema.OIDTextArray, []byte(text))
		require.NoError(t, err, text)
		var back [][]*string
		require.NoError(t, json.Unmarshal(got, &back))
		require.Len(t, back, rows)
		for i := range want {
			require.Len(t, back[i], cols)
			for j := range want[i] {
				if want[i][j] == nil {
					assert.Nil(t, back[i][j])
					continue
				}
				require.NotNil(t, back[i][j])
				assert.Equal(t, strings.ToValidUTF8(*want[i][j], "�"), *back[i][j])
			}
		}
	})
}

func TestAppendJSON_TimestampTZSameInstant(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		unix := rapid.Int64Range(-62135596800+86400, 253402300799-86400).Draw(t, "unix")
		micros := rapid.IntRange(0, 999999).Draw(t, "micros")
		offset := rapid.IntRange(-15*3600, 15*3600).Draw(t, "offset")
		if !rapid.Bool().Draw(t, "seconds offset") {
			offset -= offset % 60
		}
		inst := time.Unix(unix, int64(micros)*1000).In(time.FixedZone("", offset))
		text := inst.Format("2006-01-02 15:04:05.999999") + pgOffset(offset)
		got, err := schema.AppendJSON(nil, schema.OIDTimestampTZ, []byte(text))
		require.NoError(t, err, text)
		var s string
		require.NoError(t, json.Unmarshal(got, &s))
		parsed, err := time.Parse(time.RFC3339Nano, s)
		require.NoError(t, err, s)
		assert.True(t, parsed.Equal(inst), "%s → %s", text, s)
	})
}

func pgOffset(secs int) string {
	sign := "+"
	if secs < 0 {
		sign, secs = "-", -secs
	}
	out := fmt.Sprintf("%s%02d", sign, secs/3600)
	if m, s := secs%3600/60, secs%60; m != 0 || s != 0 {
		out += fmt.Sprintf(":%02d", m)
		if s != 0 {
			out += fmt.Sprintf(":%02d", s)
		}
	}
	return out
}

func pgInterval(months, days int32, micros int64) string {
	var parts []string
	neg := false
	field := func(v int64, unit string) {
		if v == 0 {
			return
		}
		sign := ""
		if neg && v > 0 {
			sign = "+"
		}
		plural := "s"
		if v == 1 {
			plural = ""
		}
		parts = append(parts, fmt.Sprintf("%s%d %s%s", sign, v, unit, plural))
		neg = v < 0
	}
	field(int64(months/12), "year")
	monUnit := months % 12
	if monUnit != 0 {
		field(int64(monUnit), "mon")
	}
	field(int64(days), "day")
	if micros != 0 || len(parts) == 0 {
		parts = append(parts, pgIntervalTime(micros, neg))
	}
	return strings.Join(parts, " ")
}

func pgIntervalTime(micros int64, prevNeg bool) string {
	sign := ""
	abs := uint64(micros)
	switch {
	case micros < 0:
		sign, abs = "-", uint64(-micros)
	case prevNeg:
		sign = "+"
	}
	sec := abs / 1_000_000
	tm := fmt.Sprintf("%s%02d:%02d:%02d", sign, sec/3600, sec%3600/60, sec%60)
	if frac := strings.TrimRight(fmt.Sprintf("%06d", abs%1_000_000), "0"); frac != "" {
		tm += "." + frac
	}
	return tm
}

var isoDurationRE = regexp.MustCompile(`^P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)?` +
	`(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d+)(?:\.(\d+))?S)?)?$`)

func TestAppendJSON_IntervalRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		months := rapid.Int32Range(-1_000_000, 1_000_000).Draw(t, "months")
		days := rapid.Int32Range(-1_000_000, 1_000_000).Draw(t, "days")
		micros := rapid.Int64Range(-1<<62, 1<<62).Draw(t, "micros")
		text := pgInterval(months, days, micros)
		got, err := schema.AppendJSON(nil, schema.OIDInterval, []byte(text))
		require.NoError(t, err, text)
		var s string
		require.NoError(t, json.Unmarshal(got, &s))
		m := isoDurationRE.FindStringSubmatch(s)
		require.NotNil(t, m, "%s → %s", text, s)
		num := func(i int) int64 {
			if m[i] == "" {
				return 0
			}
			v, perr := strconv.ParseInt(m[i], 10, 64)
			require.NoError(t, perr)
			return v
		}
		assert.Equal(t, int64(months), num(1)*12+num(2), "months of %s → %s", text, s)
		assert.Equal(t, int64(days), num(3))
		frac := (m[7] + "000000")[:6]
		f, err := strconv.ParseInt(frac, 10, 64)
		require.NoError(t, err)
		sec := num(6)
		usec := f
		if _, timePart, ok := strings.Cut(s, "T"); ok && strings.Contains(timePart, "-") {
			usec = -f
		}
		total := (num(4)*3600+num(5)*60+sec)*1_000_000 + usec
		assert.Equal(t, micros, total, "%s → %s", text, s)
	})
}

func TestAppendJSON_RangeRoundTrip(t *testing.T) {
	t.Parallel()
	type rangeJSON struct {
		Lower    *int64 `json:"lower"`
		Upper    *int64 `json:"upper"`
		LowerInc bool   `json:"lower_inc"`
		UpperInc bool   `json:"upper_inc"`
	}
	rapid.Check(t, func(t *rapid.T) {
		var want rangeJSON
		text := "("
		if rapid.Bool().Draw(t, "lower bounded") {
			v := rapid.Int64().Draw(t, "lower")
			want.Lower, want.LowerInc = &v, rapid.Bool().Draw(t, "lower inc")
			text = map[bool]string{true: "[", false: "("}[want.LowerInc] + strconv.FormatInt(v, 10)
		}
		text += ","
		if rapid.Bool().Draw(t, "upper bounded") {
			v := rapid.Int64().Draw(t, "upper")
			want.Upper, want.UpperInc = &v, rapid.Bool().Draw(t, "upper inc")
			text += strconv.FormatInt(v, 10) + map[bool]string{true: "]", false: ")"}[want.UpperInc]
		} else {
			text += ")"
		}
		got, err := schema.AppendJSON(nil, schema.OIDInt8Range, []byte(text))
		require.NoError(t, err, text)
		var back rangeJSON
		require.NoError(t, json.Unmarshal(got, &back))
		assert.Equal(t, want, back, text)
	})
}
