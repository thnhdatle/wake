// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/event"
)

func TestValue_Int64_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr bool
	}{
		{"max int64", "9223372036854775807", math.MaxInt64, false},
		{"min int64", "-9223372036854775808", math.MinInt64, false},
		{"above 2^53 exact", "9007199254740993", 9007199254740993, false},
		{"zero", "0", 0, false},
		{"padded", " 42 ", 42, false},
		{"overflow", "9223372036854775808", 0, true},
		{"fraction", "1.5", 0, true},
		{"exponent", "1e3", 0, true},
		{"string", `"42"`, 0, true},
		{"null", "null", 0, true},
		{"empty", "", 0, true},
		{"bool", "true", 0, true},
		{"object", "{}", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.Value(tc.raw).Int64()
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrValueType)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValue_Uint64_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    uint64
		wantErr bool
	}{
		{"max uint64", "18446744073709551615", math.MaxUint64, false},
		{"small", "7", 7, false},
		{"negative", "-1", 0, true},
		{"overflow", "18446744073709551616", 0, true},
		{"fraction", "2.0", 0, true},
		{"string", `"7"`, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.Value(tc.raw).Uint64()
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrValueType)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValue_BigInt_Table(t *testing.T) {
	t.Parallel()
	huge := "123456789012345678901234567890"
	want, ok := new(big.Int).SetString(huge, 10)
	require.True(t, ok)

	got, err := event.Value(huge).BigInt()
	require.NoError(t, err)
	assert.Zero(t, want.Cmp(got))

	got, err = event.Value("-9007199254740993").BigInt()
	require.NoError(t, err)
	assert.Equal(t, "-9007199254740993", got.String())

	for _, bad := range []string{"1.5", `"1"`, "null", "1e2", ""} {
		_, err = event.Value(bad).BigInt()
		require.ErrorIs(t, err, event.ErrValueType, bad)
	}
}

func TestValue_Number_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    json.Number
		wantErr bool
	}{
		{"integer", "9007199254740993", "9007199254740993", false},
		{"decimal", "12.50", "12.50", false},
		{"exponent", "-1.5e10", "-1.5e10", false},
		{"string", `"12.50"`, "", true},
		{"null", "null", "", true},
		{"bare minus", "-", "", true},
		{"two numbers", "1 2", "", true},
		{"nan", "NaN", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.Value(tc.raw).Number()
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrValueType)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValue_String_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{"plain", `"a@example.com"`, "a@example.com", false},
		{"escapes", `"a\"b\\c\né<>&"`, "a\"b\\c\né<>&", false},
		{"empty", `""`, "", false},
		{"number", "42", "", true},
		{"null", "null", "", true},
		{"empty input", "", "", true},
		{"truncated", `"abc`, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.Value(tc.raw).String()
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrValueType)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValue_Bool_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    bool
		wantErr bool
	}{
		{"true", "true", true, false},
		{"false", " false ", false, false},
		{"string", `"true"`, false, true},
		{"number", "1", false, true},
		{"null", "null", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.Value(tc.raw).Bool()
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrValueType)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValue_Text_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{"int8 max", "9223372036854775807", "9223372036854775807", false},
		{"above 2^53", "9007199254740993", "9007199254740993", false},
		{"decimal as written", "12.50", "12.50", false},
		{"string verbatim", `"say \"hi\" <b>"`, `say "hi" <b>`, false},
		{"true", "true", "true", false},
		{"false", "false", "false", false},
		{"null", "null", "", true},
		{"empty", "", "", true},
		{"object", `{"a":1}`, "", true},
		{"array", "[1]", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.Value(tc.raw).Text()
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrValueType)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestValue_IsNull_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{"null", "null", true},
		{"padded null", " null\n", true},
		{"empty", "", true},
		{"zero", "0", false},
		{"empty string", `""`, false},
		{"false", "false", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, event.Value(tc.raw).IsNull())
		})
	}
}

func TestValue_Raw_TrimsWhitespace(t *testing.T) {
	t.Parallel()
	assert.JSONEq(t, `{"a":1}`, string(event.Value(` {"a":1} `).Raw()))
	assert.Equal(t, "42", string(event.Value(" 42\n").Raw()))
}
