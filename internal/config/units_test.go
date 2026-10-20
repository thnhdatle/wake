// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/thnhdatle/wake/internal/config"
)

func TestParseDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    time.Duration
		wantErr bool
	}{
		{name: "seconds", in: "30s", want: 30 * time.Second},
		{name: "milliseconds", in: "10ms", want: 10 * time.Millisecond},
		{name: "minutes and seconds", in: "1m30s", want: 90 * time.Second},
		{name: "days", in: "14d", want: 14 * 24 * time.Hour},
		{name: "days and hours", in: "1d12h", want: 36 * time.Hour},
		{name: "fractional days", in: "1.5d", want: 36 * time.Hour},
		{name: "zero", in: "0", want: 0},
		{name: "spaces trimmed", in: " 5m ", want: 5 * time.Minute},
		{name: "empty", in: "", wantErr: true},
		{name: "negative", in: "-1d", wantErr: true},
		{name: "unknown unit", in: "10x", wantErr: true},
		{name: "missing unit", in: "1d12", wantErr: true},
		{name: "overflow", in: "99999999999d", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParseDuration(tc.in)

			if tc.wantErr {
				require.ErrorIs(t, err, config.ErrUnit)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseByteSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    int64
		wantErr bool
	}{
		{name: "plain bytes", in: "512", want: 512},
		{name: "explicit bytes", in: "512B", want: 512},
		{name: "KiB", in: "4KiB", want: 4 << 10},
		{name: "MiB", in: "256MiB", want: 256 << 20},
		{name: "GiB", in: "1GiB", want: 1 << 30},
		{name: "TiB", in: "2TiB", want: 2 << 40},
		{name: "space before unit", in: "64 MiB", want: 64 << 20},
		{name: "decimal unit rejected", in: "5MB", wantErr: true},
		{name: "lower case rejected", in: "5mib", wantErr: true},
		{name: "fraction rejected", in: "1.5MiB", wantErr: true},
		{name: "negative rejected", in: "-1", wantErr: true},
		{name: "empty", in: "", wantErr: true},
		{name: "multiplication overflow", in: "99999999999TiB", wantErr: true},
		{name: "number overflow", in: "99999999999999999999", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParseByteSize(tc.in)

			if tc.wantErr {
				require.ErrorIs(t, err, config.ErrUnit)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDurationAndByteSize_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"seconds", config.Duration(10 * time.Second).String(), "10s"},
		{"whole minute", config.Duration(time.Minute).String(), "1m"},
		{"whole hour", config.Duration(time.Hour).String(), "1h"},
		{"hour and minutes", config.Duration(90 * time.Minute).String(), "1h30m"},
		{"milliseconds", config.Duration(10 * time.Millisecond).String(), "10ms"},
		{"whole days", config.Duration(14 * 24 * time.Hour).String(), "14d"},
		{"zero duration", config.Duration(0).String(), "0s"},
		{"MiB", config.ByteSize(256 << 20).String(), "256MiB"},
		{"GiB", config.ByteSize(10 << 30).String(), "10GiB"},
		{"odd bytes", config.ByteSize(1025).String(), "1025"},
		{"KiB multiple", config.ByteSize(1536 << 10).String(), "1536KiB"},
		{"zero size", config.ByteSize(0).String(), "0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

func TestUnits_YAMLRoundTrip(t *testing.T) {
	t.Parallel()

	type doc struct {
		D config.Duration `yaml:"d"`
		B config.ByteSize `yaml:"b"`
	}
	tests := []struct {
		name string
		in   string
		want doc
	}{
		{"strings", "d: 1d12h\nb: 256MiB\n", doc{config.Duration(36 * time.Hour), config.ByteSize(256 << 20)}},
		{"bare integer is bytes", "d: 1s\nb: 512\n", doc{config.Duration(time.Second), 512}},
		{"quoted", "d: \"30s\"\nb: \"1GiB\"\n", doc{config.Duration(30 * time.Second), config.ByteSize(1 << 30)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got doc
			require.NoError(t, yaml.Unmarshal([]byte(tc.in), &got))
			assert.Equal(t, tc.want, got)

			out, err := yaml.Marshal(got)
			require.NoError(t, err)
			var again doc
			require.NoError(t, yaml.Unmarshal(out, &again))
			assert.Equal(t, tc.want, again)
		})
	}
}

func TestUnits_YAMLErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"bad duration names the line", "d: soon\n", "line 1"},
		{"bad size", "b: lots\n", "size"},
		{"sequence", "d: [1]\n", "expected a scalar"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got struct {
				D config.Duration `yaml:"d"`
				B config.ByteSize `yaml:"b"`
			}
			require.ErrorContains(t, yaml.Unmarshal([]byte(tc.in), &got), tc.want)
		})
	}
}

func TestUnits_TextRoundTrip(t *testing.T) {
	t.Parallel()

	var d config.Duration
	require.NoError(t, d.UnmarshalText([]byte("2d")))
	text, err := d.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "2d", string(text))

	var b config.ByteSize
	require.NoError(t, b.UnmarshalText([]byte("3MiB")))
	text, err = b.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "3MiB", string(text))
	assert.Equal(t, int64(3<<20), b.Int64())
	assert.Equal(t, 2*24*time.Hour, d.Std())
	require.Error(t, b.UnmarshalText([]byte("x")))
	require.Error(t, d.UnmarshalText([]byte("x")))
}
