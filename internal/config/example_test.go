// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/thnhdatle/wake/internal/config"
)

func TestExample_ParsesAndValidates(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadWith(config.Options{Path: examplePath})

	require.NoError(t, err)
	types := make([]config.SinkType, 0, len(cfg.Sinks))
	for _, s := range cfg.Sinks {
		types = append(types, s.Type)
	}
	all := []config.SinkType{config.SinkKafka, config.SinkNATS, config.SinkWebhook, config.SinkStdout}
	assert.ElementsMatch(t, all, types, "the example must cover every sink type")
}

func TestExample_EveryFieldIsDocumented(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(examplePath)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(raw, &doc))

	missing := missingKeys(reflect.TypeFor[config.Config](), []any{doc}, "")

	assert.Empty(t, missing, "keys of Config absent from deploy/config.example.yaml")
}

func TestExample_MissingKeyDetection(t *testing.T) {
	t.Parallel()

	var doc map[string]any
	require.NoError(t, yaml.Unmarshal([]byte("source: {name: a}\nsinks:\n  - {name: k}\n"), &doc))

	missing := missingKeys(reflect.TypeFor[config.Config](), []any{doc}, "")

	assert.Contains(t, missing, "source.dsn_env")
	assert.Contains(t, missing, "sinks[].kafka")
	assert.Contains(t, missing, "admin")
	assert.NotContains(t, missing, "source.name")
}

func TestExample_EnvDocumented(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(examplePath)
	require.NoError(t, err)

	assert.Contains(t, string(raw), "WAKE_ADMIN_LISTEN")
	assert.Contains(t, string(raw), "Durations")
	assert.Contains(t, string(raw), "Sizes")
}

func missingKeys(t reflect.Type, nodes []any, path string) []string {
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		key, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if key == "" || key == "-" {
			continue
		}
		full := key
		if path != "" {
			full = path + "." + key
		}
		children := childrenAt(nodes, key)
		if len(children) == 0 {
			out = append(out, full)
			continue
		}
		out = append(out, descend(f.Type, children, full)...)
	}
	slices.Sort(out)
	return out
}

func childrenAt(nodes []any, key string) []any {
	var out []any
	for _, n := range nodes {
		if m, ok := n.(map[string]any); ok {
			if c, ok := m[key]; ok {
				out = append(out, c)
			}
		}
	}
	return out
}

func descend(ft reflect.Type, children []any, path string) []string {
	if ft.Kind() == reflect.Pointer {
		ft = ft.Elem()
	}
	switch {
	case ft.Kind() == reflect.Struct && !isUnitType(ft):
		return missingKeys(ft, children, path)
	case ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Struct:
		var elems []any
		for _, c := range children {
			if l, ok := c.([]any); ok {
				elems = append(elems, l...)
			}
		}
		return missingKeys(ft.Elem(), elems, path+"[]")
	default:
		return nil
	}
}

func isUnitType(t reflect.Type) bool {
	return t == reflect.TypeFor[config.Duration]() || t == reflect.TypeFor[config.ByteSize]()
}
