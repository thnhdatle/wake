// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

const EnvPrefix = "WAKE_"

func EnvName(path string) string {
	return EnvPrefix + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(path))
}

type setting struct {
	path  string
	value reflect.Value
}

var textUnmarshalerType = reflect.TypeFor[textUnmarshaler]()

func settings(cfg *Config) []setting {
	var out []setting
	collect(reflect.ValueOf(cfg).Elem(), "", &out)
	return out
}

func collect(v reflect.Value, prefix string, out *[]setting) {
	for i := range v.NumField() {
		key, _, _ := strings.Cut(v.Type().Field(i).Tag.Get("yaml"), ",")
		if key == "" || key == "-" {
			continue
		}
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		fv := v.Field(i)
		switch {
		case isScalar(fv.Type()):
			*out = append(*out, setting{path: path, value: fv})
		case fv.Kind() == reflect.Struct:
			collect(fv, path, out)
		}
	}
}

func isScalar(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(textUnmarshalerType) {
		return true
	}
	switch t.Kind() { //nolint:exhaustive // only these kinds are overridable; the default rejects the rest
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int64:
		return true
	case reflect.Slice:
		return t.Elem().Kind() == reflect.String
	default:
		return false
	}
}

func (s setting) assign(raw string) error {
	if u, ok := s.value.Addr().Interface().(textUnmarshaler); ok {
		return u.UnmarshalText([]byte(raw))
	}
	switch s.value.Kind() { //nolint:exhaustive // only the kinds isScalar admits; the default rejects the rest
	case reflect.String:
		s.value.SetString(raw)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid boolean %q", raw)
		}
		s.value.SetBool(b)
	case reflect.Int, reflect.Int64:
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer %q", raw)
		}
		s.value.SetInt(n)
	case reflect.Slice:
		var items []string
		for item := range strings.SplitSeq(raw, ",") {
			if item = strings.TrimSpace(item); item != "" {
				items = append(items, item)
			}
		}
		s.value.Set(reflect.ValueOf(items))
	default:
		return fmt.Errorf("unsupported type %s", s.value.Type())
	}
	return nil
}

func applyEnv(cfg *Config, lookup func(string) (string, bool)) error {
	if lookup == nil {
		return nil
	}
	var errs []error
	for _, s := range settings(cfg) {
		name := EnvName(s.path)
		raw, ok := lookup(name)
		if !ok {
			continue
		}
		if err := s.assign(raw); err != nil {
			errs = append(errs, fmt.Errorf("env %s (%s): %w", name, s.path, err))
		}
	}
	return errors.Join(errs...)
}

func applyOverrides(cfg *Config, overrides map[string]string) error {
	if len(overrides) == 0 {
		return nil
	}
	byPath := make(map[string]setting)
	for _, s := range settings(cfg) {
		byPath[s.path] = s
	}
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var errs []error
	for _, k := range keys {
		s, ok := byPath[k]
		if !ok {
			errs = append(errs, fmt.Errorf("override %s: unknown or non-scalar key", k))
			continue
		}
		if err := s.assign(overrides[k]); err != nil {
			errs = append(errs, fmt.Errorf("override %s: %w", k, err))
		}
	}
	return errors.Join(errs...)
}
