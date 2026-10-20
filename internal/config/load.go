// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"go.yaml.in/yaml/v3"
)

// Options controls how a configuration is assembled. Precedence, highest
// first: Overrides (CLI flags), Env, the file, defaults.
type Options struct {
	// Path of the YAML file; empty means no file.
	Path string
	// Env looks up environment variables (os.LookupEnv in the binary); nil
	// disables env overrides.
	Env func(key string) (string, bool)
	// Overrides maps dotted key paths ("admin.listen") to raw values.
	Overrides map[string]string
	// SkipValidate returns the merged configuration without validating it, for
	// commands that only need a few keys (healthcheck, config print).
	SkipValidate bool
}

// Load reads the YAML file at path over the defaults, applies WAKE_* env var
// overrides and validates the result. An empty path uses the defaults and env
// vars only. Problems are joined so that all are reported at once.
func Load(path string) (Config, error) {
	return LoadWith(Options{Path: path, Env: os.LookupEnv})
}

// LoadWith is Load with explicit options.
func LoadWith(opts Options) (Config, error) {
	var raw []byte
	if opts.Path != "" {
		var err error
		raw, err = os.ReadFile(opts.Path) //nolint:gosec // operator-supplied config path
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
	}
	return Parse(raw, opts)
}

// Parse builds a configuration from YAML bytes (which may be empty) merged
// over the defaults, then applies opts.Env and opts.Overrides. Unknown keys
// are errors. opts.Path is used only in messages.
func Parse(data []byte, opts Options) (Config, error) {
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse config %s: %w", sourceLabel(opts.Path), err)
	}
	var errs []error
	if err := applyEnv(&cfg, opts.Env); err != nil {
		errs = append(errs, err)
	}
	if err := applyOverrides(&cfg, opts.Overrides); err != nil {
		errs = append(errs, err)
	}
	cfg.finalize()
	if !opts.SkipValidate {
		if err := cfg.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return cfg, nil
}

func sourceLabel(path string) string {
	if path == "" {
		return "<input>"
	}
	return path
}

// YAML renders the effective configuration as YAML.
func (c Config) YAML() ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(c); err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return buf.Bytes(), nil
}
