// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Environment names select which threshold set applies.
const (
	EnvMac = "mac"
	EnvCI  = "ci"
)

// EnvCriteriaPath overrides the criteria.yaml location.
const EnvCriteriaPath = "WAKE_ACCEPTANCE_CRITERIA"

// ErrNotFound is returned when a criterion id or threshold key does not exist.
var ErrNotFound = errors.New("harness: not found")

// ErrSkipped is returned when the selected environment is `ci: skip`.
var ErrSkipped = errors.New("harness: criterion is skipped for this environment")

// Thresholds maps a threshold key to its numeric (or string) value.
type Thresholds map[string]any

// Pass holds the thresholds of both environments.
type Pass struct {
	Mac    Thresholds
	CI     Thresholds
	CISkip bool
}

// UnmarshalYAML accepts `ci: skip` as well as a mapping.
func (p *Pass) UnmarshalYAML(n *yaml.Node) error {
	var raw struct {
		Mac Thresholds `yaml:"mac"`
		CI  yaml.Node  `yaml:"ci"`
	}
	if err := n.Decode(&raw); err != nil {
		return fmt.Errorf("harness: decode pass: %w", err)
	}
	p.Mac = raw.Mac
	switch raw.CI.Kind {
	case 0:
		return nil
	case yaml.ScalarNode:
		if raw.CI.Value != "skip" {
			return fmt.Errorf("harness: pass.ci scalar must be \"skip\", got %q", raw.CI.Value)
		}
		p.CISkip = true
	case yaml.DocumentNode, yaml.SequenceNode, yaml.MappingNode, yaml.AliasNode:
		if err := raw.CI.Decode(&p.CI); err != nil {
			return fmt.Errorf("harness: decode pass.ci: %w", err)
		}
	}
	return nil
}

// Entry is one criterion of criteria.yaml with the thresholds of the selected
// environment already resolved.
type Entry struct {
	ID       string   `yaml:"id"`
	Title    string   `yaml:"title"`
	Test     string   `yaml:"test"`
	Kind     string   `yaml:"kind"` // functional, performance or release
	RealDeps []string `yaml:"real_deps"`
	Requires []string `yaml:"requires"`
	Pass     Pass     `yaml:"pass"`

	env string
}

// File is the parsed criteria.yaml.
type File struct {
	Project  string   `yaml:"project"`
	Machine  string   `yaml:"machine"`
	Criteria []*Entry `yaml:"criteria"`

	// Path is where the file was read from.
	Path string `yaml:"-"`
}

// Environment returns "ci" when the CI variable is set to a truthy value and
// "mac" otherwise.
func Environment() string { return EnvironmentOf(os.Getenv("CI")) }

// EnvironmentOf maps a CI variable value to "mac" or "ci".
func EnvironmentOf(ci string) string {
	switch strings.ToLower(ci) {
	case "", "0", "false", "no":
		return EnvMac
	default:
		return EnvCI
	}
}

// Parse decodes criteria YAML and binds every criterion to env.
func Parse(data []byte, env string) (*File, error) {
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("harness: parse criteria: %w", err)
	}
	for _, c := range f.Criteria {
		c.env = env
	}
	return &f, nil
}

// Load reads and parses the criteria file at path.
func Load(path, env string) (*File, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from the repo layout or an explicit override
	if err != nil {
		return nil, fmt.Errorf("harness: read criteria: %w", err)
	}
	f, err := Parse(data, env)
	if err != nil {
		return nil, err
	}
	f.Path = path
	return f, nil
}

// FindCriteria locates criteria.yaml: the override variable first, then
// acceptance/criteria.yaml and criteria.yaml in cwd and each parent.
func FindCriteria() (string, error) {
	if p := os.Getenv(EnvCriteriaPath); p != "" {
		return p, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("harness: getwd: %w", err)
	}
	for {
		for _, rel := range []string{"criteria.yaml", filepath.Join("acceptance", "criteria.yaml")} {
			p := filepath.Join(dir, rel)
			if _, err := os.Stat(p); err == nil {
				return p, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("%w: criteria.yaml", ErrNotFound)
		}
		dir = parent
	}
}

// LoadDefault finds and loads criteria.yaml for the environment chosen by CI.
func LoadDefault() (*File, error) {
	p, err := FindCriteria()
	if err != nil {
		return nil, err
	}
	return Load(p, Environment())
}

// Criterion returns the criterion with the given id from the default file.
func Criterion(id string) (*Entry, error) {
	f, err := LoadDefault()
	if err != nil {
		return nil, err
	}
	return f.Criterion(id)
}

// Criterion returns the criterion with the given id.
func (f *File) Criterion(id string) (*Entry, error) {
	for _, c := range f.Criteria {
		if c.ID == id {
			return c, nil
		}
	}
	return nil, fmt.Errorf("%w: criterion %s", ErrNotFound, id)
}

// Env returns the environment ("mac" or "ci") the criterion is bound to.
func (c *Entry) Env() string { return c.env }

// Thresholds returns the thresholds of the bound environment.
func (c *Entry) Thresholds() (Thresholds, error) {
	if c.env == EnvCI {
		if c.Pass.CISkip {
			return nil, ErrSkipped
		}
		return c.Pass.CI, nil
	}
	return c.Pass.Mac, nil
}

// Int returns an integer threshold.
func (c *Entry) Int(key string) (int64, error) {
	t, err := c.Thresholds()
	if err != nil {
		return 0, err
	}
	return t.Int(key)
}

// Float returns a floating point threshold.
func (c *Entry) Float(key string) (float64, error) {
	t, err := c.Thresholds()
	if err != nil {
		return 0, err
	}
	return t.Float(key)
}

// Duration returns a duration threshold, see Thresholds.Duration.
func (c *Entry) Duration(key string) (time.Duration, error) {
	t, err := c.Thresholds()
	if err != nil {
		return 0, err
	}
	return t.Duration(key)
}

// Int returns key as an integer. Floats with no fractional part are accepted.
func (t Thresholds) Int(key string) (int64, error) {
	f, err := t.Float(key)
	if err != nil {
		return 0, err
	}
	if f != float64(int64(f)) {
		return 0, fmt.Errorf("harness: threshold %q = %v is not an integer", key, f)
	}
	return int64(f), nil
}

// Float returns key as a float64.
func (t Thresholds) Float(key string) (float64, error) {
	v, ok := t[key]
	if !ok {
		return 0, fmt.Errorf("%w: threshold %q", ErrNotFound, key)
	}
	switch n := v.(type) {
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case uint64:
		return float64(n), nil
	case float64:
		return n, nil
	default:
		return 0, fmt.Errorf("harness: threshold %q has non-numeric value %v", key, v)
	}
}

var unitSuffix = regexp.MustCompile(`_(ms|s|min)(_max|_min)?$`)

// Duration returns key as a duration. A string value is parsed with
// time.ParseDuration; a number takes its unit from the key name: a trailing
// `_ms`, `_s` or `_min`, optionally followed by `_max` or `_min` (for example
// `outage_s`, `latency_p99_ms_max`).
func (t Thresholds) Duration(key string) (time.Duration, error) {
	if s, ok := t[key].(string); ok {
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("harness: threshold %q: %w", key, err)
		}
		return d, nil
	}
	f, err := t.Float(key)
	if err != nil {
		return 0, err
	}
	m := unitSuffix.FindStringSubmatch(key)
	if m == nil {
		return 0, fmt.Errorf("harness: threshold %q has no unit suffix (_ms, _s, _min)", key)
	}
	unit := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "min": time.Minute}[m[1]]
	return time.Duration(f * float64(unit)), nil
}
