// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	dateLayout = "2006-01-02"
	detailsMax = 600
)

type criterion struct {
	ID    string `yaml:"id"`
	Title string `yaml:"title"`
	Test  string `yaml:"test"`
	Kind  string `yaml:"kind"`
	Pass  struct {
		Mac map[string]any `yaml:"mac"`
		CI  map[string]any `yaml:"ci"`
	} `yaml:"pass"`
}

type result struct {
	ID       string         `json:"id"`
	Measured map[string]any `json:"measured"`
	Pass     bool           `json:"pass"`
	Details  string         `json:"details"`
}

// loadCriteria reads criteria.yaml into a map keyed by criterion id.
func loadCriteria(path string) (map[string]criterion, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from a flag of this tool
	if err != nil {
		return nil, fmt.Errorf("read criteria: %w", err)
	}
	var doc struct {
		Criteria []criterion `yaml:"criteria"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	out := make(map[string]criterion, len(doc.Criteria))
	for _, c := range doc.Criteria {
		out[c.ID] = c
	}
	return out, nil
}

// loadNotMet reads not-met.yaml into id -> cause; a missing file is empty.
func loadNotMet(path string) (map[string]string, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from a flag of this tool
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read not-met: %w", err)
	}
	var entries []struct {
		ID    string `yaml:"id"`
		Cause string `yaml:"cause"`
	}
	if err := yaml.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		out[e.ID] = strings.Join(strings.Fields(e.Cause), " ")
	}
	return out, nil
}

// thresholds returns the numeric thresholds of c for the CI or the Mac profile.
func thresholds(c criterion, ci bool) map[string]float64 {
	src := c.Pass.Mac
	if ci {
		src = c.Pass.CI
	}
	out := make(map[string]float64, len(src))
	for k, v := range src {
		if f, ok := toFloat(v); ok {
			out[k] = f
		}
	}
	return out
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// loadResult reads one acceptance/logs/WAK-NN.json. found is false when the file is missing.
func loadResult(path string) (res result, date string, found bool, err error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from a flag of this tool
	if errors.Is(err, fs.ErrNotExist) {
		return result{}, "", false, nil
	}
	if err != nil {
		return result{}, "", false, fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &res); err != nil {
		return result{}, "", false, fmt.Errorf("parse %s: %w", path, err)
	}
	date = time.Now().Format(dateLayout)
	if st, serr := os.Stat(path); serr == nil {
		date = st.ModTime().Format(dateLayout)
	}
	return res, date, true, nil
}

// truncate shortens s to at most n runes on one line, marking the cut with "...".
func truncate(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
