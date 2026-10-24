// SPDX-License-Identifier: Apache-2.0

package httpadmin_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/testutil"
)

func specRoutes(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "openapi.yaml"))
	require.NoError(t, err)
	var spec struct {
		OpenAPI string                               `yaml:"openapi"`
		Paths   map[string]map[string]map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(data, &spec))
	require.True(t, strings.HasPrefix(spec.OpenAPI, "3.1"), "OpenAPI 3.1")
	var out []string
	for path, ops := range spec.Paths {
		for method := range ops {
			out = append(out, strings.ToUpper(method)+" "+path)
		}
	}
	slices.Sort(out)
	return out
}

func TestRoutes_MatchOpenAPI(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	srv, err := httpadmin.New("127.0.0.1:0", testInfo, testutil.DiscardLogger(),
		httpadmin.WithRegistry(reg, reg),
		httpadmin.WithReadiness(func(context.Context) []httpadmin.Check { return nil }),
		httpadmin.WithStatus(func(context.Context) (any, error) { return struct{}{}, nil }))
	require.NoError(t, err)

	got := srv.Routes()
	slices.Sort(got)

	assert.Equal(t, specRoutes(t), got)
}
