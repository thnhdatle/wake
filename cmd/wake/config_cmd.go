// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/pkg/cache/cdcsource"
)

func loadConfig(path string, skipValidate bool) (config.Config, error) {
	cfg, err := config.LoadWith(config.Options{Path: path, Env: os.LookupEnv, SkipValidate: skipValidate})
	if err != nil {
		return config.Config{}, &exitError{code: exitConfig, err: err}
	}
	return cfg, nil
}

func newConfigCmd(cfgPath *string) *cobra.Command {
	cfg := &cobra.Command{Use: "config", Short: "Configuration operations"}
	cfg.AddCommand(newConfigValidateCmd(cfgPath), newConfigPrintCmd(cfgPath))
	return cfg
}

func newConfigValidateCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration; exits 2 listing every problem",
		Long: "Validate the configuration; exits 2 listing every problem.\n" +
			"When the cache_invalidator section is configured and the environment variable named by\n" +
			"cache_invalidator.validate_dsn_env is set, the templates are also checked against the\n" +
			"database (replica identity, published columns, tables), as 'wake cache-invalidator' does at startup.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(*cfgPath, false)
			if err != nil {
				return err
			}
			env := invalEnv{getenv: os.Getenv, log: slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), nil))}
			if err = validateInvalidatorTemplates(cmd.Context(), &cfg, env); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "configuration is valid")
			return nil
		},
	}
}

func validateInvalidatorTemplates(ctx context.Context, cfg *config.Config, env invalEnv) error {
	if !cfg.CacheInvalidator.Configured() {
		return nil
	}
	m, err := cdcsource.NewMapping(cfg.CacheInvalidator.Templates)
	if err != nil {
		return invalConfigError(fmt.Errorf("cache_invalidator.templates: %w", err))
	}
	return checkInvalidatorTemplates(ctx, cfg, m, env)
}

func newConfigPrintCmd(cfgPath *string) *cobra.Command {
	c := &cobra.Command{
		Use:   "print",
		Short: "Print the effective configuration as YAML",
		Long: "Print the effective configuration (defaults, file and WAKE_* env vars merged) as YAML.\n" +
			"The configuration holds environment variable names, never secret values, and this command\n" +
			"never reads their contents. --redact additionally masks passwords embedded in URLs.\n" +
			"The configuration is printed even when it does not validate; use 'config validate' to check it.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(*cfgPath, true)
			if err != nil {
				return err
			}
			if redact, _ := cmd.Flags().GetBool("redact"); redact {
				cfg = cfg.Redacted()
			}
			out, err := cfg.YAML()
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(out)
			return err //nolint:wrapcheck // writing to the command's own stdout
		},
	}
	c.Flags().Bool("redact", false, "mask passwords embedded in URLs (secret values are never printed)")
	return c
}
