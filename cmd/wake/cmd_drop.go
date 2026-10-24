// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/thnhdatle/wake/internal/adapter/pgrepl"
	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/config"
)

var errNotConfirmed = errors.New("refusing a destructive operation without --yes")

func confirm(cmd *cobra.Command, what string) error {
	if yes, _ := cmd.Flags().GetBool("yes"); yes {
		return nil
	}
	return &exitError{code: exitConfig, err: withCode(codeConfirmationRequired,
		fmt.Errorf("%w: %s", errNotConfirmed, what))}
}

func newSlotDropCmd(cfgPath *string) *cobra.Command {
	c := &cobra.Command{
		Use:   "drop",
		Short: "Drop the replication slot",
		Long: "Drop the replication slot wake_<source.name> (decommission). Postgres then releases the WAL\n" +
			"it retained; changes not yet delivered are lost. The state file then refers to a missing slot:\n" +
			"the next `wake run` refuses with slot_missing unless --reinit-state is given.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadForDrop(*cfgPath)
			if err != nil {
				return err
			}
			slot := slotName(&cfg)
			if err = confirm(cmd, "drop replication slot "+slot); err != nil {
				return err
			}
			dsn, err := sourceDSN(&cfg)
			if err != nil {
				return err
			}
			conn, err := pgrepl.New(dsn, pgrepl.WithRuntimeParam("application_name", appName))
			if err != nil {
				return &exitError{code: exitConfig, err: withCode(codeConfigInvalid, err)}
			}
			defer func() { _ = conn.Close(context.WithoutCancel(cmd.Context())) }()
			if err := conn.DropSlot(cmd.Context(), slot); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "dropped replication slot %s (if it existed); the state file %s now "+
				"refers to a missing slot\n", slot, cfg.State.Path)
			return nil
		},
	}
	c.Flags().Bool("yes", false, "confirm the destructive operation")
	return c
}

func newPublicationDropCmd(cfgPath *string) *cobra.Command {
	c := &cobra.Command{
		Use:   "drop",
		Short: "Drop the managed publications",
		Long: "Drop the managed publications wake_<source.name> and wake_<source.name>_insert if they exist.\n" +
			"An existing-mode publication (source.publication.name) is never altered.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadForDrop(*cfgPath)
			if err != nil {
				return err
			}
			names := pgsql.ManagedPublicationNames(cfg.Source.Name, true)
			if err = confirm(cmd, fmt.Sprintf("drop publications %v", names)); err != nil {
				return err
			}
			dsn, err := sourceDSN(&cfg)
			if err != nil {
				return err
			}
			pool, err := pgsql.NewPool(dsn, pgsql.WithMaxConns(1), pgsql.WithApplicationName(appName))
			if err != nil {
				return &exitError{code: exitConfig, err: withCode(codeConfigInvalid, err)}
			}
			defer pool.Close(context.WithoutCancel(cmd.Context()))
			pubs, err := pgsql.NewPublications(pool)
			if err != nil {
				return err
			}
			if err := pubs.Drop(cmd.Context(), cfg.Source.Name); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "dropped publications %v (those that existed)\n", names)
			if cfg.Source.Publication.Mode == config.PublicationExisting {
				fmt.Fprintf(cmd.OutOrStdout(), "existing-mode publication %s was not altered\n",
					cfg.Source.Publication.Name)
			}
			return nil
		},
	}
	c.Flags().Bool("yes", false, "confirm the destructive operation")
	return c
}

func loadForDrop(cfgPath string) (config.Config, error) {
	cfg, err := loadConfig(cfgPath, true)
	if err != nil {
		return config.Config{}, err
	}
	if err = requireSourceName(&cfg); err != nil {
		return config.Config{}, err
	}
	return cfg, nil
}
