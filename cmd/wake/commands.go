// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/platform/buildinfo"
)

// exitError carries a specific process exit code.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

func newRootCmd() *cobra.Command {
	var cfgPath string
	root := &cobra.Command{
		Use:           "wake",
		Short:         "Wake streams Postgres changes to Kafka, NATS and webhooks",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&cfgPath, "config", os.Getenv("WAKE_CONFIG"),
		"path to the YAML config file (env WAKE_CONFIG)")

	run := &cobra.Command{Use: "run", Short: "Run the CDC service", Args: cobra.NoArgs}
	var reinit bool
	run.Flags().BoolVar(&reinit, "reinit-state", false,
		"start although the state file is missing, belongs to another slot or system, or its slot is missing "+
			"(reseeds schema versions, discards snapshot progress; see docs/guides/errors.md)")
	run.RunE = func(cmd *cobra.Command, _ []string) error {
		return runService(cmd.Context(), runOptions{cfgPath: cfgPath, reinit: reinit})
	}

	snapshot := &cobra.Command{Use: "snapshot", Short: "Snapshot operations"}
	snapshot.AddCommand(newSnapshotTableCmd(&cfgPath))

	slot := &cobra.Command{Use: "slot", Short: "Replication slot operations"}
	slot.AddCommand(newSlotDropCmd(&cfgPath))

	pub := &cobra.Command{Use: "publication", Short: "Publication operations"}
	pub.AddCommand(newPublicationDropCmd(&cfgPath))

	root.AddCommand(
		run,
		newStatusCmd(&cfgPath),
		newCheckCmd(&cfgPath),
		snapshot,
		newCacheInvalidatorCmd(&cfgPath),
		slot, pub, newConfigCmd(&cfgPath),
		newHealthcheckCmd(&cfgPath),
		newVersionCmd(),
	)
	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintln(cmd.OutOrStdout(), buildinfo.Get())
		},
	}
}

func newHealthcheckCmd(cfgPath *string) *cobra.Command {
	var addr string
	c := &cobra.Command{
		Use:   "healthcheck",
		Short: "Exit 0 when the admin /readyz returns 200",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if addr == "" {
				cfg, err := loadConfig(*cfgPath, true)
				if err != nil {
					return err
				}
				addr = cfg.Admin.Listen
			}
			return httpadmin.Healthcheck(cmd.Context(), addr)
		},
	}
	c.Flags().StringVar(&addr, "addr", "",
		"admin address to probe (default: admin.listen from config, else "+config.DefaultAdminListen+")")
	return c
}
