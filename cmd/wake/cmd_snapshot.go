// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/watermark"
)

func newSnapshotTableCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "table <schema.table>",
		Short: "Snapshot one table",
		Long: "Request a table snapshot: writes the transactional logical message\n" +
			"pg_logical_emit_message(true, 'wake.snapshot', '{\"table\":\"<schema>.<table>\"}') with the source DSN.\n" +
			"The running instance, or the next one to start, picks it up from the stream; requests for\n" +
			"uncaptured tables are logged and ignored. <schema.table> uses the Postgres names.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			trigger := watermark.Trigger{Table: args[0]}
			payload, err := trigger.MarshalJSON()
			if err != nil {
				return &exitError{code: exitConfig, err: withCode(codeConfigInvalid,
					fmt.Errorf("table %q must have the form <schema>.<table>: %w", args[0], err))}
			}
			cfg, err := loadConfig(*cfgPath, true)
			if err != nil {
				return err
			}
			dsn, err := sourceDSN(&cfg)
			if err != nil {
				return err
			}
			at, err := emitTrigger(cmd.Context(), dsn, payload)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "snapshot of %s requested at LSN %s; the running instance (or the "+
				"next one to start) runs it\n", trigger.Table, at)
			return nil
		},
	}
}

func emitTrigger(ctx context.Context, dsn string, payload []byte) (string, error) {
	pool, err := pgsql.NewPool(dsn, pgsql.WithMaxConns(1), pgsql.WithApplicationName(appName))
	if err != nil {
		return "", &exitError{code: exitConfig, err: withCode(codeConfigInvalid, err)}
	}
	defer pool.Close(context.WithoutCancel(ctx))
	emitter, err := pgsql.NewEmitter(pool)
	if err != nil {
		return "", err
	}
	at, err := emitter.EmitMessage(ctx, watermark.TriggerPrefix, payload)
	if err != nil {
		return "", err
	}
	return at.String(), nil
}

func requireSourceName(cfg *config.Config) error {
	if cfg.Source.Name == "" {
		return &exitError{code: exitConfig, err: withCode(codeConfigInvalid,
			errors.New("source.name: required"))}
	}
	return nil
}
