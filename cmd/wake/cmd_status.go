// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/adapter/statefile"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func newStatusCmd(cfgPath *string) *cobra.Command {
	var addr string
	c := &cobra.Command{
		Use:   "status",
		Short: "Show slot, lag, snapshot and sink status",
		Long: "Print GET /v1/status of the running instance (admin.listen). If the admin endpoint is\n" +
			"unreachable, read the state file and pg_replication_slots directly and say so.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(*cfgPath, true)
			if err != nil {
				return err
			}
			if addr == "" {
				addr = cfg.Admin.Listen
			}
			body, err := httpadmin.FetchStatus(cmd.Context(), addr)
			if err == nil {
				return printJSON(cmd.OutOrStdout(), body)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "admin endpoint %s unreachable (%v); reading the state file %s and "+
				"pg_replication_slots directly\n", addr, err, cfg.State.Path)
			doc, err := fallbackStatus(cmd.Context(), &cfg)
			if err != nil {
				return err
			}
			out, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("encode status: %w", err)
			}
			return printJSON(cmd.OutOrStdout(), out)
		},
	}
	c.Flags().StringVar(&addr, "addr", "", "admin address (default: admin.listen from config)")
	return c
}

func fallbackStatus(ctx context.Context, cfg *config.Config) (*statusDoc, error) {
	if err := requireSourceName(cfg); err != nil {
		return nil, err
	}
	store, err := statefile.New(cfg.State.Path)
	if err != nil {
		return nil, withCode(codeConfigInvalid, err)
	}
	st, err := store.Load(ctx)
	if err != nil && !errors.Is(err, pipeline.ErrStateNotFound) {
		return nil, err
	}
	var (
		slot    pipeline.SlotStatus
		slotErr error
	)
	if dsn, derr := sourceDSN(cfg); derr != nil {
		slotErr = derr
	} else {
		slot, slotErr = querySlot(ctx, dsn, slotName(cfg))
	}
	doc := newStatusDoc(cfg, slot, slotErr, st)
	doc.Origin = "state_file"
	if slotErr == nil && slot.Exists {
		doc.ConfirmedLSN = slot.ConfirmedFlush.String()
	}
	return doc, nil
}

func querySlot(ctx context.Context, dsn, slot string) (pipeline.SlotStatus, error) {
	pool, err := pgsql.NewPool(dsn, pgsql.WithMaxConns(1), pgsql.WithApplicationName(appName))
	if err != nil {
		return pipeline.SlotStatus{}, err
	}
	defer pool.Close(context.WithoutCancel(ctx))
	cat, err := pgsql.NewCatalog(pool)
	if err != nil {
		return pipeline.SlotStatus{}, err
	}
	return cat.Slot(ctx, slot)
}

func printJSON(w io.Writer, raw []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return fmt.Errorf("format status: %w", err)
	}
	buf.WriteByte('\n')
	_, err := w.Write(buf.Bytes())
	return err //nolint:wrapcheck // writing to the command's own stdout
}
