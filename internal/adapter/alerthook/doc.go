// SPDX-License-Identifier: Apache-2.0

// Package alerthook implements pipeline.AlertNotifier: it POSTs
// one alert state change as JSON
//
//	{"rule":"slot_lag_warn","state":"firing","value":9.4e6,"threshold":8388608,"ts":"2026-10-18T12:00:00Z"}
//
// to lag.alert_webhook. The request is bounded by a short timeout (default
// DefaultTimeout, at most MaxTimeout, well under lag.interval); a non-2xx
// response or a transport error is returned, and the lag monitor retries on
// its next poll.
//
// Main type: Notifier.
//
//	n, err := alerthook.New(cfg.Lag.AlertWebhook)
//	if err != nil {
//		return err
//	}
//	err = n.Notify(ctx, alert)
package alerthook
