// SPDX-License-Identifier: Apache-2.0

package pipeline

import "time"

// AlertState is the state an alert rule changed to.
type AlertState string

// Alert states.
const (
	AlertStateFiring   AlertState = "firing"
	AlertStateResolved AlertState = "resolved"
)

// Alert is one alert state change, posted to lag.alert_webhook as
// {"rule","state","value","threshold","ts"}.
type Alert struct {
	// Rule names the rule, as in wake_alert_active{rule}.
	Rule      string
	State     AlertState
	Value     float64
	Threshold float64
	Ts        time.Time
}
