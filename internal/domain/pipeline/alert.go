// SPDX-License-Identifier: Apache-2.0

package pipeline

import "time"

type AlertState string

const (
	AlertStateFiring   AlertState = "firing"
	AlertStateResolved AlertState = "resolved"
)

type Alert struct {
	Rule      string
	State     AlertState
	Value     float64
	Threshold float64
	Ts        time.Time
}
