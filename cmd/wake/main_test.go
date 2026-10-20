// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package main

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
