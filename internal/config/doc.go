// SPDX-License-Identifier: Apache-2.0

// Package config loads, merges and validates the Wake configuration
// (docs/guides/configuration.md).
//
// Sources, highest precedence first: CLI flag overrides, WAKE_<PATH> env vars
// (EnvName), the YAML file, defaults (Default). Unknown keys are errors.
// Durations take Go strings plus a "d" suffix ("30s", "14d", "1d12h"); sizes
// take bytes with a binary suffix ("64MiB"). Secrets are never values here:
// *_env keys name environment variables and *_file keys name files.
//
// Validate reports every problem at once as a joined list of *ValidationError
// whose Path names the offending key.
//
//	cfg, err := config.Load("wake.yaml")
//	if err != nil {
//		return err // lists every problem; the CLI exits 2
//	}
//	topic, err := config.TopicName("cdc", "public", "users")
package config
