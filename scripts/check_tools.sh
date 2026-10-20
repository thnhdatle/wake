#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Prints the version of every external tool and lists what is missing.
set -euo pipefail

missing=0
check() {
	local name=$1
	shift
	if command -v "$name" >/dev/null 2>&1; then
		printf '  %-14s %s\n' "$name" "$("$@" 2>&1 | head -n1)"
	else
		printf '  %-14s MISSING\n' "$name"
		missing=$((missing + 1))
	fi
}

echo "tool versions:"
check go go version
check golangci-lint golangci-lint version
check goreleaser goreleaser --version
check gitleaks gitleaks version
check hadolint hadolint --version
check shellcheck shellcheck --version
check kubeconform kubeconform -v
check trivy trivy --version
check govulncheck govulncheck -version
check docker docker --version
check jq jq --version

if [ "$missing" -gt 0 ]; then
	echo "$missing tool(s) missing; see Prerequisites in CONTRIBUTING.md" >&2
	exit 1
fi
