#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Installs the tools that make ci needs on a Linux amd64 CI runner.
set -euo pipefail

GOLANGCI_LINT_VERSION=v2.14.0
GITLEAKS_VERSION=v8.30.1
KUBECONFORM_VERSION=v0.8.0
GOVULNCHECK_VERSION=v1.9.0
HADOLINT_VERSION=v2.15.1
TRIVY_VERSION=0.76.0

bin=${HOME}/.local/bin
mkdir -p "$bin"
export GOBIN=$bin

go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}"
go install "github.com/zricethezav/gitleaks/v8@${GITLEAKS_VERSION}"
go install "github.com/yannh/kubeconform/cmd/kubeconform@${KUBECONFORM_VERSION}"
go install "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}"

curl -sSfL -o "$bin/hadolint" \
	"https://github.com/hadolint/hadolint/releases/download/${HADOLINT_VERSION}/hadolint-Linux-x86_64"
chmod +x "$bin/hadolint"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -sSfL -o "$tmp/trivy.tgz" \
	"https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz"
tar -xzf "$tmp/trivy.tgz" -C "$bin" trivy

if [ -n "${GITHUB_PATH:-}" ]; then
	echo "$bin" >>"$GITHUB_PATH"
fi
echo "installed CI tools into $bin"
