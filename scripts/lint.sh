#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs every linter of `make lint`; skips those with nothing to check.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
. scripts/lib.sh

if has_go; then
	need golangci-lint
	golangci-lint run
else
	echo "lint: no Go files, skipping golangci-lint"
fi

for dockerfile in Dockerfile Dockerfile.release; do
	if [ -f "$dockerfile" ]; then
		need hadolint
		hadolint "$dockerfile"
	fi
done

need shellcheck
shopt -s nullglob
shell_files=(scripts/*.sh .githooks/*)
if [ "${#shell_files[@]}" -gt 0 ]; then
	shellcheck "${shell_files[@]}"
fi

need gitleaks
gitleaks detect --no-banner --redact

if [ -d deploy/kubernetes ]; then
	need kubeconform
	kubeconform -strict -summary -ignore-filename-pattern 'kustomization\.yaml$' deploy/kubernetes
fi
