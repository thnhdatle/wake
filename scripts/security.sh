#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# govulncheck, go-licenses (allowlist below, see CONTRIBUTING.md) and trivy on the local image.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
. scripts/lib.sh

IMAGE=${IMAGE:-thnhdatle/wake:dev}
ALLOWED='Apache-2.0,MIT,MIT-CMU,BSD-2-Clause,BSD-3-Clause,ISC,MPL-2.0,0BSD,Zlib,Unlicense,CC0-1.0'

if has_pkgs; then
	need govulncheck
	govulncheck ./...
	go tool go-licenses check ./... --ignore github.com/thnhdatle/wake --allowed_licenses="$ALLOWED"
else
	echo "security: no Go packages, skipping govulncheck and go-licenses"
fi

if command -v docker >/dev/null 2>&1 && docker image inspect "$IMAGE" >/dev/null 2>&1; then
	need trivy
	trivy image --quiet --severity CRITICAL,HIGH --ignore-unfixed --exit-code 1 "$IMAGE"
else
	echo "security: image $IMAGE not built, skipping trivy (run make docker first)"
fi
