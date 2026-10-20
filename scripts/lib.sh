#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Shared helpers for scripts/*.sh. Source this file; do not execute it.

# has_go reports whether the repository contains any Go source file.
has_go() {
	[ -n "$(find . -name '*.go' -not -path './.*/*' -print -quit)" ]
}

# has_pkgs reports whether `go list ./...` resolves to at least one package.
has_pkgs() {
	[ -n "$(go list ./... 2>/dev/null)" ]
}

# need prints an error and exits when a tool is missing.
need() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing tool: $1" >&2
		exit 1
	fi
}
