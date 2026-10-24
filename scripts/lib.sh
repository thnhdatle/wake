#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Shared helpers for scripts/*.sh.

has_go() {
	[ -n "$(find . -name '*.go' -not -path './.*/*' -print -quit)" ]
}

has_pkgs() {
	[ -n "$(go list ./... 2>/dev/null)" ]
}

need() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing tool: $1" >&2
		exit 1
	fi
}
