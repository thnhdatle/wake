#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Pre-release checks, run by `make release-check` before tagging.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
problem() {
	echo "release-check: $*" >&2
	fail=1
}

hidden=$(git ls-files | grep -E '^\.[^/]+/' | grep -vE '^\.(github|githooks)/' || true)
[ -z "$hidden" ] || problem "unexpected hidden files are tracked: $(echo "$hidden" | tr '\n' ' ')"
markdown=$(git ls-files '*.md' | grep -v / |
	grep -vE '^(README|CHANGELOG|CONTRIBUTING|CODE_OF_CONDUCT|SECURITY|ROADMAP)\.md$' || true)
[ -z "$markdown" ] || problem "unexpected top-level Markdown files: $(echo "$markdown" | tr '\n' ' ')"

re='^(feat|fix|perf|refactor|test|docs|build|ci|chore)(\([a-z0-9-]+\))?!?: [a-z0-9].{0,70}[^.]$'
while read -r sha; do
	[ -n "$sha" ] || continue
	body=$(git log -1 --format=%B "$sha" | sed -e :a -e '/^[[:space:]]*$/{$d;N;ba' -e '}')
	if [ "$(printf '%s\n' "$body" | wc -l)" -gt 1 ]; then
		problem "commit ${sha:0:9} has more than one line"
	fi
	subject=$(printf '%s\n' "$body" | head -n1)
	if ! [[ $subject =~ $re ]]; then
		problem "commit ${sha:0:9} subject does not match the format: $subject"
	fi
done < <(git rev-list HEAD 2>/dev/null || true)

todos=$(git ls-files --cached --others --exclude-standard -- '*.go' '*.sh' '*.yml' '*.yaml' 'Makefile' |
	grep -v '^scripts/release_check.sh$' |
	xargs grep -nE 'TODO' 2>/dev/null |
	grep -vE 'TODO\((roadmap|#[0-9]+)\)' || true)
[ -z "$todos" ] || problem "bare TODO found:
$todos"

while read -r f; do
	[ -n "$f" ] || continue
	head -n 5 "$f" | grep -q 'SPDX-License-Identifier: Apache-2.0' || problem "missing SPDX header: $f"
done < <(git ls-files --cached --others --exclude-standard -- '*.go')

for f in README.md LICENSE NOTICE CONTRIBUTING.md CODE_OF_CONDUCT.md SECURITY.md CHANGELOG.md ROADMAP.md \
	.github/CODEOWNERS .github/dependabot.yml .github/PULL_REQUEST_TEMPLATE.md \
	.github/ISSUE_TEMPLATE/bug_report.yml .github/ISSUE_TEMPLATE/feature_request.yml \
	.github/ISSUE_TEMPLATE/config.yml .github/workflows/ci.yml .github/workflows/release.yml \
	.github/workflows/nightly.yml; do
	[ -f "$f" ] || problem "missing community file: $f"
done

latest=$(grep -m1 -E '^## \[[0-9]' CHANGELOG.md || true)
[[ $latest =~ ^##\ \[[0-9]+\.[0-9]+\.[0-9]+\]\ -\ [0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] ||
	problem "CHANGELOG release section is not dated: ${latest:-none}"

exit "$fail"
