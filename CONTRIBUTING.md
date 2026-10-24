# Contributing to Wake

Build, test and propose changes to Wake.

## Prerequisites

- Go, the version in `go.mod` (downloaded by the toolchain directive)
- Docker with the Compose plugin (Docker Desktop, OrbStack, Colima or Linux)
- GNU make
- the tools below (macOS commands); `make bootstrap` lists what is missing

```bash
brew install golangci-lint goreleaser gitleaks hadolint shellcheck kubeconform trivy jq
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
brew install syft   # only for make release-dry
```

CI uses `scripts/install_ci_tools.sh`. Other Go tools are pinned in `go.mod`: run `go tool <name>`.

## Getting started

```bash
git clone https://github.com/thnhdatle/wake.git
cd wake
make bootstrap   # checks tools, downloads modules, enables .githooks, pulls compose images
make lint test   # must be green before every commit
```

The hooks check the commit format and run gofumpt, golangci-lint and gitleaks.

## Makefile targets

Every target prints a one-line PASS or FAIL summary.

| Target | Purpose |
|---|---|
| `make bootstrap` | check tools, modules, hooks, images |
| `make generate` | run code generators |
| `make fmt` | gofumpt |
| `make lint` | golangci-lint, hadolint, shellcheck, gitleaks, kubeconform |
| `make test` | unit tests, race detector |
| `make test-integration PG=17` | integration tests, Postgres 14 to 17 |
| `make coverage` | coverage thresholds (repo 80%, domain and app 90%) |
| `make fuzz FUZZTIME=30s` | every fuzz target |
| `make examples` | run examples, check output |
| `make quickstart-test` | README quickstart in a clean clone |
| `make security` | govulncheck, go-licenses, trivy |
| `make build` | binary in `./bin/wake` |
| `make docker` | image `thnhdatle/wake:dev` |
| `make up` / `make down` | start / stop the compose stack |
| `make ci` | `lint test test-integration coverage examples security` |
| `make release-check` | pre-release checks |
| `make release-dry` | GoReleaser snapshot build |

## Test levels

| Level | Where | Run |
|---|---|---|
| Unit | next to the code | `make test` |
| Integration | `*_integration_test.go`, `//go:build integration` | `make test-integration PG=<14\|15\|16\|17>` |
| Fuzz | next to parsers, corpus in `testdata/fuzz` | `make fuzz` |

Unit tests use in-memory fakes and `t.Parallel()`. Integration tests see the Postgres version as `WAKE_TEST_PG`.

## Commit format

```text
<type>(<scope>): <summary>
```

- One line only: no body, no footer, no trailers. 72 characters at most.
- Types: feat, fix, perf, refactor, test, docs, build, ci, chore.
- Scope: optional, lowercase letters, digits and hyphens (`pgoutput`, `kafka`, `deps`).
- Summary: imperative, lowercase first letter, no trailing period.
- Breaking change: add `!`, as in `feat(api)!: rename route field`.

The `commit-msg` hook rejects anything else. Each commit is one logical change and passes
`make lint test`.

## Adding a dependency

A new dependency must:

- have a release in the last 12 months;
- use one of Apache-2.0, MIT, MIT-CMU, BSD-2-Clause, BSD-3-Clause, ISC, MPL-2.0, 0BSD, Zlib,
  Unlicense, CC0-1.0;
- bring in no GPL or AGPL transitive dependency.

`make security` checks licenses. Explain the choice in the PR.

## Proposing a design change

For an architectural change, open an issue before the PR: problem, proposal, alternatives,
consequences.

## Pull request checklist

- Tests at the right level
- `make lint test` passes (plus `make test-integration` for adapter changes)
- Docs and `CHANGELOG.md` (Unreleased) updated
- Single-line commit messages
- Layering: `internal/domain` imports only the standard library, `internal/app` only domain and
  platform; adapters never import each other

## Questions

Usage questions go to GitHub Discussions. Bugs and proposals go to issues. Report security
problems privately, see [SECURITY.md](SECURITY.md).
