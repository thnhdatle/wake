# Contributing to Wake

Thank you for helping. This guide covers what you need to build, test and propose changes.

## Prerequisites

- Go (the version pinned in `go.mod`; the toolchain directive downloads it automatically)
- Docker: Docker Desktop, OrbStack, Colima or any Linux Docker engine with the Compose plugin.
  Integration and acceptance tests start Postgres, Kafka, NATS and Redis containers.
- `golangci-lint`, `goreleaser`, `gitleaks`, `hadolint`, `shellcheck`, `kubeconform`, `trivy`,
  `govulncheck` and `jq` (on macOS:
  `brew install golangci-lint goreleaser gitleaks hadolint shellcheck kubeconform trivy jq` and
  `go install golang.org/x/vuln/cmd/govulncheck@v1.9.0`). CI installs the same versions with
  `scripts/install_ci_tools.sh`; `make bootstrap` lists what is missing.
- `syft` for SBOMs in `make release-dry` (`brew install syft`)
- GNU make

Go development tools (gofumpt, gremlins, gotestsum, benchstat, go-licenses) are pinned in `go.mod`
and run with `go tool <name>`. Nothing is installed globally.

## Getting started

```bash
git clone https://github.com/thnhdatle/wake.git
cd wake
make bootstrap   # checks tools, downloads modules, enables .githooks, pulls compose images
make lint test   # must be green before every commit
```

`make bootstrap` points `core.hooksPath` at `.githooks`: the `commit-msg` hook validates the commit
format and `pre-commit` runs gofumpt, golangci-lint on new code, and gitleaks.

## Makefile targets

Every target prints a one-line PASS or FAIL summary.

| Target | Purpose |
|---|---|
| `make bootstrap` | check tools, download modules, install hooks, pull images |
| `make generate` | run code generators |
| `make fmt` | format Go code with gofumpt |
| `make lint` | golangci-lint, hadolint, shellcheck, gitleaks, kubeconform |
| `make test` | unit tests with the race detector |
| `make test-integration PG=17` | integration tests against Postgres 14, 15, 16 or 17 (Testcontainers) |
| `make coverage` | merged coverage with thresholds (repo 80%, domain and app 90%) |
| `make mutation` | mutation tests on the core packages listed in `scripts/mutation.sh` (score 60%) |
| `make acceptance` | [acceptance suite](acceptance/README.md); `CI=1` uses CI thresholds, `ONLY=<id>,<id>` runs a subset |
| `make bench` | benchmarks |
| `make fuzz FUZZTIME=30s` | every fuzz target |
| `make examples` | run every example and check its output |
| `make quickstart-test` | run the README quickstart in a clean clone |
| `make security` | govulncheck, go-licenses, trivy on the local image |
| `make build` | binary in `./bin/wake` with version information |
| `make docker` | local image `thnhdatle/wake:dev` |
| `make up` / `make down` | start / stop the compose stack |
| `make ci` | `lint test test-integration coverage examples security` |
| `make release-check` | pre-release checks |
| `make release-dry` | GoReleaser snapshot build |

## Test levels

- Unit tests sit next to the code, run with `make test`, use in-memory fakes and `t.Parallel()`.
- Integration tests are `*_integration_test.go` files with `//go:build integration`; run
  `make test-integration PG=<14|15|16|17>`. The variable is exported to tests as `WAKE_TEST_PG`.
- Acceptance tests live in `acceptance/` and run the real stack under load and failure injection
  ([acceptance/README.md](acceptance/README.md)). Docker Desktop or any Linux Docker works. A full `make acceptance` run takes about 45 to 60 minutes;
  use `ONLY=` to iterate on a subset. Do not run other compose stacks at the same time.
- Fuzz targets live next to parsers and decoders with a seed corpus in `testdata/fuzz`.

## Commit format

One line only: no body, no footer, no trailers.

```text
<type>(<scope>): <summary>
```

- `type`: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`
- `scope`: the package or area, for example `pgoutput`, `snapshot`, `kafka`, `cache`, `deps`
- `summary`: imperative, lowercase first letter, no trailing period, 72 characters at most for the line
- breaking changes: `feat(api)!: rename route field`

Each commit is one logical change and passes `make lint test`.

## Adding a dependency

A new dependency must be maintained (a release in the last 12 months), carry one of the licenses
Apache-2.0, MIT, MIT-CMU, BSD-2-Clause, BSD-3-Clause, ISC, MPL-2.0, 0BSD, Zlib, Unlicense or CC0-1.0,
and bring in no GPL or AGPL transitive dependency. `make security` runs `go-licenses check ./...`.
Record the reasoning in the pull request.

## Proposing a design change

Open an issue describing the problem and the proposed change, with the alternatives considered and
the consequences, before opening a pull request for an architectural change.

## Pull request checklist

- Tests for the change, at the right level
- `make lint test` passes (and `make test-integration` for adapter changes)
- Docs and `CHANGELOG.md` (Unreleased) updated
- Single-line commit messages in the format above
- Code follows the layering rules: `internal/domain` imports only the standard library, `internal/app`
  imports only domain and platform, adapters never import each other

## Questions

Use GitHub Discussions for usage questions and issues for bugs and proposals. Report security problems
privately as described in [SECURITY.md](SECURITY.md).
