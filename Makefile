# SPDX-License-Identifier: Apache-2.0
# Every target prints a one-line PASS/FAIL summary.
SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

PG         ?= 17
FUZZTIME   ?= 30s
IMAGE      ?= thnhdatle/wake:dev
# Release version shown in the acceptance report and the benchmark results.
RELEASE    ?= v0.1.0
COMPOSE    := docker compose -f deploy/docker-compose.yml
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
BUILDINFO  := github.com/thnhdatle/wake/internal/platform/buildinfo
LDFLAGS    := -s -w -X $(BUILDINFO).Version=$(VERSION) -X $(BUILDINFO).Commit=$(COMMIT) -X $(BUILDINFO).Date=$(BUILD_DATE)

export WAKE_TEST_PG := $(PG)
# Release builds are static (CGO off); the race detector needs cgo outside macOS.
export CGO_ENABLED ?= 0

# step <name> <command>: run the command and print a one-line PASS/FAIL summary.
# Commands passed here must not contain commas; put such logic in scripts/.
define step
if $(2); then echo "PASS  $(1)"; else echo "FAIL  $(1)" >&2; exit 1; fi
endef

.PHONY: bootstrap generate fmt lint test test-integration coverage mutation acceptance bench fuzz \
	examples quickstart-test security build docker up down ci release-check release-dry

bootstrap:
	@$(call step,bootstrap,scripts/check_tools.sh && go mod download && git config core.hooksPath .githooks \
		&& $(COMPOSE) pull --ignore-buildable)

generate:
	@$(call step,generate,if [ -n "$$(grep -rl 'go:generate' --include='*.go' . 2>/dev/null)" ]; \
		then go generate ./...; else echo "no go:generate directives"; fi)

fmt:
	@$(call step,fmt,go tool gofumpt -l -w .)

lint:
	@$(call step,lint,scripts/lint.sh)

test:
	@$(call step,test,CGO_ENABLED=1 go test -race -count=1 ./...)

test-integration:
	@$(call step,test-integration PG=$(PG),go test -tags integration -count=1 ./...)

coverage:
	@$(call step,coverage,PG=$(PG) scripts/coverage.sh)

mutation:
	@$(call step,mutation,scripts/mutation.sh)

acceptance:
	@$(call step,acceptance,go run ./acceptance/cmd/runner -version $(RELEASE))

bench:
	@$(call step,bench,RELEASE=$(RELEASE) scripts/bench.sh)

fuzz:
	@$(call step,fuzz,FUZZTIME=$(FUZZTIME) scripts/fuzz.sh)

examples:
	@$(call step,examples,scripts/examples.sh)

quickstart-test:
	@$(call step,quickstart-test,scripts/quickstart_test.sh)

security:
	@$(call step,security,IMAGE=$(IMAGE) scripts/security.sh)

build:
	@$(call step,build,go build -trimpath -ldflags '$(LDFLAGS)' -o bin/wake ./cmd/wake)

docker:
	@$(call step,docker,docker build -t $(IMAGE) .)

up:
	@$(call step,up,$(COMPOSE) up -d --wait)

down:
	@$(call step,down,$(COMPOSE) down -v)

ci:
	@$(MAKE) --no-print-directory lint test test-integration coverage examples security
	@echo "PASS  ci"

release-check:
	@$(call step,release-check,scripts/release_check.sh)

release-dry:
	@$(call step,release-dry,goreleaser release --snapshot --clean)
