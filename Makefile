# Root Makefile: runs the shared targets in every module, builds the
# distribution, and validates the examples.
SHELL := /bin/bash
ROOT  := $(abspath .)
GO    ?= go

# component modules first, then the root module (examples and end to end tests)
MODULES := extension/budgetextension processor/budgetprocessor .
COMMON  := $(MAKE) --no-print-directory -f $(ROOT)/Makefile.Common

BUILDER_CONFIG := cmd/otelcol-budget/builder-config.yaml
DIST           := bin/otelcol-budget
REDIS_MODULE   := github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/redisstorageextension
# REDIS_FORK=<module>@<version> or a local path replaces the redis_storage
# build pinned in the manifest (contrib PR #51568)
REDIS_FORK     ?=

.PHONY: all
all: generate lint test build

.PHONY: tools
tools:
	@$(COMMON) tools

.PHONY: generate lint test cover bench tidy
generate lint test cover bench tidy:
	@set -e; for m in $(MODULES); do echo "== $@ $$m"; $(COMMON) -C $$m $@; done

lint: dashes
.PHONY: dashes
dashes:
	@! LC_ALL=C grep -rnE "$$(printf '\xe2\x80\x93|\xe2\x80\x94')" --include='*.go' --include='*.md' --include='*.yaml' \
		--include='*.sh' --include='*.json' --exclude-dir=bin . \
		|| (echo "em or en dash found, use commas, periods, or parentheses" && false)

# Extension tests against the redis_storage pinned in the OCB manifest
# (contrib PR #51568), through an alternate go.mod so go.mod stays on the
# released version. TestRealRedisStorage then runs in increment mode.
REDIS_PR := $(shell awk '/redisstorageextension =>/ {print $$4 "@" $$5}' $(BUILDER_CONFIG))
# with_redis_pr runs a go command in the extension module against REDIS_PR
define with_redis_pr
	@tmp=$$(mktemp -d) && cd extension/budgetextension && cp go.mod $$tmp/pr.mod && cp go.sum $$tmp/pr.sum && \
		$(GO) mod edit -modfile=$$tmp/pr.mod -replace=$(REDIS_MODULE)=$(REDIS_PR) && \
		echo "redis_storage: $(REDIS_PR)" && \
		$(1) -modfile=$$tmp/pr.mod -mod=mod $(2) ; rc=$$?; rm -rf $$tmp; exit $$rc
endef

.PHONY: test-redis-pr
test-redis-pr:
	$(call with_redis_pr,$(GO) test,-race ./...)

# Redis load, increment versus G-Counter, against a throwaway Redis container
.PHONY: results-redis
results-redis:
	@docker rm -f otelcol-budget-results-redis >/dev/null 2>&1 || true
	@docker run -d --name otelcol-budget-results-redis -p 127.0.0.1:26390:6379 valkey/valkey:8-alpine >/dev/null
	$(call with_redis_pr,REDIS_ADDR=127.0.0.1:26390 $(GO) test,-tags results -run TestResultsRedisLoad -v -timeout 30m ./internal/sync/ | grep -v '^=== ') ; \
		rc=$$?; docker rm -f otelcol-budget-results-redis >/dev/null; exit $$rc

# in-process measurements for docs/results.md
.PHONY: results
results:
	cd extension/budgetextension && $(GO) test -tags results -run 'TestResults' -v . | grep -v '^=== '
	$(GO) test -tags results -run 'TestResults' -v ./examples/ | grep -vE '^=== |^[0-9]{4}-[0-9]{2}-[0-9]{2}T'

.PHONY: build
build:
	@$(COMMON) $(ROOT)/bin/builder
	@mkdir -p $(dir $(DIST)) && cp $(BUILDER_CONFIG) $(DIST).yaml.tmp
	@if [ -n "$(REDIS_FORK)" ]; then \
		sed -i.bak 's#^\(  - $(REDIS_MODULE) => \).*#\1$(subst @, ,$(REDIS_FORK))#' $(DIST).yaml.tmp && rm -f $(DIST).yaml.tmp.bak; \
		echo "using redis_storage $(REDIS_FORK)"; \
	fi
	$(ROOT)/bin/builder --config=$(DIST).yaml.tmp
	@rm -f $(DIST).yaml.tmp
	@echo "binary: $(DIST)/otelcol-budget"

.PHONY: validate-examples
validate-examples: build
	@for f in examples/*.yaml; do POD_NAME=otelcol-gw-0 GATEWAY_REPLICAS=3 $(DIST)/otelcol-budget validate --config=$$f || exit 1; done

.PHONY: clean
clean:
	rm -rf $(DIST) coverage.out
	@for m in $(MODULES); do rm -f $$m/coverage.out; done
