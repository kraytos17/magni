SRC_DIR     := src
TEST_DIR    := tests
BUILD_DIR   := build
ODIN        ?= odin
COLLECTIONS := -collection:src=$(SRC_DIR)

DEBUG_FLAGS   := -debug -o:none -warnings-as-errors -use-separate-modules
RELEASE_FLAGS := -o:aggressive -lto:thin -no-bounds-check -no-type-assert \
                 -disable-assert -microarch:native -source-code-locations:none
TEST_FLAGS    := -debug -o:none -warnings-as-errors -use-separate-modules \
                 -define:ODIN_TEST_THREADS=1

# ── fuzz defaults (override: make fuzz-campaign FUZZ_SECONDS=60) ────
FUZZ_WORKERS ?= 4
FUZZ_SECONDS ?= 3600
FUZZ_OUT     ?= fuzz/afl-output
# MAGNI_FUZZ_OUT takes precedence if set in env

.DEFAULT_GOAL := help

.PHONY: all build release run rebuild clean clean-all
.PHONY: test test-verbose test-single test-cli test-cli-full smoke quick ci
.PHONY: check vet vet-all
.PHONY: perf bench
.PHONY: fuzz-build fuzz-test fuzz-corpus fuzz-run fuzz-campaign fuzz-status fuzz-stop
.PHONY: fuzz-showmap fuzz-cmin fuzz-promote fuzz-clean fuzz-one
.PHONY: help

all: build ## default: build debug binary

build: ## build debug binary → build/magni
	@python3 magni.py build

release: ## build release binary (LTO, no checks) → build/magni_release
	@python3 magni.py build --release

run: build ## build debug and run
	@./$(BUILD_DIR)/magni

rebuild: clean build ## clean and rebuild debug

clean: ## remove build/ only
	@python3 magni.py clean

clean-all: clean fuzz-clean ## remove build/ + fuzz artifacts (afl-output, build, corpus pycache)

test: ## run all Odin tests (debug)
	@python3 magni.py test

test-verbose: ## run tests with verbose output (no fancy)
	@python3 magni.py test --verbose

test-single: ## run one test: make test-single NAME=test_foo
	@test -n "$(NAME)" || { echo "usage: make test-single NAME=<test_name>" >&2; exit 2; }
	@python3 magni.py test --name "$(NAME)"

test-cli: build ## basic CLI smoke checks (needs build/magni)
	@python3 magni.py test-cli

test-cli-full: build ## comprehensive CLI integration tests
	@python3 magni.py test-cli --full

smoke: test test-cli ## quick smoke: unit tests + CLI

quick: check test ## fast gate: check + test (no vet/fuzz)

ci: vet-all test fuzz-test ## CI gate: vet-all + test + fuzz corpus ASan
	@echo "ci: all gates passed"

check: ## parse + type check (no vet)
	@python3 magni.py build --check-only

vet: ## vet (fast, no LLVM): vet | vet FLAGS=shadowing
	@python3 magni.py vet $(VET_FLAGS)

vet-all: ## vet via build+test (LLVM, strict-style, shadowing)
	@python3 magni.py vet --all

perf: ## run timing baseline (release flags)
	$(ODIN) run tests/perf $(COLLECTIONS) $(RELEASE_FLAGS)

bench: perf ## alias for perf

fuzz-corpus: ## regenerate seed corpus → fuzz/corpus/
	@python3 magni.py corpus generate

fuzz-build: ## build ASan fuzz target → fuzz/fuzz_target
	@python3 magni.py build --asan

fuzz-cov: ## build coverage-instrumented target → fuzz/fuzz_target_cov
	@python3 magni.py build --cov

fuzz-test: fuzz-build ## run every corpus seed under ASan (regression gate)
	@python3 magni.py corpus test

fuzz-run: fuzz-cov ## launch interactive AFL++ campaign (Ctrl-C to stop, pass ARGS=" -V 60")
	@python3 magni.py fuzz run -- $(ARGS)

fuzz-campaign: fuzz-cov ## headless parallel campaign (FUZZ_WORKERS=4 FUZZ_SECONDS=3600)
	@python3 magni.py fuzz campaign -w $(FUZZ_WORKERS) -s $(FUZZ_SECONDS) -- $(ARGS)
	@echo "campaign: $(FUZZ_WORKERS) workers, $(FUZZ_SECONDS)s, out=$(or $(MAGNI_FUZZ_OUT),$(FUZZ_OUT))"

fuzz-status: ## show per-worker fuzzer_stats
	@python3 magni.py fuzz status

fuzz-stop: ## pkill afl-fuzz
	@python3 magni.py fuzz stop

fuzz-clean: ## remove fuzz artifacts (afl-output, build, pycache)
	@python3 magni.py clean --fuzz-only

fuzz-showmap: fuzz-cov ## show coverage tuples for current corpus
	@python3 magni.py fuzz showmap

fuzz-cmin: fuzz-cov ## minimize corpus (-i fuzz/corpus -o /tmp/min)
	@python3 magni.py fuzz cmin

fuzz-promote: fuzz-cov ## merge grown queue → update gen_corpus.py
	@python3 magni.py corpus promote

fuzz-promote-min: fuzz-cov ## merge + afl-cmin minimize → update gen_corpus.py
	@python3 magni.py corpus promote --minimize

fuzz-promote-test: fuzz-cov ## merge → update gen_corpus.py → regenerate → ASan gate
	@python3 magni.py corpus promote --minimize
	@python3 magni.py corpus generate
	@python3 magni.py corpus test

fuzz-one: fuzz-build ## repro one crash: make fuzz-one FILE=fuzz/afl-output/.../id:000000
	@test -n "$(FILE)" || { echo "usage: make fuzz-one FILE=<path>" >&2; exit 2; }
	@ASAN_OPTIONS=detect_leaks=0:abort_on_error=1 ./fuzz/fuzz_target "$(FILE)"

help: ## show this help
	@python3 magni.py help
