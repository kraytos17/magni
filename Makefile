SRC_DIR     := src
TEST_DIR    := tests
TARGET_DIR  := target
TARGET_DEBUG := $(TARGET_DIR)/debug
ODIN        ?= odin
COLLECTIONS := -collection:src=$(SRC_DIR)

RELEASE_FLAGS := -o:aggressive -lto:thin -no-bounds-check -no-type-assert \
                 -disable-assert -microarch:native -source-code-locations:none

FUZZ_WORKERS ?= 4
FUZZ_SECONDS ?= 3600
FUZZ_OUT     ?= fuzz/afl-output
# ROLES: comma-separated worker roles (default: master + rotation).
# Full list is single-sourced in magni/roles.py; see `python3 magni.py help`.
# Roles: master,explore,fast,coe,seek,cmplog,asan,laf,mopt,oldq,exec,grammar,exec_grammar
# e.g. make fuzz-campaign ROLES="master,cmplog,asan,explore"
ROLES ?=

.DEFAULT_GOAL := help

.PHONY: all build release run rebuild clean clean-all
.PHONY: test test-verbose test-single test-py test-cli test-cli-full smoke quick ci
.PHONY: check vet vet-all lint
.PHONY: perf bench census
.PHONY: fuzz-build fuzz-test fuzz-exec-build fuzz-exec-test
.PHONY: fuzz-corpus fuzz-exec-corpus fuzz-cov fuzz-run fuzz-campaign fuzz-status fuzz-stop
.PHONY: fuzz-promote fuzz-clean fuzz-one fuzz-one-exec fuzz-grammar-test
.PHONY: help

all: build ## default: build debug binary

build: ## build debug binary → target/debug/magni
	@python3 magni.py build

release: ## build release binary (LTO, no checks) → target/release/magni
	@python3 magni.py build --release

run: build ## build debug and run
	@./$(TARGET_DEBUG)/magni

rebuild: clean build ## clean and rebuild debug

clean: ## remove target/debug + target/release only
	@python3 magni.py clean

clean-all: clean fuzz-clean ## remove target/ + fuzz artifacts (afl-output, corpus pycache)

test: ## run all Odin tests (debug)
	@python3 magni.py test

test-verbose: ## run tests with verbose output (no fancy)
	@python3 magni.py test --verbose

test-single: ## run one test: make test-single NAME=test_foo
	@test -n "$(NAME)" || { echo "usage: make test-single NAME=<test_name>" >&2; exit 2; }
	@python3 magni.py test --name "$(NAME)"

test-py: ## run magni.py's own unit tests (no Odin/AFL needed)
	@python3 magni.py test-py

test-cli: build ## basic CLI smoke checks (needs target/debug/magni)
	@python3 magni.py test-cli

test-cli-full: build ## comprehensive CLI integration tests (needs target/debug/magni)
	@python3 magni.py test-cli --full

smoke: test test-cli ## quick smoke: unit tests + CLI

quick: check test test-py ## fast gate: check + test + orchestrator unit tests (no vet/fuzz)

ci: vet-all test test-py fuzz-test fuzz-exec-test ## CI gate: vet-all + test + test-py + fuzz parser+exec corpora ASan
	@echo "ci: all gates passed"

check: ## parse + type check (no vet)
	@python3 magni.py build --check-only

vet: ## fast vet (odin check, full flags); extra: vet --shadowing|--unused|--style|--cast|--semicolon via magni.py vet
	@python3 magni.py vet

vet-all: ## vet via build+test (LLVM, strict-style, shadowing)
	@python3 magni.py vet --all

lint: ## ruff check + format check on tooling (needs ruff)
	@ruff check magni/ tests_magni/
	@ruff format --check magni/ tests_magni/

perf: ## run timing baseline (release flags)
	$(ODIN) run tests/perf $(COLLECTIONS) $(RELEASE_FLAGS)

census: ## per-query allocation census: counts/bytes, heap vs temp (counts only, not timing)
	$(ODIN) run tests/census $(COLLECTIONS) $(RELEASE_FLAGS)

bench: perf ## alias for perf

fuzz-corpus: ## regenerate seed corpus → fuzz/corpus/
	@python3 magni.py corpus generate

fuzz-exec-corpus: ## regenerate exec scripts → fuzz/corpus_exec/
	@python3 magni.py corpus generate --exec

fuzz-build: ## build ASan fuzz target → target/fuzz/fuzz_target
	@python3 magni.py build --asan

fuzz-cov: ## build coverage-instrumented target → target/fuzz/fuzz_target_cov
	@python3 magni.py build --cov

fuzz-test: ## run every corpus seed under ASan (regression gate; target auto-rebuilds if stale)
	@python3 magni.py corpus test

fuzz-exec-build: ## build executor/storage target (coverage + ASan) → target/fuzz-exec/fuzz_exec_target
	@python3 magni.py build --exec

fuzz-exec-test: ## run every exec seed under ASan (regression gate; target auto-rebuilds if stale)
	@python3 magni.py corpus test --exec

fuzz-run: fuzz-cov ## launch interactive AFL++ campaign (Ctrl-C to stop, pass ARGS=" -V 60")
	@python3 magni.py fuzz run -- $(ARGS)

fuzz-campaign: fuzz-cov ## headless parallel campaign (FUZZ_WORKERS=4 FUZZ_SECONDS=3600 ROLES="...")
	@python3 magni.py fuzz campaign -w $(FUZZ_WORKERS) -s $(FUZZ_SECONDS) $(if $(ROLES),--roles "$(ROLES)") -- $(ARGS)
	@echo "campaign: $(FUZZ_WORKERS) workers, $(FUZZ_SECONDS)s, out=$(or $(MAGNI_FUZZ_OUT),$(FUZZ_OUT))"

fuzz-status: ## show per-worker fuzzer_stats
	@python3 magni.py fuzz status

fuzz-stop: ## pkill afl-fuzz
	@python3 magni.py fuzz stop

fuzz-clean: ## remove fuzz artifacts (afl-output, build, pycache)
	@python3 magni.py clean --fuzz-only

fuzz-promote: fuzz-cov ## merge grown queue → update gen_corpus.py (MIN=1 to minimize, TEST=1 for full gate, EXEC=1 for exec pool)
	@python3 magni.py corpus promote $(if $(filter 1,$(MIN)),--minimize) $(if $(filter 1,$(TEST)),--test) $(if $(filter 1,$(EXEC)),--exec)

fuzz-one: fuzz-build ## repro one crash: make fuzz-one FILE=fuzz/afl-output/.../id:000000
	@test -n "$(FILE)" || { echo "usage: make fuzz-one FILE=<path>" >&2; exit 2; }
	@ASAN_OPTIONS=detect_leaks=0:abort_on_error=1 ./target/fuzz/fuzz_target "$(FILE)"

fuzz-one-exec: fuzz-exec-build ## repro one exec crash: make fuzz-one-exec FILE=fuzz/afl-exec-output/.../id:000000
	@test -n "$(FILE)" || { echo "usage: make fuzz-one-exec FILE=<path>" >&2; exit 2; }
	@ASAN_OPTIONS=abort_on_error=1:symbolize=0 ./target/fuzz-exec/fuzz_exec_target "$(FILE)"

fuzz-grammar-test: ## selftest the SQL-aware Python mutator (no AFL++ needed)
	@python3 fuzz/grammar_mutator.py --selftest

help: ## show this help
	@python3 magni.py help
