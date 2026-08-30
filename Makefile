SRC_DIR     := src
TEST_DIR    := tests
BUILD_DIR   := build
ODIN        ?= odin
COLLECTIONS := -collection:src=$(SRC_DIR)

# ── compiler flags ──────────────────────────────────────────────────
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

# ── phony ───────────────────────────────────────────────────────────
.PHONY: all build release run rebuild clean clean-all
.PHONY: test test-verbose test-single test-cli test-cli-full smoke quick ci
.PHONY: check vet vet-all
.PHONY: perf bench
.PHONY: fuzz fuzz-corpus corpus fuzz-build fuzz-cov fuzz-cov-build fuzz-test
.PHONY: fuzz-run fuzz-campaign fuzz-status fuzz-stop fuzz-clean
.PHONY: fuzz-showmap fuzz-cmin fuzz-one fuzz-help
.PHONY: help

# ── BUILD ───────────────────────────────────────────────────────────
all: build ## default: build debug binary

build: ## build debug binary → build/magni
	@mkdir -p $(BUILD_DIR)
	$(ODIN) build $(SRC_DIR) -out:$(BUILD_DIR)/magni $(COLLECTIONS) $(DEBUG_FLAGS)

release: ## build release binary (LTO, no checks) → build/magni_release
	@mkdir -p $(BUILD_DIR)
	$(ODIN) build $(SRC_DIR) -out:$(BUILD_DIR)/magni_release $(COLLECTIONS) $(RELEASE_FLAGS)

run: build ## build debug and run
	@./$(BUILD_DIR)/magni

rebuild: clean build ## clean and rebuild debug

clean: ## remove build/ only
	@rm -rf $(BUILD_DIR)
	@echo "cleaned $(BUILD_DIR)/"

clean-all: clean fuzz-clean ## remove build/ + fuzz artifacts (afl-output, build, corpus pycache)

# ── TEST ────────────────────────────────────────────────────────────
test: ## run all Odin tests (debug)
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS)

test-verbose: ## run tests with verbose output (no fancy)
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS) -define:ODIN_TEST_FANCY=false

test-single: ## run one test: make test-single test_integration_vacuum
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS) \
	        -define:ODIN_TEST_NAMES="tests.$(filter-out $@,$(MAKECMDGOALS))"

# swallow test-single args so make doesn't error on unknown target
%:
	@true

test-cli: build ## basic CLI smoke checks (needs build/magni)
	@bash tests/cli_smoke.sh

test-cli-full: build ## comprehensive CLI integration tests
	@bash tests/cli_test.sh

smoke: test test-cli ## quick smoke: unit tests + CLI
	@echo "smoke: all checks passed"

quick: check test ## fast gate: check + test (no vet/fuzz)

ci: vet-all test fuzz-test ## CI gate: vet-all + test + fuzz corpus ASan
	@echo "ci: all gates passed"

# ── CHECK / VET ─────────────────────────────────────────────────────
check: ## parse + type check (no vet)
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS) -warnings-as-errors

vet: ## vet (fast, no LLVM): vet | vet shadowing|unused|style|cast
	@bash scripts/vet.sh $(filter-out $@,$(MAKECMDGOALS))

vet-all: ## vet via build+test (LLVM, strict-style, shadowing)
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) -vet -vet-shadowing -warnings-as-errors -strict-style -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) -vet -vet-shadowing -warnings-as-errors -strict-style -define:ODIN_TEST_THREADS=1

# ── PERF ────────────────────────────────────────────────────────────
perf: ## run timing baseline (release flags)
	$(ODIN) run tests/perf $(COLLECTIONS) $(RELEASE_FLAGS)

bench: perf ## alias for perf

# ── FUZZ ────────────────────────────────────────────────────────────
# Generic dispatcher keeps old UX: make fuzz <cmd>  e.g. make fuzz campaign
fuzz: ## fuzz dispatcher: corpus|build|cov|test|run|campaign|status|stop
	@bash fuzz/scripts/fuzz.sh $(filter-out $@,$(MAKECMDGOALS))

# Streamlined explicit targets (preferred for tab-completion + FUZZ_* vars)
fuzz-corpus corpus: ## regenerate seed corpus (python3 fuzz/corpus/gen_corpus.py)
	@python3 fuzz/corpus/gen_corpus.py
	@rm -rf fuzz/corpus/__pycache__
	@echo "corpus: $$(ls fuzz/corpus | grep -v gen_corpus | wc -l) seeds in fuzz/corpus/ ($$(du -sh fuzz/corpus | cut -f1))"

fuzz-build: ## build ASan fuzz target → fuzz/fuzz_target
	@bash fuzz/scripts/build.sh

fuzz-cov fuzz-cov-build: ## build coverage-instrumented target → fuzz/fuzz_target_cov
	@bash fuzz/scripts/build-cov.sh

fuzz-test: fuzz-build ## run every corpus seed under ASan (regression gate)
	@bash fuzz/scripts/test-corpus.sh

fuzz-run: fuzz-cov ## launch interactive AFL++ campaign (Ctrl-C to stop, pass ARGS=" -V 60")
	@bash fuzz/scripts/run-afl.sh $(ARGS)

fuzz-campaign: fuzz-cov ## headless parallel campaign (FUZZ_WORKERS=4 FUZZ_SECONDS=3600)
	@AFL_NO_UI=1 bash fuzz/scripts/run-afl-parallel.sh $(FUZZ_WORKERS) -V $(FUZZ_SECONDS) $(ARGS)
	@echo "campaign: $(FUZZ_WORKERS) workers, $(FUZZ_SECONDS)s, out=$(or $(MAGNI_FUZZ_OUT),$(FUZZ_OUT))"

fuzz-status: ## show per-worker fuzzer_stats
	@bash fuzz/scripts/status.sh

fuzz-stop: ## pkill afl-fuzz
	@pkill afl-fuzz || true
	@echo "stopped afl-fuzz (if running)"

fuzz-clean: ## remove fuzz artifacts (afl-output, build, pycache)
	@rm -rf $(or $(MAGNI_FUZZ_OUT),$(FUZZ_OUT)) fuzz/build fuzz/corpus/__pycache__
	@echo "cleaned fuzz artifacts"

fuzz-showmap: fuzz-cov ## show coverage tuples for current corpus
	@rm -rf /tmp/magni.map /tmp/magni_corpus_tmp && mkdir -p /tmp/magni_corpus_tmp && cp fuzz/corpus/* /tmp/magni_corpus_tmp/ 2>/dev/null; rm -f /tmp/magni_corpus_tmp/gen_corpus.py; rm -rf /tmp/magni_corpus_tmp/__pycache__; \
	  AFL_MAP_SIZE=7181 afl-showmap -C -i /tmp/magni_corpus_tmp -o /tmp/magni.map -- ./fuzz/fuzz_target_cov @@ 2>&1 | grep -E "Captured|coverage"; \
	  echo "map: $$(wc -l < /tmp/magni.map 2>/dev/null || echo 0) tuples listed in /tmp/magni.map"

fuzz-cmin: fuzz-cov ## minimize corpus (-i fuzz/corpus -o /tmp/min)
	@rm -rf /tmp/min /tmp/magni_corpus_tmp && mkdir -p /tmp/magni_corpus_tmp && cp fuzz/corpus/* /tmp/magni_corpus_tmp/ 2>/dev/null; rm -f /tmp/magni_corpus_tmp/gen_corpus.py; rm -rf /tmp/magni_corpus_tmp/__pycache__; \
	  afl-cmin -i /tmp/magni_corpus_tmp -o /tmp/min -m none -t 1000 -- ./fuzz/fuzz_target_cov @@ 2>&1 | tail -5; \
	  echo "minimized: $$(ls /tmp/min 2>/dev/null | wc -l) files in /tmp/min"

fuzz-one: fuzz-build ## repro one crash: make fuzz-one FILE=fuzz/afl-output/.../id:000000
	@test -n "$(FILE)" || { echo "usage: make fuzz-one FILE=<path>" >&2; exit 2; }
	@bash fuzz/scripts/run-one.sh "$(FILE)"

fuzz-help: ## fuzz help
	@bash fuzz/scripts/fuzz.sh help || true
	@echo ""
	@echo "Streamlined fuzz targets:"
	@echo "  make fuzz-corpus          regenerate corpus"
	@echo "  make fuzz-build           ASan target"
	@echo "  make fuzz-cov             coverage target"
	@echo "  make fuzz-test            ASan gate"
	@echo "  make fuzz-run ARGS=\"-V 60\"   interactive"
	@echo "  make fuzz-campaign FUZZ_SECONDS=60 FUZZ_WORKERS=2"
	@echo "  make fuzz-status | fuzz-stop | fuzz-clean | fuzz-showmap | fuzz-cmin"

# ── HELP ────────────────────────────────────────────────────────────
help: ## show this help
	@bash scripts/help.sh 2>/dev/null || { \
	  echo "MagniDB — available targets:"; echo ""; \
	  awk 'BEGIN{FS=":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST); \
	}
