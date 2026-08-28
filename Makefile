SRC_DIR     := src
TEST_DIR    := tests
BUILD_DIR   := build
ODIN        := odin
COLLECTIONS := -collection:src=$(SRC_DIR)

DEBUG_FLAGS   := -debug -o:none -warnings-as-errors -use-separate-modules
RELEASE_FLAGS := -o:aggressive -no-bounds-check -no-type-assert -disable-assert \
                 -microarch:native -source-code-locations:none
TEST_FLAGS    := -debug -o:none -warnings-as-errors -use-separate-modules \
                 -define:ODIN_TEST_THREADS=1
VET_FLAGS     := -vet -vet-shadowing -warnings-as-errors -strict-style

.PHONY: build release run clean rebuild
.PHONY: test test-verbose test-single test-cli test-cli-full
.PHONY: check check-vet vet vet-all vet-shadowing vet-unused vet-style vet-cast
.PHONY: perf fuzz-corpus fuzz-build fuzz-test fuzz-run help

build:
	@echo "Building debug version..."
	@mkdir -p $(BUILD_DIR)
	$(ODIN) build $(SRC_DIR) -out:$(BUILD_DIR)/magni $(COLLECTIONS) $(DEBUG_FLAGS)

run: build
	@./$(BUILD_DIR)/magni

release:
	@echo "Building release version..."
	@mkdir -p $(BUILD_DIR)
	$(ODIN) build $(SRC_DIR) -out:$(BUILD_DIR)/magni_release $(COLLECTIONS) $(RELEASE_FLAGS)

rebuild: clean build

clean:
	@rm -rf $(BUILD_DIR)

test:
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS)

test-verbose:
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS) -define:ODIN_TEST_FANCY=false

test-single:
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS) \
	        -define:ODIN_TEST_NAMES="tests.$(filter-out $@,$(MAKECMDGOALS))"

%:
	@true

test-cli: build
	@bash tests/cli_smoke.sh

test-cli-full: build
	@bash tests/cli_test.sh

check:
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS) -warnings-as-errors

check-vet:
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS) $(VET_FLAGS)

vet:
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS) $(VET_FLAGS)

vet-all:
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) $(VET_FLAGS) -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(VET_FLAGS) -define:ODIN_TEST_THREADS=1

vet-shadowing:
	@bash scripts/vet.sh shadowing

vet-unused:
	@bash scripts/vet.sh unused

vet-style:
	@bash scripts/vet.sh style

vet-cast:
	@bash scripts/vet.sh cast

perf:
	$(ODIN) run tests/perf $(COLLECTIONS) -o:speed

fuzz-corpus:
	@python3 fuzz/corpus/gen_corpus.py

fuzz-build:
	@bash fuzz/scripts/build.sh

fuzz-test: fuzz-build
	@bash fuzz/scripts/test-corpus.sh

fuzz-run:
	@bash fuzz/scripts/run-afl.sh

help:
	@bash scripts/help.sh
