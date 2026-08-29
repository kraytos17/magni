SRC_DIR     := src
TEST_DIR    := tests
BUILD_DIR   := build
ODIN        := odin
COLLECTIONS := -collection:src=$(SRC_DIR)

DEBUG_FLAGS   := -debug -o:none -warnings-as-errors -use-separate-modules
RELEASE_FLAGS := -o:aggressive -lto:thin -no-bounds-check -no-type-assert \
                 -disable-assert -microarch:native -source-code-locations:none
TEST_FLAGS    := -debug -o:none -warnings-as-errors -use-separate-modules \
                 -define:ODIN_TEST_THREADS=1

.PHONY: all build release run clean rebuild
.PHONY: test test-verbose test-single test-cli test-cli-full
.PHONY: check vet vet-all perf fuzz help

all: build

build:
	@mkdir -p $(BUILD_DIR)
	$(ODIN) build $(SRC_DIR) -out:$(BUILD_DIR)/magni $(COLLECTIONS) $(DEBUG_FLAGS)

run: build
	@./$(BUILD_DIR)/magni

release:
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

vet:
	@bash scripts/vet.sh $(filter-out $@,$(MAKECMDGOALS))

vet-all:
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) -vet -vet-shadowing -warnings-as-errors -strict-style -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) -vet -vet-shadowing -warnings-as-errors -strict-style -define:ODIN_TEST_THREADS=1

perf:
	$(ODIN) run tests/perf $(COLLECTIONS) $(RELEASE_FLAGS)

fuzz:
	@bash fuzz/scripts/fuzz.sh $(filter-out $@,$(MAKECMDGOALS))

help:
	@bash scripts/help.sh
