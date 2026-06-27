PROJECT       := magni
BINARY        := $(PROJECT)
SRC_DIR       := src
BUILD_DIR     := build
ODIN          := odin
COLLECTIONS   := -collection:src=$(SRC_DIR)
TEST_DIR      := tests

DEBUG_FLAGS   := -debug -o:none -warnings-as-errors \
                 -use-separate-modules

RELEASE_FLAGS := -o:aggressive \
                 -no-bounds-check \
                 -no-type-assert \
                 -disable-assert \
                 -microarch:native \
                 -source-code-locations:none

TEST_FLAGS    := -debug -o:none -warnings-as-errors \
                 -use-separate-modules \
                 -define:ODIN_TEST_THREADS=1

VET_FLAGS     := -vet -vet-shadowing -warnings-as-errors -strict-style

CHECK_FLAGS   := -warnings-as-errors

.PHONY: all build run release test test-verbose test-single clean rebuild \
        check vet vet-shadowing vet-style vet-all vet-cast vet-unused \
        check-vet help

all: build

build:
	@echo "Building debug version..."
	@mkdir -p $(BUILD_DIR)
	$(ODIN) build $(SRC_DIR) -out:$(BUILD_DIR)/$(BINARY) $(COLLECTIONS) $(DEBUG_FLAGS)

run: build
	@echo "Running $(BUILD_DIR)/$(BINARY)..."
	@./$(BUILD_DIR)/$(BINARY)

release:
	@echo "Building release version..."
	@mkdir -p $(BUILD_DIR)
	$(ODIN) build $(SRC_DIR) -out:$(BUILD_DIR)/$(BINARY)_release $(COLLECTIONS) $(RELEASE_FLAGS)

test:
	@echo "Running tests..."
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS)

test-verbose:
	@echo "Running tests (verbose)..."
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS) \
	        -define:ODIN_TEST_FANCY=false

test-single:
	@echo "Running single test: $(filter-out $@,$(MAKECMDGOALS))"
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS) \
	        -define:ODIN_TEST_NAMES="tests.$(filter-out $@,$(MAKECMDGOALS))"

%:
	@true

test-cli: build
	@echo "Running CLI integration tests..."
	@TMP=$$(mktemp /tmp/magni_cli_test.XXXXXX.db); \
	 TMP2=$$(mktemp /tmp/magni_cli_test.XXXXXX); \
	 trap 'rm -f "$$TMP" "$$TMP2"' EXIT; \
	 echo "  [test] --help succeeds"; \
	 ./build/magni --help > /dev/null 2>&1 && echo "    PASS" || echo "    FAIL"; \
	 echo "  [test] -h succeeds"; \
	 ./build/magni -h > /dev/null 2>&1 && echo "    PASS" || echo "    FAIL"; \
	 echo "  [test] default database path creates test.db"; \
	 rm -f test.db && ./build/magni --eval "CREATE TABLE t (x INT);" 2>&1 | head -1 > /dev/null && \
	   [ -f test.db ] && rm -f test.db && echo "    PASS" || echo "    FAIL"; \
	 echo "  [test] positional database path works"; \
	 ./build/magni --eval "CREATE TABLE t (x INT);" "$$TMP" 2>&1 | head -1 > /dev/null && \
	   [ -f "$$TMP" ] && echo "    PASS" || echo "    FAIL"; \
	 echo "  [test] --eval executes SQL"; \
	 printf "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (42);\nSELECT * FROM t;\n" > "$$TMP2"; \
	 ./build/magni --file "$$TMP2" "$$TMP" 2>&1 | grep -q "42" && echo "    PASS" || echo "    FAIL"; \
	 echo "  [test] pipe mode reads SQL from stdin"; \
	 printf "CREATE TABLE t (x INT);\nINSERT INTO t VALUES (99);\nSELECT * FROM t;\n" | \
	   ./build/magni "$$TMP"2 2>&1 | grep -q "99" && echo "    PASS" || echo "    FAIL"
	@echo "CLI tests complete."

vet:
	@echo "Running comprehensive vet (fast check on src)..."
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS) $(VET_FLAGS)

vet-all:
	@echo "Running comprehensive vet (build+test)..."
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) $(VET_FLAGS) -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(VET_FLAGS) -define:ODIN_TEST_THREADS=1

vet-shadowing:
	@echo "Checking for variable shadowing..."
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) -vet-shadowing -warnings-as-errors -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) -vet-shadowing -warnings-as-errors -define:ODIN_TEST_THREADS=1

vet-unused:
	@echo "Checking for unused declarations..."
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) -vet-unused -warnings-as-errors -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) -vet-unused -warnings-as-errors -define:ODIN_TEST_THREADS=1

vet-style:
	@echo "Checking code style..."
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) -vet-style -vet-semicolon -warnings-as-errors -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) -vet-style -vet-semicolon -warnings-as-errors -define:ODIN_TEST_THREADS=1

vet-cast:
	@echo "Checking for redundant casts..."
	$(ODIN) build $(SRC_DIR) $(COLLECTIONS) -vet-cast -warnings-as-errors -out:/dev/null
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) -vet-cast -warnings-as-errors -define:ODIN_TEST_THREADS=1

check:
	@echo "Checking syntax and types..."
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS) $(CHECK_FLAGS)

check-vet:
	@echo "Checking with comprehensive vet..."
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS) $(CHECK_FLAGS) $(VET_FLAGS)

clean:
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)

rebuild: clean build

help:
	@echo "Targets:"
	@echo ""
	@echo "  BUILD"
	@echo "    build         - build debug binary"
	@echo "    release       - build release binary (aggressive opt)"
	@echo "    run           - build debug and run"
	@echo "    clean         - remove build directory"
	@echo "    rebuild       - clean and build"
	@echo ""
	@echo "  TEST"
	@echo "    test          - run all tests (debug)"
	@echo "    test-verbose  - run all tests with verbose output"
	@echo ""
	@echo "  VET (comprehensive checks)"
	@echo "    vet           - vet + shadowing + strict-style (fast check)"
	@echo "    vet-all       - same as vet, but via build+test (LLVM)"
	@echo "    vet-shadowing - check variable shadowing only"
	@echo "    vet-unused    - check unused variables/imports only"
	@echo "    vet-style     - check style (trailing commas, semicolons)"
	@echo "    vet-cast      - check redundant casts/transmutes only"
	@echo ""
	@echo "  CHECK (fast, no LLVM)"
	@echo "    check         - parse + type check (no vet)"
	@echo "    check-vet     - parse + type check with comprehensive vet"
	@echo ""
	@echo "  Flags:  $(COLLECTIONS)"
	@echo "  Odin:   $(shell $(ODIN) version 2>&1 || echo 'not found')"
