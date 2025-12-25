PROJECT       := magni
BINARY        := $(PROJECT)
SRC_DIR       := src
BUILD_DIR     := build
ODIN          := odin
COLLECTIONS   := -collection:src=$(SRC_DIR)
DEBUG_FLAGS   := -debug -o:none
RELEASE_FLAGS := -o:speed -no-bounds-check
TEST_FLAGS    := -debug -o:none -warnings-as-errors
SOURCES       := $(wildcard $(SRC_DIR)/*.odin)
TEST_DIR      := tests

.PHONY: all build run release vet check clean rebuild help
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

vet:
	@echo "Running odin vet..."
	$(ODIN) vet $(SRC_DIR) $(COLLECTIONS)

check:
	@echo "Checking syntax..."
	$(ODIN) check $(SRC_DIR) $(COLLECTIONS)

clean:
	@echo "Cleaning build directory..."
	@rm -rf $(BUILD_DIR)

test:
	@echo "Running tests..."
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS)

test-verbose:
	@echo "Running tests (verbose)..."
	$(ODIN) test $(TEST_DIR) $(COLLECTIONS) $(TEST_FLAGS) -define:ODIN_TEST_FANCY=false

rebuild: clean build

help:
	@echo "Targets:"
	@echo "  build         - build debug version"
	@echo "  run           - build and run"
	@echo "  release       - build optimized version"
	@echo "  vet           - run odin vet"
	@echo "  check         - run odin check"
	@echo "  clean         - remove build directory"
	@echo "  rebuild       - clean and rebuild"
