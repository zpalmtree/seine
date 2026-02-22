.PHONY: all clean build build-native build-cpu test run release tag-release

VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m)
ifeq ($(ARCH),x86_64)
	ARCH := amd64
endif
ifeq ($(ARCH),aarch64)
	ARCH := arm64
endif

all: build

# Build (default features, includes nvidia)
build:
	@echo "Building seine..."
	cargo build --release
	@cp target/release/seine .

# Build with native CPU tuning (no GPU)
build-native:
	@echo "Building seine (native CPU, no GPU)..."
	RUSTFLAGS="-C target-cpu=native" cargo build --profile release-native --no-default-features
	@cp target/release-native/seine .

# Build CPU-only (no GPU, default tuning)
build-cpu:
	@echo "Building seine (CPU only)..."
	cargo build --release --no-default-features
	@cp target/release/seine .

# Run tests
test:
	@echo "Running tests..."
	cargo test --workspace

# Run after building
run: build
	./seine

# Build release package for current platform
release: build
	@echo "Packaging release for $(OS)-$(ARCH)..."
	@mkdir -p releases
	@cp target/release/seine releases/seine
	@cd releases && zip -q seine-$(VERSION)-$(OS)-$(ARCH).zip seine
ifeq ($(OS),darwin)
	@cd releases && shasum -a 256 seine-$(VERSION)-$(OS)-$(ARCH).zip >> SHA256SUMS.txt
else
	@cd releases && sha256sum seine-$(VERSION)-$(OS)-$(ARCH).zip >> SHA256SUMS.txt
endif
	@rm -f releases/seine
	@echo "Built: releases/seine-$(VERSION)-$(OS)-$(ARCH).zip"
	@echo "Checksum added to releases/SHA256SUMS.txt"

# Update Cargo version + create matching git tag (annotated)
# Usage: make tag-release TAG=v0.1.10
tag-release:
	@if [ -z "$(TAG)" ]; then \
		echo "Usage: make tag-release TAG=vX.Y.Z"; \
		exit 1; \
	fi
	@./scripts/release_tag.sh $(TAG)

# Clean build artifacts
clean:
	@echo "Cleaning..."
	cargo clean
	rm -f seine
	rm -rf releases/

# Clean miner data
clean-data:
	@echo "Removing miner data..."
	rm -rf data/ seine-data/

# Clean everything
clean-all: clean clean-data

# Fetch dependencies
deps:
	@echo "Fetching Rust dependencies..."
	cargo fetch
