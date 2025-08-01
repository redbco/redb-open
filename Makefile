# Makefile for reDB Node Open Source

# Project variables
BINARY_DIR := bin
BUILD_DIR := build
SERVICES := supervisor security unifiedmodel transformation mesh anchor core webhook clientapi serviceapi queryapi mcpserver cli

# Default to darwin arm64 build
GOOS ?= darwin
GOARCH ?= arm64
GO_BUILD_FLAGS := -v

# Detect operating system
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    HOST_OS := darwin
else
    HOST_OS := linux
endif

# Version information
VERSION ?= $(shell git describe --tags --always --dirty)
GIT_COMMIT ?= $(shell git rev-parse --short HEAD)
BUILD_TIME ?= $(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Build flags for version information
VERSION_FLAGS := -X main.Version=$(VERSION) \
                -X main.GitCommit=$(GIT_COMMIT) \
                -X main.BuildTime=$(BUILD_TIME)

# Protocol Buffer files
PROTO_FILES := api/proto/common/v1/common.proto \
			   api/proto/supervisor/v1/supervisor.proto \
			   api/proto/security/v1/security.proto \
			   api/proto/unifiedmodel/v1/unifiedmodel.proto \
			   api/proto/transformation/v1/transformation.proto \
			   api/proto/mesh/v1/mesh.proto \
               api/proto/anchor/v1/anchor.proto \
			   api/proto/core/v1/core.proto \
			   api/proto/webhook/v1/webhook.proto

.PHONY: all clean build test proto dev local

all: clean proto build test

# Create necessary directories
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Clean build artifacts
clean:
	rm -rf $(BINARY_DIR)
	rm -rf $(BUILD_DIR)

# Build all services (cross-compile for Linux by default)
build: $(BINARY_DIR) $(addprefix build-,$(SERVICES))

# Local development build (builds for host OS)
local: GOOS=$(HOST_OS)
local: build

# Generic build rule for services
build-%: 
	@echo "Building $* for $(GOOS)/$(GOARCH)..."
	@if [ "$*" = "supervisor" ]; then \
		CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build $(GO_BUILD_FLAGS) -ldflags "$(VERSION_FLAGS)" \
		-o $(BINARY_DIR)/redb-node ./cmd/$*/cmd; \
	elif [ "$*" = "cli" ]; then \
		CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build $(GO_BUILD_FLAGS) -ldflags "$(VERSION_FLAGS)" \
		-o $(BINARY_DIR)/redb-$* ./cmd/$*/cmd; \
	else \
		CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build $(GO_BUILD_FLAGS) -ldflags "$(VERSION_FLAGS)" \
		-o $(BINARY_DIR)/redb-$* ./services/$*/cmd; \
	fi

# Development build (builds for local OS)
dev: GOOS=$(HOST_OS)
dev: clean proto build test

# Run all tests
test:
	@echo "Running tests..."
	@for service in $(SERVICES); do \
		if [ "$$service" = "supervisor" ] || [ "$$service" = "cli" ]; then \
			go test -v ./cmd/$$service/...; \
		else \
			go test -v ./services/$$service/...; \
		fi \
	done

# Generate Protocol Buffer code
proto:
	@echo "Generating Protocol Buffer code..."
	protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. \
		$(PROTO_FILES)

# Development tools
.PHONY: dev-tools
dev-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Lint the code
.PHONY: lint
lint:
	golangci-lint run ./...

# Lint the code using golangci-lint v2
.PHONY: lint-v2
lint-v2:
	@for d in ./cmd/* ./services/* ./pkg/* ; do \
		echo "Linting $$d..."; \
		golangci-lint run --config=.golangci-v2.yml $$d/... || exit 1; \
	done

# Build for multiple platforms
.PHONY: build-all
build-all: $(BUILD_DIR)
	@echo "Building for multiple platforms..."
	@for os in linux darwin windows; do \
		for arch in amd64 arm64; do \
			mkdir -p $(BUILD_DIR)/$$os-$$arch; \
			for service in $(SERVICES); do \
				if [ "$$service" = "supervisor" ]; then \
					echo "Building supervisor for $$os/$$arch..."; \
					GOOS=$$os GOARCH=$$arch \
					CGO_ENABLED=0 go build $(GO_BUILD_FLAGS) \
					-ldflags "$(VERSION_FLAGS)" \
					-o $(BUILD_DIR)/$$os-$$arch/redb-node \
					./cmd/supervisor/cmd; \
				elif [ "$$service" = "cli" ]; then \
					echo "Building cli for $$os/$$arch..."; \
					GOOS=$$os GOARCH=$$arch \
					CGO_ENABLED=0 go build $(GO_BUILD_FLAGS) \
					-ldflags "$(VERSION_FLAGS)" \
					-o $(BUILD_DIR)/$$os-$$arch/redb-cli \
					./cmd/cli/cmd; \
				else \
					echo "Building $$service for $$os/$$arch..."; \
					GOOS=$$os GOARCH=$$arch \
					CGO_ENABLED=0 go build $(GO_BUILD_FLAGS) \
					-ldflags "$(VERSION_FLAGS)" \
					-o $(BUILD_DIR)/$$os-$$arch/redb-$$service \
					./services/$$service/cmd; \
				fi \
			done \
		done \
	done

# Install the binaries (for Linux only)
.PHONY: install
install: build
	@if [ "$(GOOS)" != "linux" ]; then \
		echo "Error: install target is for Linux only"; \
		exit 1; \
	fi
	@echo "Installing binaries..."
	@for service in $(SERVICES); do \
		if [ "$$service" = "supervisor" ]; then \
			install -m 755 $(BINARY_DIR)/redb-node /usr/local/bin/; \
		else \
			install -m 755 $(BINARY_DIR)/redb-$$service /usr/local/bin/; \
		fi \
	done

# Show version information
.PHONY: version
version:
	@echo "Version: $(VERSION)"
	@echo "Git commit: $(GIT_COMMIT)"
	@echo "Build time: $(BUILD_TIME)"

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all        - Clean, generate proto files, build, and test"
	@echo "  clean      - Remove build artifacts"
	@echo "  build      - Build all services (cross-compile for Linux)"
	@echo "  local      - Build for local development (host OS)"
	@echo "  dev        - Development build (clean, proto, build, test)"
	@echo "  test       - Run all tests"
	@echo "  proto      - Generate Protocol Buffer code"
	@echo "  dev-tools  - Install development tools"
	@echo "  lint       - Run linter"
	@echo "  build-all  - Build for multiple platforms"
	@echo "  install    - Install binaries (Linux only)"
	@echo "  version    - Show version information"