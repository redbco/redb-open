## Install and Run

### Prerequisites

- Go 1.23+
- Rust toolchain (stable) with Cargo
  - Recommended: `curl https://sh.rustup.rs -sSf | sh` then `rustup default stable`
- Protocol Buffers compiler (`protoc`)
- PostgreSQL 17+
- Redis Server

macOS (Homebrew)
```bash
brew install go rust protobuf postgresql@17 redis
```

Ubuntu/Debian
```bash
sudo apt update
sudo apt install -y golang rustc cargo protobuf-compiler redis-server
# PostgreSQL 17 (PGDG)
sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt update && sudo apt -y install postgresql-17
```

### Build

```bash
git clone https://github.com/redbco/redb-open.git
cd redb-open

# Optional Go tooling for protobuf and linting
make dev-tools

# Build for your host OS/arch (includes Rust mesh and Go services)
make local

# Run tests (Go + Rust)
make test
```

Artifacts are placed under `bin/` for local builds and `build/<os-arch>/` for multi-platform builds.

### Initialize & Run

```bash
# Initialize the system (creates schema and default tenant/user if desired)
./bin/redb-node --initialize

# Start the supervisor (or run as a service)
./bin/redb-node

# Login via CLI
./bin/redb-cli auth login
```

When prompted during initialization, provide your PostgreSQL connection details. You can also preconfigure via `bin/config.yaml` or `sample_config/config.yaml`.

### Cross-Compiling and Targets

The `build-all` target builds Go services and the Rust mesh for linux/darwin/windows on amd64/arm64. For Rust, relevant targets are added via Cargo when invoked by Make.

```bash
make build-all
```

If you manually cross-compile the mesh, install targets (examples):
```bash
rustup target add aarch64-apple-darwin x86_64-unknown-linux-gnu
```


