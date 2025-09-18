# Mesh Network

A high-performance, secure mesh networking system built in Rust designed to enable reliable communication across distributed mesh topologies of up to one million nodes.

## Overview

Mesh is a distributed networking system that provides secure, efficient communication between nodes in a mesh topology. It serves as a communication fabric for complex distributed applications, featuring domain-aware routing, post-quantum cryptography, deliver-once semantics, and comprehensive observability. The system exposes a clean gRPC API for integration with applications written in Go or other languages.

## Architecture

The project is organized as a Rust workspace with the following crates:

### Core Protocol & Transport

- **`mesh-wire`** - Wire protocol framing, encoding/decoding, fast header processing, and message chunking
- **`mesh-session`** - TCP/TLS connection management, handshake protocols, keepalive, and session lifecycle
- **`mesh-storage`** - Write-ahead logging (WAL) and deduplication with pluggable backends (memory, file, Redis)

### Routing & Topology

- **`mesh-routing`** - Domain-aware routing tables, ECMP, and path computation algorithms
- **`mesh-topology`** - Domain graph management and link-state protocols for topology discovery

### Services & Integration

- **`mesh-grpc`** - gRPC services (MeshData, MeshControl) with message tracking, delivery queues, and metrics
- **`mesh-bin`** - Main binary with node bootstrap, configuration management, and system integration

## Features

- 🌐 **Domain-Aware Routing** - Intelligent routing with ECMP and path computation algorithms
- ⚡ **High Performance** - Efficient wire protocol with fast 48-byte headers and message chunking
- 💾 **Reliable Delivery** - WAL with deduplication ensuring exactly-once message delivery
- 🔌 **gRPC Integration** - Clean APIs for data plane operations and control plane management
- 🛡️ **Session Management** - TCP/TLS connections with handshake protocols and keepalive mechanisms
- 🏗️ **Hierarchical Topology** - Domain-based topology management with link-state protocols
- 📊 **Message Tracking** - Comprehensive message lifecycle tracking and delivery confirmation
- 🔗 **Pluggable Storage** - Multiple backend options (memory, file-based, Redis) for persistence

## Getting Started

### Prerequisites

- Rust 1.77+ (recommended)
- Protocol Buffers compiler (`protoc`)
- Docker (optional, for local development setup)

### Building

```bash
# Clone the repository
git clone https://github.com/redbco/redb-open.git
cd redb-open

# Build all crates
cargo build

# Build with optimizations
cargo build --release

# Run the mesh binary
cargo run --bin mesh
```

### Running Tests

```bash
# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p mesh-wire

# Run with output
cargo test -- --nocapture
```

### Development

```bash
# Check code formatting
cargo fmt --check

# Run clippy lints
cargo clippy --all-targets --all-features

# Generate documentation
cargo doc --open
```

### Using the Makefile

The project includes a comprehensive Makefile for development automation. Use `make help` to see all available targets:

```bash
# Show all available commands
make help

# Common development tasks
make build          # Build with TLS support (default)
make test           # Run unit tests
make lint           # Run all linting tools (fmt + clippy)
make release        # Build optimized release
make release-all    # Build optimized release with full features (Redis backend + TLS)

# Development workflow
make dev-check      # Quick check (format, lint, test)
make dev-full       # Full development build and test

# TLS testing
make certs-localhost    # Generate localhost certificates
make run-tls-listener   # Run TLS listener node
make run-tls-connector  # Run TLS connector node

# CI/Release
make ci-full        # Run complete CI pipeline
make pre-release    # Prepare for release (full CI + docs)
```

## Configuration

The mesh node is configured through command-line arguments. The main binary supports various options for network configuration, TLS settings, and operational parameters.

Example usage:

```bash
# Basic mesh node
cargo run --bin mesh -- --node-id 1001 --listen 0.0.0.0:8080

# With gRPC enabled
cargo run --bin mesh -- --node-id 1001 --listen 0.0.0.0:8080 --enable-grpc --grpc-listen 0.0.0.0:50051

# With TLS and connections to other nodes
cargo run --bin mesh -- --node-id 1001 --listen 0.0.0.0:8080 --tls --connect 192.168.1.100:8080
```

## gRPC API

The mesh node exposes gRPC APIs for data plane operations and control plane management.

### MeshData Service

```proto
service MeshData {
    rpc Send(SendRequest) returns (SendResponse);
    rpc Subscribe(stream SubscribeRequest) returns (stream Received);
    rpc Ack(Ack) returns (.google.protobuf.Empty);
}
```

- **Send**: Applications send messages to destination nodes with routing, WAL persistence, and delivery tracking.
- **Subscribe**: Applications subscribe to receive messages addressed to this node.
- **Ack**: Applications can acknowledge received messages for delivery confirmation.

### MeshControl Service

```proto
service MeshControl {
    rpc PublishState(StateSync) returns (.google.protobuf.Empty);
    rpc GetTopology(GetTopologyRequest) returns (Topology);
    rpc SetPolicy(SetPolicyRequest) returns (.google.protobuf.Empty);
}
```

- **PublishState**: Nodes publish their current state and topology information.
- **GetTopology**: Returns the current network topology view.
- **SetPolicy**: Allows configuration of routing and operational policies.

### Example Go Client

```go
conn, _ := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := meshpb.NewMeshDataClient(conn)

resp, _ := client.Send(ctx, &meshpb.SendRequest{
    DstNode: 12345,
    Payload: []byte("hello"),
    RequireAck: true,
})
```

## Architecture Details

### Key Components

- **Sessions**: TCP connections with handshake protocols, keepalive mechanisms, and lifecycle management.
- **Wire Protocol**: Efficient framing with 48-byte fixed headers for fast routing and message chunking support.
- **Routing**: Domain-aware routing tables with ECMP and path computation algorithms.
- **Storage**: Write-ahead logging and deduplication with pluggable backends (memory, file, Redis).
- **gRPC Services**: MeshData and MeshControl services with message tracking and delivery queues.

### Wire Protocol

The mesh uses an efficient wire protocol with:
- 48-byte fixed headers with `srcNode`, `dstNode`, and routing information for fast forwarding
- Length-prefixed framing for reliable message boundaries  
- Message chunking support for large payloads
- CBOR-encoded metadata for extensibility

## Security

- **TLS Support** - Optional TLS 1.3 for encrypted transport between nodes
- **Session Authentication** - Handshake protocols for secure session establishment
- **Message Integrity** - Reliable delivery with WAL persistence and deduplication

## Scalability

The mesh architecture supports scalable network topologies:

- **Domain-Based Organization**: Hierarchical topology management with domain-aware routing
- **Efficient Routing**: ECMP and path computation algorithms for optimal message delivery
- **Link-State Protocols**: Topology discovery and maintenance within domains
- **Session Management**: Efficient connection handling with automatic reconnection

## Observability

Built-in monitoring and debugging capabilities:

- **Message Tracking**: Comprehensive lifecycle tracking for all messages with delivery confirmation
- **Session Metrics**: Connection status, keepalive monitoring, and session lifecycle events
- **gRPC Metrics**: Request/response tracking and performance monitoring for API calls
- **Structured Logging**: Detailed logs for debugging and operational visibility

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Current Implementation Status

The mesh network currently provides:

1. **✅ Wire Protocol** - Efficient framing with 48-byte headers and message chunking
2. **✅ Session Management** - TCP connections with handshake protocols and keepalive
3. **✅ Storage Layer** - WAL and deduplication with multiple backend options
4. **✅ Routing System** - Domain-aware routing with ECMP and path computation
5. **✅ Topology Management** - Link-state protocols for topology discovery
6. **✅ gRPC Services** - MeshData and MeshControl APIs with message tracking
7. **✅ Integration** - Complete node binary with configuration and system integration

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPLv3). For commercial use or if you need a different license, please contact us for a separate commercial license.

## Security

For security issues, please email security@redb.co instead of opening a public issue.
