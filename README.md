## reDB Node
### The Data Portability Platform for the AI Era

reDB is a distributed, policy-driven data mesh that unifies access, mobility, and transformation across heterogeneous databases and clouds. Built for developers, data platform teams, and AI agents.

### What’s new

- ⚙️ Mesh microservice rewritten in Rust for efficiency and correctness (Tokio + Tonic)
- 🧠 Major upgrades to `pkg/unifiedmodel` and the Unified Model service: richer conversion engine, analytics/metrics, and privacy-aware detection
- 🧰 Makefile now builds Go services and the Rust mesh; Rust toolchain is required
- 📄 Documentation structure and content updated

### Why reDB

- 🔌 Connect any mix of SQL/NoSQL/vector/graph without brittle pipelines
- 🧠 Unified schema model across paradigms with conversion and diffing
- 🚀 Zero-downtime replication and migration workflows
- 🔐 Policy-first access with masking and tenant isolation
- 🤖 AI-native via MCP: expose data resources and tools to LLMs safely

## Build & Quick Start

Prerequisites: Go 1.23+, Rust (stable), protoc, PostgreSQL 17+, Redis

```bash
git clone https://github.com/redbco/redb-open.git
cd redb-open
make dev-tools   # optional Go tools
make local       # builds Go services + Rust mesh
./bin/redb-node --initialize
./bin/redb-node &
./bin/redb-cli auth login
```

Full install docs: see `docs/INSTALL.md`.

### Make targets

- `make local`: build for host OS/arch
- `make build`: cross-compile Go for Linux by default + Rust mesh
- `make build-all`: linux/darwin/windows on amd64/arm64
- `make test`: run Go and Rust tests
- `make proto`, `make lint`, `make dev`

## First login

After starting, authenticate with the CLI:

```bash
./bin/redb-cli auth login
```

## Architecture (short)

Supervisor orchestrates microservices for Security, Core, Unified Model, Anchor, Transformation, Integration, Mesh, Client API, Webhook, MCP Server, and clients (CLI, Dashboard). Ports and deeper details in `docs/ARCHITECTURE.md`.

## Database support

Adapters cover relational, document, graph, vector, search, key-value, columnar, wide-column, and object storage. See `docs/DATABASE_SUPPORT.md` for the current matrix and how to add adapters.

## CLI

See `docs/CLI_REFERENCE.md` for command groups and examples.

## Unified Model

Shared schema layer and microservice for cross-paradigm representation, comparison, analytics, conversion, and detection. See `docs/UNIFIED_MODEL.md`.

## Project structure

```
redb-open/
├── cmd/                  # Command-line applications
│   ├── cli/              # CLI client (200+ commands)
│   └── supervisor/       # Service orchestrator
├── services/             # Core microservices
│   ├── anchor/           # Database connectivity (16+ adapters)
│   ├── clientapi/        # Primary REST API (50+ endpoints)
│   ├── core/             # Central business logic hub
│   ├── mcpserver/        # AI/LLM integration (MCP protocol)
│   ├── mesh/             # Mesh protocol and networking
│   ├── queryapi/         # Database query execution interface
│   ├── security/         # Authentication and authorization
│   ├── serviceapi/       # Administrative and service management
│   ├── transformation/   # Internal data processing (no external integrations)
│   ├── integration/      # External integrations (LLMs, RAG, custom)
│   ├── unifiedmodel/     # Database abstraction and schema translation
│   └── webhook/          # External system integration
├── pkg/                  # Shared libraries and utilities
│   ├── config/           # Configuration management
│   ├── database/         # Database connection utilities
│   ├── encryption/       # Cryptographic operations
│   ├── grpc/             # gRPC client/server utilities
│   ├── health/           # Health monitoring framework
│   ├── keyring/          # Secure key management
│   ├── logger/           # Structured logging
│   ├── models/           # Common data models
│   ├── service/          # BaseService lifecycle framework
│   └── syslog/           # System logging integration
├── web/dashboard/        # Web dashboard
├── api/proto/            # Protocol Buffer definitions
└── scripts/              # Database schemas and deployment
```

## Docs

- Architecture: `docs/ARCHITECTURE.md`
- Install & run: `docs/INSTALL.md`
- Database support: `docs/DATABASE_SUPPORT.md`
- CLI reference: `docs/CLI_REFERENCE.md`
- Dashboard: `docs/DASHBOARD.md`
 - Anchor service: `docs/ANCHOR.md`

## Contributing

We welcome issues and PRs. Read `CONTRIBUTING.md` for guidelines and our simple governance.

## License

AGPL-3.0 for open-source use (`LICENSE`). Commercial license available (`LICENSE-COMMERCIAL.md`).

## Getting started (recap)

1) Install: Go 1.23+, Rust, protoc, PostgreSQL 17, Redis
2) Build: `make local`
3) Initialize: `./bin/redb-node --initialize`
4) Start: `./bin/redb-node`
5) Login: `./bin/redb-cli auth login`

---

**reDB Node** provides a comprehensive open source platform for managing heterogeneous database environments with advanced features including schema version control, cross-database replication, data transformation pipelines, distributed mesh networking, and AI-powered database operations.

## Community

- Documentation: [Project Wiki](https://github.com/redbco/redb-open/wiki)
- Issues: [GitHub Issues](https://github.com/redbco/redb-open/issues)
- Discussions: [GitHub Discussions](https://github.com/redbco/redb-open/discussions)
- Discord: [Join us](https://discord.gg/K3UkDYXG77)

---

**reDB Node** is an open source project maintained by the community. We believe in the power of open source to drive innovation in database management and distributed systems.