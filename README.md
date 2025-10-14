## reDB Node
### The Data Portability Platform for the AI Era

reDB is a distributed, policy-driven data mesh that unifies access, mobility, and transformation across heterogeneous databases and clouds. Built for developers, data platform teams, and AI agents.

### What's new

- 🔄 CDC-based Relationships: Real-time database synchronization with logical replication and cross-database change capture
- ⚙️ Mesh microservice rewritten in Rust for efficiency and correctness (Tokio + Tonic)
- 🧠 Major upgrades to `pkg/unifiedmodel` and the Unified Model service: richer conversion engine, analytics/metrics, and privacy-aware detection
- 🧰 Makefile now builds Go services and the Rust mesh; Rust toolchain is required
- 📄 Documentation structure and content updated

### Why reDB

- 🔌 Connect any mix of SQL/NoSQL/vector/graph without brittle pipelines
- 🧠 Unified schema model across paradigms with conversion and diffing
- 🚀 Real-time CDC replication with cross-database change capture
- 🔄 Zero-downtime migration workflows with automatic initial data sync
- 🔐 Policy-first access with masking and tenant isolation
- 🤖 AI-native via MCP: expose data resources and tools to LLMs safely

## Build & Quick Start

Prerequisites: Go 1.23+, Rust (stable), protoc, PostgreSQL 17+, Redis

```bash
git clone https://github.com/redbco/redb-open.git
cd redb-open
make dev-tools   # optional Go tools
make local       # builds Go services + Rust mesh

# Copy the sample configuration before starting the application
cp ./sample_config/config.yaml ./bin
cd ./bin

# Initialize and start the application
./redb-node --initialize
./redb-node &

```

Full install docs: see `docs/INSTALL.md`.

## Getting started with the application

### Initial Setup

After starting, create a profile and authenticate with the CLI:

```bash
# Create your user profile and login
./redb-cli profiles create default
./redb-cli auth login --profile default
```

### Database Connectivity

Connect your databases and explore their structure:

```bash
# Connect your databases
./redb-cli databases connect --string "postgresql://user:password@localhost:5432/testdb1" --name "pg"
./redb-cli databases connect --string "mysql://user:password@localhost:3307/testdb2" --name "my"

# Show connected instances and databases
./redb-cli instances list
./redb-cli databases list

# Show the discovered tables and their metadata
./redb-cli databases show pg --tables
```

### Schema Management

Deploy and manage database schemas across different systems:

```bash
# Show the repositories and commits
./redb-cli repos list
./redb-cli branches show pg/main

# Deploy the PostgreSQL testdb1 to a new database in MySQL
./redb-cli commits deploy-schema pg/main/12345abc --instance my_instance --db-name deployed1
```

### Data Replication

Set up data synchronization between databases using mappings and CDC:

```bash
# Create a mapping between tables
./redb-cli mappings add --scope table --source pg.users --target deployed1.users
./redb-cli mappings show pg_users_to_deployed1_users

# The data can be copied by either using a one-time data copy, or a continuous CDC replication
# One-time data copy from PostgreSQL to MySQL
./redb-cli mappings copy-data pg_users_to_deployed1_users

# Or set up real-time CDC replication
./redb-cli relationships add --mapping pg_users_to_deployed1_users --type replication
./redb-cli relationships start pg_to_deployed1

# Monitor the relationship status
./redb-cli relationships list
./redb-cli relationships show pg_to_deployed1

# Manage the relationship lifecycle
./redb-cli relationships stop pg_to_deployed1    # Pause synchronization
./redb-cli relationships start pg_to_deployed1   # Resume synchronization
./redb-cli relationships remove pg_to_deployed1  # Remove completely
```

### MCP Server (AI Integration)

Expose your data as resources and tools to AI agents using the Model Context Protocol:

```bash
# First, create a mapping for the data you want to expose
./redb-cli mappings add --scope table --source pg.users --target mcp://users_resource

# Create an MCP server on a specific port
./redb-cli mcpservers add --name my-server --port 9000

# Create a resource that exposes data through the mapping
./redb-cli mcpresources add --name users_resource --mapping pg_users_to_mcp_users_resource

# Attach the resource to your MCP server
./redb-cli mcpresources attach --resource users_resource --server my-server

# Create a tool that allows querying the data
./redb-cli mcptools add --name query_users --mapping pg_users_to_mcp_users_resource

# Attach the tool to your MCP server
./redb-cli mcptools attach --tool query_users --server my-server
```

Now your MCP server is running and can be used by AI agents like Claude Desktop, Cline, or any MCP-compatible client. For detailed MCP server management, see `docs/MCP_SERVER_MANAGEMENT.md`.

### Mesh Networking

Create or join a distributed mesh for multi-node deployments:

```bash
# Seed a mesh (Node 1)
./redb-cli mesh seed

# Join a mesh (Node 2)
./redb-cli mesh join localhost:10001
```

### Make targets

- `make local`: build for host OS/arch
- `make build`: cross-compile Go for Linux by default + Rust mesh
- `make build-all`: linux/darwin/windows on amd64/arm64
- `make test`: run Go and Rust tests
- `make proto`, `make lint`, `make dev`

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
- MCP Server Management: `docs/MCP_SERVER_MANAGEMENT.md`

## Contributing

We welcome issues and PRs. Read `CONTRIBUTING.md` for guidelines and our simple governance.

## License

AGPL-3.0 for open-source use (`LICENSE`). Commercial license available (`LICENSE-COMMERCIAL.md`).

## Getting started (recap)

1) Install: Go 1.23+, Rust, protoc, PostgreSQL 17, Redis
2) Build: `make local`
3) Initialize: `./bin/redb-node --initialize`
4) Start: `./bin/redb-node`
5) Create profile: `./bin/redb-cli profiles create default --hostname localhost:8080`
6) Login: `./bin/redb-cli auth login --profile default`

---

**reDB Node** provides a comprehensive open source platform for managing heterogeneous database environments with advanced features including real-time CDC replication, schema version control, cross-database synchronization, data transformation pipelines, distributed mesh networking, and AI-powered database operations.

## Community

- Documentation: [Project Wiki](https://github.com/redbco/redb-open/wiki)
- Issues: [GitHub Issues](https://github.com/redbco/redb-open/issues)
- Discussions: [GitHub Discussions](https://github.com/redbco/redb-open/discussions)
- Discord: [Join us](https://discord.gg/rV4WBZAw5D)

---

**reDB Node** is an open source project maintained by the community. We believe in the power of open source to drive innovation in database management and distributed systems.