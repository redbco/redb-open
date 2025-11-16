## reDB Node
### The Open Data Infrastructure for the AI Era

reDB is a mesh-based overlay network for all data stores, streams and environments. It delivers one unified layer for access, replication, and control - across clouds, databases, and on-prem systems.

### What's new

- 🌐 Web Dashboard: Modern web interface for managing databases, mappings, streams, and MCP servers
- 📡 Stream Service: First-class support for message brokers (Kafka, MQTT, Kinesis, Pub/Sub, Event Hubs) with stream-to-database integration
- 💻 CLI Interactive mode: Interactive REPL mode to CLI with command history and tab completion
- 🔏 Data Transformations: Mappings now support built-in data transformations with copy-data, CDC replication and MCP server
- 🤖 MCP Server: Tables can be represented through mappings as MCP Resources or Tools
- 🔄 CDC-based Relationships: Real-time database synchronization with logical replication and cross-database change capture

### Why reDB

- 🔌 Connect any mix of SQL/NoSQL/vector/graph/streams without brittle pipelines
- 🧠 Unified schema model across paradigms with conversion and diffing
- 🚀 Real-time CDC replication with cross-database change capture
- 🔄 Zero-downtime migration workflows with automatic initial data sync
- 📡 Stream integration with message brokers (Kafka, MQTT, Kinesis, Pub/Sub, Event Hubs) and event routing
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

# Alternatively just start the CLI in interactive mode and run commands directly
./redb-cli
profiles create default
auth login --profile default
```

### Database Connectivity

Connect your databases and explore their structure:

```bash
# Connect your databases
databases connect --string "postgresql://user:password@localhost:5432/testdb1" --name "pg"
databases connect --string "mysql://user:password@localhost:3307/testdb2" --name "my"

# Show connected instances and databases
instances list
databases list

# Show the discovered tables and their metadata
databases show pg --tables
```

### Schema Management

Deploy and manage database schemas across different systems:

```bash
# Show the repositories and commits
repos list
branches show pg/main

# Deploy the PostgreSQL testdb1 to a new database in MySQL
commits deploy-schema pg/main/12345abc --instance my_instance --db-name new
```

### Schema Mapping

Map and transform an existing schema to a target

```bash
# Create a mapping between tables
mappings add --scope table --source pg.users --target new.users

# Show the mapping status
mappings list
mappings show pg_users_to_new_users

# Add a new mapping rule
mappings add-rule --mapping user-mapping --rule email_rule --source pg.users.email --target new.users.email

# Modify a rule by adding a transformation to it
mappings modify-rule --mapping user-mapping --rule email_rule --transformation uppercase

# Remove a mapping rule
mappings remove-rule --mapping user-mapping --rule email_rule --delete

# Validate a mapping
mappings validate pg_users_to_new_users
```

### Data Replication

Set up data synchronization between databases using mappings and CDC:

```bash
# The data can be copied by either using a one-time data copy, or a continuous CDC replication
# One-time data copy from PostgreSQL to MySQL
mappings copy-data pg_users_to_new_users

# Or set up real-time CDC replication
relationships add --mapping pg_users_to_new_users --type replication
relationships start pg_to_new

# Monitor the relationship status
relationships list
relationships show pg_to_new

# Manage the relationship lifecycle
relationships stop pg_to_new    # Pause synchronization
relationships start pg_to_new   # Resume synchronization
relationships remove pg_to_new  # Remove completely
```

### MCP Server (AI Integration)

Expose your data as resources and tools to AI agents using the Model Context Protocol:

```bash
# First, create a mapping for the data you want to expose
mappings add --scope table --source pg.users --target mcp://users_res

# Create a virtual MCP server on a specific port
mcpservers add --name mcp-server --port 9000

# Create a resource that exposes data through the mapping
mcpresources add --name users_res --mapping pg_users_to_mcp_users_res

# Attach the resource to your MCP server
mcpresources attach --resource users_res --server mcp-server

# Create a tool that allows querying the data
mcptools add --name query_users --mapping pg_users_to_mcp_users_res

# Attach the tool to your MCP server
mcptools attach --tool query_users --server mcp-server
```

Now your MCP server is running and can be used by AI agents like Claude Desktop, Cline, or any MCP-compatible client. For detailed MCP server management, see `docs/MCP_SERVER_MANAGEMENT.md`.

### Stream Integration

Connect to message brokers and integrate streaming data with your databases:

```bash
# Connect to a Kafka stream
streams connect --name kafka-prod --type kafka --string "localhost:9092"

# Connect to an MQTT broker
streams connect --name mqtt-local --type mqtt --string "tcp://localhost:1883"

# List connected streams
streams list

# Show stream details
streams show kafka-prod

# Create a mapping from a stream topic to a database table
mappings add --scope table --source kafka-prod.events --target pg.events

# Set up real-time stream-to-database routing
relationships add --mapping kafka_events_to_pg_events --type stream-routing
relationships start kafka_to_pg
```

For detailed information on stream adapters and integration patterns, see `docs/STREAM.md`.

### Mesh Networking

Create or join a distributed mesh for multi-node deployments:

```bash
# Seed a mesh (Node 1)
mesh seed

# Join a mesh (Node 2)
mesh join localhost:10001
```

### Make targets

- `make local`: build for host OS/arch
- `make build`: cross-compile Go for Linux by default + Rust mesh
- `make build-all`: linux/darwin/windows on amd64/arm64
- `make test`: run Go and Rust tests
- `make proto`, `make lint`, `make dev`

## Architecture (short)

Supervisor orchestrates microservices for Security, Core, Unified Model, Anchor, Stream, Transformation, Integration, Mesh, Client API, Webhook, MCP Server, and clients (CLI, Dashboard). Ports and deeper details in `docs/ARCHITECTURE.md`.

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
│   ├── stream/           # Stream broker integration (Kafka, MQTT, etc.)
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
- Stream service: `docs/STREAM.md`
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