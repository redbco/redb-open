# reDB Node
## The Data Portability Platform for the AI Era

reDB is a distributed, policy-driven data mesh that enables True Data Portability across any mix of databases, warehouses, clouds, and environments. Built for developers, architects, and AI systems, reDB unifies data access, data mobility, and schema transformation into one open platform.

## Why reDB?

Modern enterprises operate in fragmented data ecosystems: multi-cloud, hybrid, on-prem, and filled with incompatible database technologies—relational, document, key-value, vector, graph, and more. These silos limit agility, complicate migrations, and throttle AI.

reDB solves this with a unified approach to enterprise data infrastructure:
- 🔌 reDB Mesh: A decentralized network that securely connects all your databases—cloud, on-prem, or hybrid—without brittle pipelines or manual tunnels.
- 🧠 Unified Schema Model: Translate and normalize structures across relational, NoSQL, and graph databases into a single, interoperable model.
- 🚀 Zero-Downtime Migration & Replication: Replicate and migrate live data across environments—reliably, securely, and in real time.
- 🔐 Policy-Driven Data Obfuscation: Automatically protect sensitive data with contextual masking and privacy rules at the access layer.
- 🤖 AI-Ready Access: Through the Model Context Protocol (MCP), reDB gives AI agents and tools frictionless access to data with full compliance and schema context.

This project is for:
- Data platform teams who need real-time migrations and multi-database interoperability
- AI/ML engineers who need contextual access to distributed data
- Developers who want production-like data access with built-in privacy
- Enterprises undergoing cloud transitions, database modernization, or data compliance transformations

Key capabilities that this project aims to provide:
- ✅ Multi-database interoperability across SQL, NoSQL, vector, and graph
- ✅ Automated schema versioning and transformation
- ✅ Zero-downtime, bidirectional replication
- ✅ Real-time developer data environments with obfuscation
- ✅ Quantum-resistant encryption and policy-based access control
- ✅ Distributed MCP server for scalable AI/IDE integration

**We want to build the foundation for future-proof data infrastructure.**

## Build Instructions

This project uses Make for building and managing the application.

### Prerequisites

- **Go 1.23+** - [Download](https://golang.org/dl/)
- **PostgreSQL 17+** - [Download](https://www.postgresql.org/download/)
- **Redis Server** - [Download](https://redis.io/download)
- **Protocol Buffers Compiler** - [Installation Guide](https://grpc.io/docs/protoc-installation/)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/redbco/redb-open.git
cd redb-open

# Install development tools
make dev-tools

# Build for local development
make local

# Run tests
make test
```

### Available Make Targets

- `make all` - Clean, generate proto files, build, and test
- `make build` - Build all services (cross-compile for Linux by default)
- `make local` - Build for local development (host OS)
- `make dev` - Development build (clean, proto, build, test)
- `make clean` - Remove build artifacts
- `make test` - Run all tests
- `make proto` - Generate Protocol Buffer code
- `make lint` - Run linter
- `make dev-tools` - Install development tools
- `make build-all` - Build for multiple platforms (Linux/macOS, amd64/arm64)
- `make install` - Install binaries (Linux only)
- `make version` - Show version information

The build process creates binaries in the `bin/` directory for local builds and `build/` directory for multi-platform builds.

## Installation Instructions

### From Source

```bash
# Clone and build
git clone https://github.com/redbco/redb-open.git
cd redb-open
make build

# Install PostgreSQL 17 as prerequisite
sudo apt install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt update
sudo apt -y install postgresql

# Create an admin user that the application can use for initialization
sudo -u postgres psql
CREATE USER your_admin_user WITH ENCRYPTED PASSWORD 'your_admin_password' CREATEDB CREATEROLE LOGIN;
exit

# Install Redis Server as a prerequisite
sudo apt install redis-server

# Initialize the reDB installation
./bin/redb-node --initialize

# If prompted, provide the PostgreSQL details
Enter PostgreSQL username [postgres]: your_admin_user
Enter PostgreSQL password: *your_admin_password*
Enter PostgreSQL host [localhost]: 
Enter PostgreSQL port [5432]: 

# Select "y" to create the default tenant and user - required for a fresh install
Would you like to create a default tenant and user? (Y/n): y

# Enter the details of the default tenant and user
Enter tenant name: tenant_name
Enter admin user email: you@domain.com
Enter admin user password: *your_new_login_password*
Confirm password: *your_new_login_password*

# Initialization is complete, ready to start the application
./bin/redb-node

# The application can be run in the background as a service
```

### Using the application for the first time

```bash
# Logging in
redb@redb-demo:~$ ./bin/redb-cli auth login
Username (email): demo@redb.co
Password: 
Hostname (default: localhost:8080): 
Tenant URL: demo
Successfully logged in as demo@redb.co
Session: reDB CLI (ID: session_1752410814767386264_1nkJhJM4)

Select workspace (press Enter to skip): 
No workspace selected. Use 'redb-cli select workspace <name>' to select one later.
redb@redb-demo:~$ 

# Creating your first workspace
redb@redb-demo:~$ ./bin/redb-cli workspaces add
Workspace Name: demo
Description (optional): reDB demo workspace
Successfully created workspace 'demo' (ID: ws_0000019803D4CBCEBCA9C6AB2D)
redb@redb-demo:~$

# Selecting the current workspace
redb@redb-demo:~$ ./bin/redb-cli select workspace demo
Selected workspace: demo (ID: ws_0000019803D4CBCEBCA9C6AB2D)
redb@redb-demo:~$
```

## Architecture Overview

The reDB Node consists of 12 microservices orchestrated by a supervisor service, providing a comprehensive platform for managing heterogeneous database environments.

### Core System Architecture

```
┌────────────────────────────────────────────────────────┐
│                   Supervisor Service                   │
│               (Orchestrates all services)              │
└─────────────────────────┬──────────────────────────────┘
                          │
             ┌────────────┼─────────────┐
             │            │             │
       ┌──────────┐ ┌─────────────┐ ┌───▼────┐
       │ Security │ │Unified Model│ │  Core  │
       │ Service  │ │   Service   │ │Service │
       └──────────┘ └─────────────┘ └────────┘
             │            │             │
             ┼────────────┼─────────────┤
             │            │             │
       ┌─────▼────┐  ┌────▼─────┐  ┌────▼─────┐
       │ Anchor   │  │  Mesh    │  │Transform │
       │ Service  │  │ Service  │  │ Service  │
       └──────────┘  └──────────┘  └──────────┘
             │            │             │
             ┼────────────┼─────────────┤
             │            │             │
       ┌─────▼────┐  ┌────▼─────┐  ┌────▼─────┐
       │Client API│  │Query API │  │ Service  │
       │          │  │          │  │   API    │
       └──────────┘  └──────────┘  └──────────┘
             │            │             │
             ┼────────────┼─────────────┘
             │            │
       ┌─────▼────┐  ┌────▼─────┐
       │ Webhook  │  │   MCP    │
       │ Service  │  │ Server   │
       └──────────┘  └──────────┘
```

## Microservices Overview

### Foundation Services

#### **Supervisor Service** (`cmd/supervisor/`) - Port 50000
Central service orchestrator managing lifecycle, health monitoring, and configuration distribution for all microservices.

#### **Security Service** (`services/security/`) - Port 50051
Authentication and authorization hub providing JWT tokens, session management, RBAC, and multi-tenant security.

#### **Core Service** (`services/core/`) - Port 50062
Central business logic hub managing tenants, workspaces, databases, repositories, mappings, and policies.

### Data Services

#### **Unified Model Service** (`services/unifiedmodel/`) - Port 50053
Database abstraction layer with 16+ database adapters, schema translation, and cross-database type conversion.

#### **Anchor Service** (`services/anchor/`) - Port 50055
Database connectivity service managing direct connections, schema monitoring, and data replication across all supported databases.

#### **Transformation Service** (`services/transformation/`) - Port 50054
Data processing service providing transformation functions, obfuscation, anonymization, and schema-aware mutations.

### Network Services

#### **Mesh Service** (`services/mesh/`) - Port 50056
Distributed coordination service handling inter-node communication, consensus management, and message routing via WebSocket.

### API Services

#### **Client API** (`services/clientapi/`) - Port 50059
Primary REST API providing 50+ endpoints for resource management, serving CLI and web clients.

#### **Service API** (`services/serviceapi/`) - Port 50057
Administrative REST API for tenant management, mesh operations, and service configuration.

#### **Query API** (`services/queryapi/`) - Port 50058
Database query execution API supporting multi-database queries, result transformation, and transaction management.

### Integration Services

#### **Webhook Service** (`services/webhook/`) - Port 50060
External system integration via webhooks for sending events to external systems.

#### **MCP Server Service** (`services/mcpserver/`) - Port 50061
Model Context Protocol server enabling AI/LLM integration with database resources, tools, and prompt templates.

### Client Applications

#### **CLI** (`cmd/cli/`)
Command-line interface with for system management, database operations, and administrative tasks.

## Database Support Matrix

The Anchor Service supports 27+ database types across 8 paradigms through specialized adapters:

### Database Categorization by Paradigm

#### **RELATIONAL**
**Core Characteristics**: Row-based storage, normalized structure, SQL queries, ACID transactions

- **PostgreSQL** (postgres)
- **MySQL** (mysql)
- **Microsoft SQL Server** (mssql)
- **Oracle Database** (oracle)
- **MariaDB** (mariadb)
- **IBM Db2** (db2)
- **CockroachDB** (cockroachdb) - Distributed SQL but fundamentally relational
- **Snowflake** (snowflake) - Cloud data warehouse but SQL-based relational
- **DuckDB** (duckdb) - Analytical but fundamentally relational with SQL

#### **DOCUMENT**
**Core Characteristics**: Document-based storage, flexible schema, nested structures

- **MongoDB** (mongodb)
- **Azure CosmosDB** (cosmosdb) - Multi-model but primarily document-oriented

#### **GRAPH**
**Core Characteristics**: Graph-based storage, relationships as first-class citizens, traversal queries

- **Neo4j** (neo4j)
- **EdgeDB** (edgedb) - Object-relational but with graph capabilities and object modeling

#### **VECTOR**
**Core Characteristics**: High-dimensional vector storage, similarity search, embedding-focused

- **Chroma** (chroma)
- **Milvus** (milvus) - Includes Zilliz (managed milvus)
- **Pinecone** (pinecone)
- **LanceDB** (lancedb)
- **Weaviate** (weaviate)

#### **COLUMNAR**
**Core Characteristics**: Column-oriented storage, optimized for analytical queries, time-series focus

- **ClickHouse** (clickhouse)
- **Apache Cassandra** (cassandra) - Wide-column store, partition-key based

#### **KEY_VALUE**
**Core Characteristics**: Simple key-value pairs, in-memory focus, limited query capabilities

- **Redis** (redis)

#### **SEARCH**
**Core Characteristics**: Inverted index storage, full-text search optimization, document scoring

- **Elasticsearch** (elasticsearch)

#### **WIDE_COLUMN**
**Core Characteristics**: Flexible schema, partition-based, NoSQL query patterns

- **Amazon DynamoDB** (dynamodb)

#### **OBJECT_STORAGE**
**Core Characteristics**: File/blob storage, hierarchical key structure, metadata-based organization

- **Amazon S3** (s3)
- **Google Cloud Storage** (gcs)
- **Azure Blob Storage** (azure_blob)
- **MinIO** (minio)

### Summary by Category

| Category | Count | Databases |
|----------|-------|-----------|
| **RELATIONAL** | 9 | postgres, mysql, mssql, oracle, mariadb, db2, cockroachdb, snowflake, duckdb |
| **DOCUMENT** | 2 | mongodb, cosmosdb |
| **GRAPH** | 2 | neo4j, edgedb |
| **VECTOR** | 5 | chroma, milvus, pinecone, lancedb, weaviate |
| **COLUMNAR** | 2 | clickhouse, cassandra |
| **KEY_VALUE** | 1 | redis |
| **SEARCH** | 1 | elasticsearch |
| **WIDE_COLUMN** | 1 | dynamodb |
| **OBJECT_STORAGE** | 4 | s3, gcs, azure_blob, minio |

**Total**: 27 databases across 8 paradigms

## CLI Command Interface

The CLI provides commands organized into functional categories:

### Core Resource Management
- **Authentication**: `auth login`, `auth logout`, `auth profile`, `auth status`, `auth password`
- **Tenants & Users**: `tenants list`, `tenants show`, `tenants add`, `tenants modify`, `tenants delete`
- **Workspaces & Environments**: `workspaces list`, `workspaces show`, `workspaces add`, `workspaces modify`, `workspaces delete`
- **Regions**: `regions list`, `regions show`, `regions add`, `regions modify`, `regions delete`

### Database Operations
- **Instances**: `instances connect`, `instances list`, `instances show`, `instances modify`
- **Databases**: `databases connect`, `databases list`, `databases wipe`, `databases clone table-data`
- **Schema Management**: Database schema inspection and modification

### Version Control & Schema
- **Repositories**: `repos list`, `repos show`, `repos add`, `repos modify`
- **Branches**: `branches show`, `branches attach`, `branches detach`
- **Commits**: `commits show`, schema version management

### Data Integration
- **Mappings**: `mappings list`, `mappings add table-mapping`, column-to-column relationship definitions
- **Relationships**: Replication and migration relationship management
- **Transformations**: Data transformation and obfuscation functions

### Mesh & Network
- **Mesh Operations**: `mesh seed`, `mesh join`, `mesh show topology`, node management
- **Satellites & Anchors**: Specialized node type management
- **Routes**: Network topology and routing configuration

### AI Integration
- **MCP Servers**: Model Context Protocol server management
- **MCP Resources**: AI-accessible data resource configuration
- **MCP Tools**: AI tool and function definitions

## Shared Package Libraries

The `pkg/` directory contains reusable components shared across all microservices:

- **`pkg/config/`** - Centralized configuration management and validation
- **`pkg/database/`** - Database connection utilities (PostgreSQL, Redis)
- **`pkg/encryption/`** - Cryptographic operations and secure key management
- **`pkg/grpc/`** - gRPC client/server utilities and middleware
- **`pkg/health/`** - Health check framework and service monitoring
- **`pkg/keyring/`** - Secure key storage and cryptographic key management
- **`pkg/logger/`** - Structured logging framework used across all services
- **`pkg/models/`** - Common data models and shared structures
- **`pkg/service/`** - BaseService framework for standardized microservice lifecycle
- **`pkg/syslog/`** - System logging integration and configuration

## Project Structure Overview

```
redb-open/
├── cmd/                    # Command-line applications
│   ├── cli/               # CLI client (200+ commands)
│   └── supervisor/        # Service orchestrator
├── services/              # Core microservices
│   ├── anchor/           # Database connectivity (16+ adapters)
│   ├── clientapi/        # Primary REST API (50+ endpoints)
│   ├── core/             # Central business logic hub
│   ├── mcpserver/        # AI/LLM integration (MCP protocol)
│   ├── mesh/             # Distributed coordination and consensus
│   ├── queryapi/         # Database query execution interface
│   ├── security/         # Authentication and authorization
│   ├── serviceapi/       # Administrative and service management
│   ├── transformation/   # Data processing and obfuscation
│   ├── unifiedmodel/     # Database abstraction and schema translation
│   └── webhook/          # External system integration
├── pkg/                   # Shared libraries and utilities
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
├── api/proto/            # Protocol Buffer definitions
└── scripts/              # Database schemas and deployment
```

## Key Architectural Principles

### 1. **Shared gRPC Server Pattern**
All services use a standardized gRPC communication pattern eliminating port conflicts and ensuring proper service registration through the BaseService framework.

### 2. **Microservice Lifecycle Management**
The BaseService framework (`pkg/service/`) provides standardized service initialization, health monitoring, graceful shutdown, and dependency management across all services.

### 3. **Database Abstraction Layer**
The Unified Model Service provides a common interface for multiple database types, enabling cross-database operations, schema translation, and type conversion without vendor lock-in.

### 4. **Distributed Mesh Architecture**
The Mesh Service enables multi-node deployments with peer-to-peer communication, consensus algorithms, and distributed state synchronization for high availability.

### 5. **Security-First Design**
All requests are authenticated through the Security Service using JWT tokens, session management, and role-based access control (RBAC) with multi-tenant isolation.

### 6. **Event-Driven Integration**
The Webhook Service provides reliable event delivery to external systems with retry logic and delivery guarantees for real-time notifications.

### 7. **AI-Native Architecture**
Built-in Model Context Protocol (MCP) server enables seamless AI/LLM integration with database resources, tools, and prompt templates.

## Contributing

We welcome contributions from the open source community! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on how to:

- Set up your development environment
- Submit bug reports and feature requests
- Contribute code and documentation
- Follow our coding standards
- Participate in our community

### Current Governance Phase

This project is currently in **Phase 1: Single Maintainer** governance. This means:
- 1 approval required for pull requests
- Basic CI/CD checks (build, test, lint, security)
- Maintainer bypass available for emergencies
- Simple CODEOWNERS structure

As the community grows, governance will evolve through phases. See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete governance evolution plan.

## License

This project is dual-licensed:

- **Open Source**: [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0)
- **Commercial**: Available under a commercial license for proprietary use

### AGPL-3.0 License Summary

The AGPL-3.0 license requires that:
- Any modifications to the software must be made available to users
- If you run a modified version on a server and let other users communicate with it there, you must make the modified source code available to them
- The source code must be accessible to all users who interact with the software over a network

For commercial licensing options, please see [LICENSE-COMMERCIAL.md](LICENSE-COMMERCIAL.md) or contact us directly.

## Getting Started

1. **Install Prerequisites**: Go 1.23+, PostgreSQL 17, Redis Server
2. **Build the System**: `make local` or `make build`
3. **Initialize**: `./bin/redb-node --initialize`
4. **Start Services**: `./bin/redb-node`
5. **Access CLI**: `./bin/redb-cli auth login`

For detailed installation instructions, see the Installation Instructions section above.

---

**reDB Node** provides a comprehensive open source platform for managing heterogeneous database environments with advanced features including schema version control, cross-database replication, data transformation pipelines, distributed mesh networking, and AI-powered database operations.

## Support

- **Documentation**: [Project Wiki](https://github.com/redbco/redb-open/wiki)
- **Issues**: [GitHub Issues](https://github.com/redbco/redb-open/issues)
- **Discussions**: [GitHub Discussions](https://github.com/redbco/redb-open/discussions)
- **Community**: [Discord/Slack/Forum] https://discord.gg/K3UkDYXG77

---

**reDB Node** is an open source project maintained by the community. We believe in the power of open source to drive innovation in database management and distributed systems.