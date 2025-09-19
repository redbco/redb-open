## Architecture

reDB Node is a distributed, policy-driven data mesh composed of independent microservices. A supervisor process orchestrates the lifecycle, configuration, health, and inter-service networking.

### High-level Principles

- **Shared gRPC pattern**: Consistent transport and lifecycle across services
- **Separation of concerns**: Auth, core domain, schema, network, and integration isolated
- **Policy-first**: Access control, masking, and compliance at the platform edge
- **AI-ready**: Built-in MCP server and schema context for AI agents

### Microservices

#### Supervisor (`cmd/supervisor/`) – gRPC 50000
Orchestrates start/stop, configuration distribution, service discovery, and health.

#### Security (`services/security/`) – gRPC 50051
Authentication, authorization (RBAC), multi-tenant sessions, token issuance.

#### Core (`services/core/`) – gRPC 50055
System-of-record for tenants, workspaces, regions, instances, repos, mappings, and policies.

#### Unified Model (`services/unifiedmodel/`) – gRPC 50052
Database abstraction layer. Versioning, comparison, analytics, and conversion leveraging `pkg/unifiedmodel`.

#### Anchor (`services/anchor/`) – gRPC 50057
Database connectivity and schema discovery. Supports many adapters across paradigms.

#### Transformation (`services/transformation/`) – gRPC 50054
Schema-aware functions for formatting, hashing, encoding, and controlled mutations.

#### Integration (`services/integration/`) – gRPC 50058
External integrations (LLMs, RAG systems, custom processors). CRUD and execution over gRPC.

#### Mesh (`services/mesh/`) – gRPC 50056, Mesh 10001
Distributed coordination, inter-node communication, and message routing.

#### Client API (`services/clientapi/`) – gRPC 50059, HTTP 8080
Primary REST/HTTP API for clients and the CLI. 50+ endpoints.

#### Webhook (`services/webhook/`) – gRPC 50053
Outbound event delivery with retries and delivery guarantees.

#### MCP Server (`services/mcpserver/`) – gRPC 50060
Model Context Protocol server to expose reDB resources, tools, and prompts to AI agents.

#### CLI (`cmd/cli/`)
Administrative and developer tooling over the Client API and internal gRPC.

#### Client Dashboard (`/web/dashboard/`) – HTTP 3000
Multi-tenant Next.js app for tenant, workspace, and mesh operations.

### Key Patterns

1) Shared BaseService lifecycle (`pkg/service/`): init → health → graceful shutdown
2) Strict security boundaries: auth via Security; policy evaluation at edges
3) Unified schema representation (`pkg/unifiedmodel/`) for all paradigms
4) Event and job orchestration handled via gRPC with strong contracts

### Ports (internal defaults)

- Supervisor: 50000
- Security: 50051
- Unified Model: 50052
- Webhook: 50053
- Transformation: 50054
- Core: 50055
- Mesh: 50056
- Anchor: 50057
- Integration: 50058
- Client API: 50059 (plus HTTP 8080)
- MCP Server: 50060

