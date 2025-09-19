## Anchor Service

Database connectivity and data operations microservice. Manages live connections to heterogeneous databases, discovers schemas, executes data operations, and orchestrates replication. Integrates tightly with Core and Unified Model services.

### Location and ports

- Code: `services/anchor/`
- Default gRPC port: 50057
- Supervisor integration: connects to supervisor (50000) unless `--standalone`

### Responsibilities

- Manage instance and database connections across many paradigms
- Discover and normalize schemas for the Unified Model pipeline
- Execute data operations (fetch/insert/execute) with streaming support
- Orchestrate replication sources and table sets
- Watch configuration, schemas, and replication status; reconcile state

### Architecture

- Base service lifecycle via `pkg/service` (Initialize → Start → Health → Stop)
- Shared gRPC server registration: `AnchorService` registered on start
- Internal Postgres used for node identity and state
- gRPC clients to Core (catalog/config) and Unified Model (schema/translation)
- Watchers
  - ConfigWatcher: connect/disconnect databases and instances from Core definitions
  - SchemaWatcher: discover schemas, push to Unified Model, monitor changes
  - ReplicationWatcher: manage replication sources and table sets

Key files
- Entrypoint: `services/anchor/cmd/main.go`
- Engine: `services/anchor/internal/engine/`
- Database adapters: `services/anchor/internal/database/<adapter>/`
- Protos: `api/proto/anchor/v1/anchor.proto`

### gRPC API surface (high level)

- Connections: Connect/Update/Disconnect Instance & Database
- Metadata & Schema: GetInstanceMetadata, GetDatabaseMetadata, GetDatabaseSchema, DeployDatabaseSchema
- Data: FetchData (+Stream/+ToCache), InsertData (+Stream/+FromCache), ExecuteCommand, WipeDatabase
- Replication: Create/Reconnect/Modify/Remove ReplicationSource

See `api/proto/anchor/v1/anchor.proto` for full request/response types.

### Adapters and paradigms

Adapters live under `internal/database/` and implement a common contract:

- connect/disconnect
- schema discovery and normalization hooks
- data fetch/insert/execute
- replication primitives (where supported)

Representative adapters: postgres, mysql, mssql, oracle, mariadb, db2, cockroach, cassandra, clickhouse, mongodb, cosmosdb, neo4j, edgedb, redis, dynamodb, snowflake, milvus, weaviate, pinecone, chroma, pinecone, and more.

### Service interactions

- Core service: stores/retrieves instance/database configs; status updates on connect/disconnect; relationship/replication metadata
- Unified Model service: receives discovered schemas; provides translation and validation; assists comparison and change alerts
- Supervisor: lifecycle, health, restart keys; service location hints

### Health & metrics

- Health checks: gRPC server, engine, internal DB, external service connections, watchers
- Metrics: requests processed, errors (extensible)

### Standalone mode

Run without supervisor for local adapter development and testing:

```bash
./bin/redb-anchor --standalone --port 50057
```

### Extending Anchor

1) Create a new adapter in `internal/database/<adapter>/`
2) Implement connection, schema, data, and replication (if applicable)
3) Wire into the database manager and config repository
4) Add basic tests and discovery fixtures

See also: `docs/DATABASE_SUPPORT.md`, `docs/ADDING_NEW_DATABASE_SUPPORT.md`, and `docs/DATABASE_ADAPTER_IMPLEMENTATION_GUIDE.md`.


