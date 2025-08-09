# Mesh Microservice Architecture

This defines the full architecture and implementation plan for a Golang mesh microservice that provides asymmetric, NAT-friendly, multi-path connectivity and reliable data delivery between application nodes. The implementation leverages the shared packages in `/pkg` and follows the established engine/server/service pattern used by all microservices.

## Goals & Core Requirements
- Transport:
    - Outbound WebSocket connections (over HTTPS/TLS) to traverse NAT/firewalls; full-duplex messaging.
    - Listener for inbound WebSocket connections (over HTTPS/TLS)
- Topology: Asymmetric — not all nodes can connect to all others. Support multi-hop forwarding.
- Routing: Link-state view with shortest-path, multi-path load balancing across simultaneous paths using bandwidth/latency/utilization.
- Control Plane:
    - Gossip/Serf-like membership & health detection.
    - Mesh-level consensus: ensure system/internal-DB updates are durably delivered to all nodes.
    - No static seed node; all nodes equal after joining, but use Raft for leadership/elections where needed.
- Data Plane:
	- Three traffic classes over gRPC-facing API:
	    1.	System updates (mesh-wide propagation)
	    2.	Tenant client data (one-to-one / one-to-many; critical, ordered)
	    3.	Internal DB updates (one-to-all; ordered)
	- Client-data consensus: prove reception + processing by relevant target nodes; ordered, idempotent, exactly-once semantics at stream level.
    - Encryption:
        - Use tenant-specific encryption keys for all tenant client data (via `/pkg/encryption`)
        - Use mesh/node encryption keys for system and internal DB updates
- Persistence per node: 
    - Local PostgreSQL for application data (durable metadata/state) via `/pkg/database`
    - BoltDB for Raft consensus logs and state
    - Redis for queues/ephemeral data and deduplication via `/pkg/database`
- Supervisor integration: gRPC both ways; base server/service/engine pattern; shared global config via `/pkg/service`.
- Scalability: Goroutine-based, non-blocking I/O, bounded worker pools, backpressure, resumable streams.


## High-Level Component Map

Repository: github.com/redbco/redb-open

```
services/mesh/
    cmd/
        main.go                     # Uses /pkg/service.BaseService
    internal/
        engine/                     # Engine/Server/Service pattern
            engine.go               # Core business logic
            server.go               # gRPC server implementation
            service.go              # Service interface implementation
        config/                     # Service-specific configuration
            model.go                # Configuration models
            repository.go           # Config persistence
        transport/ws/               # WebSocket transport layer with VirtualLink
            dialer.go               # Outbound connections
            listener.go             # Inbound connections
            virtuallink.go          # VirtualLink abstraction (future enhancement)
            lanes.go                # Lane management and configuration (future enhancement)
            mux.go                  # Multiplexing across lanes (future enhancement)
            frame.go                # Message framing with lane_id
            auth.go                 # Authentication
            backpressure.go         # Per-lane credit management (future enhancement)
            health.go               # Lane health monitoring (future enhancement)
        gossip/                     # Membership & health detection
            swim.go                 # SWIM protocol implementation
            membership.go           # Membership management
        consensus/raft/             # Raft consensus engine
            node.go                 # Raft node implementation
            storage.go              # BoltDB storage interface
            transport.go            # Raft transport over WS
            groups.go               # Group management
        topology/                   # Link-state topology
            lsdb.go                 # Link-state database
            probes.go               # Network probes
            ksp.go                  # K-shortest paths
            scoring.go              # Path scoring
        router/                     # Message routing
            router.go               # Core routing logic
            scheduler.go            # Multi-path scheduling
            credits.go              # Flow control
            reroute.go              # Fast reroute
        streams/                    # Stream management
            stream.go               # Stream abstraction
            reorder.go              # Message reordering
            chunker.go              # Message chunking
        dataplane/                  # Data plane handlers
            system.go               # System update handlers
            clientdata.go           # Client data handlers
            dbupdates.go            # DB update handlers
        controlplane/               # Control plane orchestration
            fanout.go               # Message fanout
            dsg_manager.go          # Data stream group management
        queue/                      # Queue management
            redis_outbox.go         # Redis outbox implementation
            redis_inbox.go          # Redis inbox implementation
            dedupe.go               # Deduplication
        storage/                    # Database models
            models.go               # PostgreSQL models
            migrate.go              # Database migrations
            outbox.go               # Outbox pattern
        handlers/                   # Event handlers
            registry.go             # Handler registry
            default.go              # Default handlers
        security/                   # Security components
            mtls.go                 # mTLS implementation
            jwt.go                  # JWT authentication
            acl.go                  # Access control lists
        observability/              # Observability
            metrics.go              # Prometheus metrics
            tracing.go              # OpenTelemetry tracing
            logging.go              # Structured logging
api/proto/mesh/v1/                  # Protobuf services
    mesh.proto                      # MeshService, MeshStatusService, MeshDataService
api/proto/common/v1/                # Common protobuf definitions
    common.proto                    # Common types and enums
api/proto/supervisor/v1/            # Supervisor protobuf definitions
    supervisor.proto                # Supervisor service definitions
```


## Shared Package Integration

### Service Framework (`/pkg/service`)
The mesh service implements the standard service interface:
```go
type MeshService struct {
    engine *Engine
    config *config.Config
    logger *logger.Logger
}

func (s *MeshService) Initialize(ctx context.Context, cfg *config.Config) error
func (s *MeshService) Start(ctx context.Context) error
func (s *MeshService) Stop(ctx context.Context, gracePeriod time.Duration) error
func (s *MeshService) GetCapabilities() *supervisorv1.ServiceCapabilities
func (s *MeshService) CollectMetrics() map[string]int64
func (s *MeshService) HealthChecks() map[string]health.CheckFunc
```

### Database Integration (`/pkg/database`)
- PostgreSQL for persistent application state via `database.PostgreSQL`
- Redis for queues and caching via `database.Redis`
- Automatic connection management and health checks

### Configuration Management (`/pkg/config`)
- Centralized configuration via `config.Config`
- Hot-reload support for non-critical settings
- Restart-required detection for critical changes

### Structured Logging (`/pkg/logger`)
- Consistent logging format across all microservices
- Structured fields for correlation
- Supervisor streaming integration

### Health Checks (`/pkg/health`)
- Standardized health check interface
- Automatic health monitoring and reporting
- Integration with supervisor health system

### Encryption (`/pkg/encryption`)
- Tenant-specific encryption via `TenantEncryptionManager`
- Automatic key management via keyring
- Secure message encryption for client data

### gRPC Utilities (`/pkg/grpc`)
- Standardized gRPC client creation
- Connection pooling and keepalive
- Consistent error handling

### Secure Storage (`/pkg/keyring`)
- Secure storage for mesh/node keys
- Integration with system keyring
- Automatic key rotation support


## Storage Architecture

### Data Storage Separation

#### PostgreSQL (Application Data via `/pkg/database`)
Stores all application-level persistent state:
- Mesh configuration and metadata
- Node information and status
- Link state and metrics
- Stream metadata and offsets
- Topology snapshots
- Message delivery logs
- Outbox/inbox patterns for reliable delivery

#### BoltDB (Raft Consensus Data)
Stores Raft-specific consensus data:
- Raft log entries
- Raft stable state (term, vote, commit index)
- Raft snapshots (compacted state)
- Managed entirely by HashiCorp Raft library

#### Redis (Ephemeral and Queue Data via `/pkg/database`)
Stores temporary and performance-critical data:
- Message queues (outbox/inbox hot paths)
- Deduplication cache
- Rate limiting and credit tokens
- Path metrics cache
- Session state


## Process Model & Concurrency

- One goroutine per WebSocket peer for read loop; write loop per peer with buffered channel.
- Router worker pool for forwarding (bounded).
- Stream workers per active stream (coalesced by tenant or link to cap concurrency).
- Topology probes periodic; background Raft ticks; gossip timers; retry timers.
- Backpressure via credit-based flow control at stream level + per-link send queues with HWM/LWM.


## Networking & Transport

### WebSocket Transport (Initial Implementation)

#### Basic WebSocket Architecture
- **Single Connection Per Peer**: One WebSocket connection between any two connected nodes
- **Bidirectional Communication**: Full-duplex messaging over single connection
- **Message Framing**: Internal header with stream_id, msg_type, tenant_id, seq, ack, path_id, hop_count, ttl, priority
- **Connection Management**: Automatic reconnection with exponential backoff
- **Security**: Secure via wss:// with mTLS where possible; fallback JWT (short-lived) with periodic re-auth

### WebSocket Transport with VirtualLink Architecture (Future Enhancement)

#### VirtualLink Abstraction
- **VirtualLink**: Logical connection between two nodes managing K physical WebSocket connections ("lanes")
- **Lane Management**: Each lane has attributes: class (control, priority, bulk), priority, weight
- **VirtualLink Interface**: 
  - `OpenStream(params)` - Create new stream on appropriate lane
  - `Send(chunk, lane|policy)` - Send data chunk to specific lane or based on policy
  - `Stats()` - Return per-lane statistics and health
  - `Backpressure()` - Return current backpressure state per lane

#### Baseline Lane Configuration
- **Lane 0 — Control**: Small frames only (gossip, Raft, acks, window updates). Strict size cap per send.
- **Lane 1 — Priority Data**: System/internal DB updates (ordered, small-to-medium).
- **Lane 2+ — Bulk Data**: Client-data replication (big payloads, lower priority).
- **Collapsible Mode**: Flag to collapse to 1 lane for testing or constrained environments.

#### Stream Placement & Ordering
- **Default**: Pin stream to single lane to avoid cross-connection reordering
- **Multi-lane Striping**: Only for very large streams that can tolerate additional reassembly latency
- **Control Protection**: Control/system/DB streams never striped across lanes
- **Reassembly**: Sequence at stream layer and enlarge reorder buffers when striping enabled

#### Backpressure & Fairness
- **Per-lane Credit Windows**: Bytes/messages limits per lane
- **Layered Quotas**: Per-tenant/stream quotas on top of lane limits
- **Critical Stream Protection**: If bulk lane saturates, critical streams stay on their designated lanes

#### Adaptive Lane Management
- **Scale-up**: If bulk lane send buffer stays high, RTT inflates, and throughput < target for X seconds, open another bulk lane (up to cap, e.g., 4)
- **Scale-down**: If idle and queues empty for Y seconds, close extra bulk lanes
- **Cool-down**: Between expansions to avoid connection storms
- **Per-peer Cap**: Configurable connection limit for NAT/firewall safety

#### Failure Handling
- **Health Monitoring**: Health-check each lane (heartbeats, application acks)
- **Lane Failure**: Reroute streams to remaining lanes (respect class rules)
- **Control Lane Recovery**: If control lane dies, immediately promote data lane to control (priority flip), spin up new data lane if needed

### Connection Management
- **Outbound-only Dialer**: Every node dials configured peers (URIs) it can reach
- **Reverse Connections**: For unreachable peers, reverse-accept connections: if A can dial B, B marks A as reachable and forms logical bidirectional link
- **Heartbeats**: Periodic heartbeat frames (keepalive) & link probes (latency/bandwidth sampling)

### NAT/Relay

- Mesh supports relay forwarding: nodes can act as transit if they have links that shorten path cost. No centralized TURN; relaying is part of routing.


## Mesh Bootstrap and Discovery

### Initial Mesh Creation (Seeding)
1. **First Node Initialization**:
   - User calls `SeedMesh` via gRPC to create initial mesh identity
   - Generates mesh_id (ULID) and mesh encryption keys
   - Initializes mesh metadata (name, description, join policy)
   - Starts WebSocket listener on configured port
   - Sets mesh state to ACTIVE, node state to ONLINE
   - Node becomes the first member of MCG (Mesh Control Group)

2. **Join Policies**:
   - `OPEN`: Any node can join without authentication
   - `KEY_REQUIRED`: Nodes must provide correct join_key
   - `CLOSED`: No new nodes accepted (existing members only)

### Node Join Process
1. **New Node Joining**:
   - User calls `JoinMesh` via gRPC with target node address (host:port)
   - Establishes WebSocket connection to existing mesh node
   - Performs handshake:
     - Sends join request with node metadata and optional join_key
     - Receives mesh configuration and current member list
     - Exchanges node capabilities and encryption keys
   - Upon successful join:
     - New node becomes equal member of mesh
     - Joins MCG for consensus participation
     - Begins gossip protocol participation
     - Establishes connections to other reachable nodes

2. **Post-Join Behavior**:
   - All nodes are equal (no permanent "seed" node concept)
   - New connections between existing members use simplified handshake
   - Nodes maintain peer list in PostgreSQL for reconnection after restart

### Recovery and Reconnection
1. **Node Restart Recovery**:
   - Load mesh configuration from PostgreSQL
   - Attempt reconnection to previously connected peers
   - If successful, resume normal operation
   - If isolated, wait for incoming connections

2. **Mesh State Management**:
   - `STATUS_HEALTHY`: All known nodes reachable
   - `STATUS_DEGRADED`: Some nodes unreachable (< 50%)
   - `STATUS_CRITICAL`: Majority of nodes unreachable (≥ 50%)
   
3. **Consensus During Degradation**:
   - When mesh is CRITICAL (≥ 50% unreachable):
     - Block all configuration changes except node eviction
     - Maintain read-only operation for existing streams
     - Allow eviction to restore quorum
   - Evicted nodes cannot rejoin without explicit `JoinMesh`

### Node Shutdown Process
1. **Node Shutdown Process**:
    - Supervisor sends a shutdown notification the service over gRPC (StopService(StopServiceRequest) -> StopServiceResponse)
    - Mesh service notifies all other mesh nodes that it will shutdown
    - Mesh service sets node and link status to offline in the internal database

### Future Discovery Mechanisms (Optional)
- DNS-based discovery with SRV records
- Multicast/broadcast for local network discovery
- External service registry integration (Consul, etcd)
- Cloud provider metadata service integration


## Gossip (Membership & Health)

### Protocol

- SWIM-like failure detection (random probing + indirect probing) with Serf-compatible message types: join, alive, suspect, confirm, leave, user-event.
- Disseminate NodeMeta and LinkMeta (capabilities, current load, reported bandwidth, software version).

### Membership DB

- Store in PostgreSQL via `/pkg/database`: nodes, links, incarnation, status, capabilities.
- Redis cache mirrors hot set for quick lookup.


## Topology & Routing

### Link-State Advertisements (LSA)

- Each node emits LSAs for:
    - Adjacencies (who I'm directly connected to),
    - Measured latency, available bandwidth, loss, utilization,
    - Administrative weights (policy), and capacity hints.
- LSAs are flooded over the mesh (gossip channel or control channel) and versioned.

### Path Computation

- Maintain link-state DB. Compute k-shortest paths (e.g., Yen's algorithm on top of Dijkstra).
- Path cost function (lower is better):

```
cost = α * normalized_latency
     + β * (1 / normalized_bandwidth)
     + γ * utilization
     + δ * loss
     + ε * hop_count
```

Coefficients are config-driven and adjustable per traffic class.

### Multipath Load Balancing

- For each stream, choose N best disjoint/partially disjoint paths.
- Per-message chunk scheduler uses Weighted Round Robin where weight ∝ (1 / cost).
- Out-of-order mitigation: sequence across chunks; reassembly at receiver ensures ordered delivery to the application layer (per stream).

### Fast Reroute

- On link failure or SLA breach, incremental recompute; in-flight chunks redirect to next viable path. Maintain per-path send credit limits to prevent head-of-line blocking.


## Consensus

### Raft Engine (Using HashiCorp Raft with BoltDB)
- HashiCorp Raft library with BoltDB for log and stable storage
- Transport for Raft messages uses the same WS overlay (separate control channel)
- Dynamic groups:
    - **Mesh Control Group (MCG)**: All reachable nodes (or an elected quorum subset) participate; manages:
        - Membership authoritative view
        - LSA version index & digests
        - Global system/internal DB update logs (commit index)
        - Mesh configuration changes
    - **Data Stream Groups (DSG)**: Per-relationship / per-tenant stream group including the source and all targets (can be small, 2–10 nodes). Manages:
        - Stream metadata, committed offsets, ack sets, producer epoch
        - Ensures all members process writes before considering complete
- Leaderless startup: any node can bootstrap; Raft elects a leader automatically; no static seeds needed

### Consensus Boundaries

#### Mesh-Level Consensus (MCG)
- **Required for**:
  - Node membership changes (join/leave/evict)
  - Mesh configuration updates
  - System-wide parameter changes
  - Global routing policy updates
- **Blocking conditions**: When mesh is CRITICAL (≥50% nodes unreachable), only eviction operations allowed

#### Data-Level Consensus (DSG)
- **Required for**:
  - Stream creation/deletion
  - Write completion acknowledgment (all DSG members must complete write)
  - Stream offset advancement
  - Checkpoint creation
- **Write semantics**: Write operations considered complete only when all DSG members successfully process

### Applying Consensus

- Mesh-level: a system or internal-DB update is appended to MCG log; committed entry triggers guaranteed fanout to every node (with durable outbox & retry).
- Client-data: source appends data events to its DSG log; an entry is committed when the Raft quorum of that DSG acknowledges processing (not just receipt). "Processing" is defined by the target's handler returning success.

### Data Semantics (Ordering, Exactly-Once, Idempotency)

- Per-stream total order: monotonically increasing seq generated by the source (ULID logical time + counter) and validated by DSG commit order.
- Chunking: large payloads segmented with chunk_seq and stream_seq.
- Exactly-once delivery:
    - Targets maintain stream_offsets in PostgreSQL via `/pkg/database`; only commit offset when the handler completes and transaction commits.
    - Dedupe: Redis inbox_dedupe:{stream_id} stores recently seen message IDs; PostgreSQL delivery_log is authoritative for longer horizon.
- Idempotent handlers required: include message_id in any side effects (DB UPSERT keyed by (stream_id, message_id)).


## Persistence Model

### PostgreSQL Tables (Application Data via `/pkg/database`)

Tables with indexing strategy:

```sql
-- Core mesh topology
CREATE TABLE mesh(
    mesh_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    allow_join ENUM('OPEN', 'KEY_REQUIRED', 'CLOSED') DEFAULT 'KEY_REQUIRED',
    join_key_hash VARCHAR(255), -- bcrypt hash of join key
    status ENUM('ACTIVE', 'DEGRADED', 'CRITICAL') DEFAULT 'ACTIVE',
    created TIMESTAMP DEFAULT NOW(),
    updated TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_mesh_status ON mesh(status);

CREATE TABLE nodes(
    node_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    pubkey TEXT NOT NULL,
    last_seen TIMESTAMP,
    status ENUM('ONLINE', 'SUSPECT', 'OFFLINE') DEFAULT 'ONLINE',
    incarnation BIGINT DEFAULT 0,
    meta JSONB,
    platform VARCHAR(50),
    version VARCHAR(50),
    region_id VARCHAR(50),
    ip_address INET,
    port INTEGER,
    created TIMESTAMP DEFAULT NOW(),
    updated TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_nodes_status ON nodes(status);
CREATE INDEX idx_nodes_last_seen ON nodes(last_seen);

CREATE TABLE links(
    id UUID PRIMARY KEY,
    a_node UUID REFERENCES nodes(node_id),
    b_node UUID REFERENCES nodes(node_id),
    latency_ms FLOAT,
    bandwidth_mbps FLOAT,
    loss FLOAT,
    utilization FLOAT,
    status ENUM('UP', 'DOWN', 'DEGRADED') DEFAULT 'UP',
    meta JSONB,
    updated TIMESTAMP DEFAULT NOW(),
    UNIQUE(a_node, b_node)
);
CREATE INDEX idx_links_nodes ON links(a_node, b_node);
CREATE INDEX idx_links_status ON links(status);

CREATE TABLE lsa_versions(
    node_id UUID REFERENCES nodes(node_id),
    version BIGINT,
    hash VARCHAR(64),
    created TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(node_id, version)
);

-- Stream management (partitioned by month for large deployments)
CREATE TABLE streams(
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    src_node UUID REFERENCES nodes(node_id),
    dst_nodes UUID[] NOT NULL,
    qos ENUM('SYSTEM', 'PRIORITY', 'BULK') DEFAULT 'BULK',
    priority INTEGER DEFAULT 0,
    meta JSONB,
    created TIMESTAMP DEFAULT NOW()
) PARTITION BY RANGE (created);

CREATE TABLE stream_offsets(
    stream_id UUID REFERENCES streams(id),
    node_id UUID REFERENCES nodes(node_id),
    committed_seq BIGINT NOT NULL,
    updated TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(stream_id, node_id)
);
CREATE INDEX idx_stream_offsets_updated ON stream_offsets(updated);

-- Delivery log (partitioned by month, auto-archive after 90 days)
CREATE TABLE delivery_log(
    stream_id UUID,
    message_id VARCHAR(26), -- ULID
    src_node UUID REFERENCES nodes(node_id),
    dst_node UUID REFERENCES nodes(node_id),
    state ENUM('received', 'processing', 'done', 'failed') DEFAULT 'received',
    error_message TEXT,
    updated TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY(stream_id, message_id, dst_node)
) PARTITION BY RANGE (updated);
CREATE INDEX idx_delivery_log_state ON delivery_log(state);

-- Message queuing (outbox pattern)
CREATE TABLE outbox(
    id SERIAL PRIMARY KEY,
    stream_id UUID NOT NULL,
    message_id VARCHAR(26) NOT NULL,
    payload BYTEA,
    headers JSONB,
    next_attempt TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    status ENUM('pending', 'sent', 'failed') DEFAULT 'pending',
    created TIMESTAMP DEFAULT NOW(),
    updated TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_outbox_next_attempt ON outbox(next_attempt) WHERE status = 'pending';
CREATE INDEX idx_outbox_stream_status ON outbox(stream_id, status);

CREATE TABLE inbox(
    stream_id UUID NOT NULL,
    message_id VARCHAR(26) NOT NULL,
    payload BYTEA,
    headers JSONB,
    received TIMESTAMP DEFAULT NOW(),
    processed TIMESTAMP,
    PRIMARY KEY(stream_id, message_id)
);
CREATE INDEX idx_inbox_processed ON inbox(processed) WHERE processed IS NULL;

-- Topology and configuration
CREATE TABLE topology_snapshots(
    version BIGINT PRIMARY KEY,
    graph JSONB NOT NULL,
    created TIMESTAMP DEFAULT NOW()
);

CREATE TABLE config_kv(
    key TEXT PRIMARY KEY,
    value JSONB,
    updated TIMESTAMP DEFAULT NOW()
);

-- Partitioning for large tables
CREATE TABLE streams_2025_01 PARTITION OF streams FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE delivery_log_2025_01 PARTITION OF delivery_log FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
-- Add monthly partitions as needed
```

### BoltDB Storage (Raft Consensus Data)
Managed entirely by HashiCorp Raft library:
- `raft.db`: Contains log entries, stable state, and snapshots
- Location: `/var/lib/mesh/raft/` or configured data directory
- Automatic compaction and snapshot management

### Database Initialization

Tables are created by the supervisor service during the initialization of the application.

Schema is stored in two places:
- Supervisor initialization process `/cmd/supervisor/internal/initialize/schema.go` (used by the application to setup the database)
- Database SQL schema file `/scripts/DATABASE_SCHEMA.sql` (only used for documentation)


## Redis Usage (via `/pkg/database`)

- Reliable queues for outbox/inbox hot paths:
    - queue:egress:{peer} – per-peer send queue (prioritized).
    - queue:stream:{stream_id} – per-stream ordered queue.
- Dedupe & idempotency: inbox_dedupe:{stream_id} (TTL).
- Rate/credit tokens: credits:{link_id} for backpressure.
- Ephemeral path metrics cache to avoid DB contention.


## gRPC APIs (northbound to other microservices & supervisor)

Proto services in api/proto/mesh/v1/:

### MeshService
- SeedMesh -> MeshStatus
    - Initialize a new mesh with mesh_id and encryption keys
    - Start WebSocket listener for incoming connections
    - Only used once per mesh creation
- JoinMesh -> MeshStatus
    - Join existing mesh by connecting to node at specified address:port
    - Exchange mesh configuration and node metadata
    - Establish node as equal member of mesh
- StartMesh -> MeshStatus
    - When node is already member, reconnect to known peers
    - Resume operations after restart
- StopMesh -> MeshStatus
    - Graceful shutdown with notification to other nodes
    - Persist state for later recovery
- LeaveMesh -> SuccessStatus
    - Permanent departure from mesh (requires consensus)
- EvictNode -> MeshStatus
    - Force remove node from mesh (requires consensus)
    - Used when node is unreachable or misbehaving
- AddLink -> TopologyStatus
    - Establish direct connection between nodes
- DropLink -> TopologyStatus
    - Remove direct connection between nodes
- EstablishFullLinks -> TopologyStatus
    - Attempt to create full mesh connectivity
- SendNodeUpdate(NodeUpdate) -> BroadcastResult
    - Node-authorized updates (status, capabilities)
- SendMeshUpdate(MeshUpdate) -> BroadcastResult
    - Mesh-wide updates (requires MCG consensus)
- SendInternalDBUpdate(DBUpdate) -> BroadcastResult
    - Database CDC replication events
- OpenClientDataStream(OpenClientDataStreamReq) -> StreamHandle
- CloseClientDataStream(StreamHandle) -> Empty
- PublishClientData(Chunk) -> PublishResult
    - Chunk headers include (tenant_id, stream_id, seq, chunk_seq, total_chunks, checksum)
- GetMeshStatus(Empty) -> MeshStatus
    - Returns comprehensive mesh state including nodes, links, consensus health
- WatchMeshEvents(Filter) -> stream MeshEvent
    - Real-time notifications of topology changes

### MeshDataService (server-side streams from mesh into app)
- SubscribeNodeUpdates(SubscribeReq) -> stream NodeUpdate
- SubscribeMeshUpdates(SubscribeReq) -> stream MeshUpdate
- SubscribeInternalDBUpdates(SubscribeReq) -> stream DBUpdate
- SubscribeClientData(StreamSelector) -> stream ClientData
Delivery guaranteed in-order per stream.

### Supervisor Integration
- SupervisorControl (mesh implements server):
    - Start(Empty), Stop(Empty), Health(Empty) -> Healthz, Metrics(Empty) -> KVList, Config(Empty) -> ConfigDump, Logs(TailReq) -> stream LogLine
- Mesh ↔ Supervisor Client (mesh calls supervisor):
    - Notify(Event) for critical state changes.
    - Health status updates stored in PostgreSQL for supervisor monitoring

### Notes

- All streaming RPCs use application-level acks: client returns Ack{stream_id, seq}. Mesh only advances commit when acked & persisted.
- Backpressure: server respects client WindowUpdate (credit-based) sent via control frames on the stream.


## Control Plane Workflows

### Node Startup

1. Load config via `/pkg/config`; init PostgreSQL/Redis via `/pkg/database`.
2. Initialize BoltDB for Raft storage.
3. Start gRPC servers via `/pkg/service`; register with Supervisor.
4. Check PostgreSQL for existing mesh membership:
   - If member: attempt reconnection to known peers
   - If not: wait for SeedMesh or JoinMesh command
5. Once connected: start gossip, join/create Raft groups
6. Begin topology probes, emit LSAs, compute k-paths.
7. Drain outbox; resume DSGs from last committed indices.

### New Stream (Client Data)

1. App calls OpenClientDataStream(tenant, targets, qos, priority).
2. Mesh forms/joins DSG for stream participants; Raft leader elected.
3. Assign stream_id, initialize committed_seq.
4. Router precomputes N best paths per target.

### Publish Data

1. App calls PublishClientData(Chunk) (or buffered uploader).
2. Chunk persisted to outbox; append to DSG log.
3. On Raft commit, chunk is eligible to forward.
4. Router schedules chunk across paths; intermediate hops forward.
5. Target node:
    - Validates order & integrity; writes to inbox.
    - Invokes handler (registered in handlers/) for business processing (e.g., writing to local DB).
    - On success, update stream_offsets in PostgreSQL via `/pkg/database`, emit Ack(seq) to DSG (via Raft apply).
6. When DSG quorum marks processed, source advances committed_seq and clears outbox entries.

### System / Internal DB Updates
- Issued via SendSystemUpdate / SendInternalDBUpdate.
- Append to MCG log; Raft commit triggers mesh-wide fanout (router uses best paths); each node persists and applies; final state visible in GetMeshStatus.


## Event Handlers (extensible)

- OnMessageReceived: pre-verify signature, schema, dedupe check.
- OnSystemUpdate: idempotent apply; may write config_kv and trigger reload.
- OnInternalDBUpdate: write to local PostgreSQL via `/pkg/database`; optional transactional fences.
- OnClientData: pluggable processors per tenant/stream; must be idempotent and transactional; return Processed to advance commit.
- OnTopologyChange: notify app via MeshStatus stream; router recompute.
- OnConsensusRoleChange: leader/follower transitions (e.g., start/stop background duties).


## Component Health and Failure Domains

### Component-Level Health
Each component maintains its health state, reported to supervisor:
- **WebSocket Transport**: Connection state, reconnection attempts
- **Raft Consensus**: Leader status, follower lag, snapshot state
- **Router**: Path availability, queue depths
- **Storage**: PostgreSQL connection, BoltDB integrity, Redis availability

### Service-Level Degradation
Mesh service adjusts operational mode based on component health:
- **Read-Only Mode**: When consensus unavailable but connectivity intact
- **Degraded Mode**: When some nodes unreachable but quorum maintained
- **Critical Mode**: When majority unreachable, only eviction allowed

### Failure Handling
- **Redis Failure**: Fall back to PostgreSQL for queuing (with performance impact)
- **PostgreSQL Failure**: Block new operations, maintain in-memory state
- **BoltDB Corruption**: Restore from Raft snapshots or peer sync
- **Network Partition**: Maintain separate MCG per partition, reconcile on heal


## Reliability, Flow Control, and Backpressure

- Credit-based flow control per stream and per link; credits granted based on:
    - Receiver's buffer, CPU load, and Redis/DB lag.
- Retry policy: exponential backoff with jitter; at-least-once at transport, exactly-once at app via dedupe+offsets.
- Pause/Resume: supervisor or API can pause streams or throttle by tenant/priority.
- Snapshotting: Raft snapshot to bound log size; stream compaction using committed_seq.


## Security

- mTLS (node certificates; SPIFFE/SPIRE compatible optionally).
- JWT for app-to-mesh gRPC, short-lived, audience = mesh node ID.
- Message signatures: headers include sig (Ed25519) for end-to-end integrity (optional if mTLS trusted).
- ACLs: per-tenant, per-stream; policy stored in config_kv and enforced on publish/subscribe and forwarding.
- Key rotation: rolling via system update; dual-valid window.
- Tenant encryption via `/pkg/encryption` for client data.


## Observability

- Metrics (Prometheus): link RTT/bw/loss, queue depths, Raft terms/commit lag, stream throughput/latency, drop/retry counts, dedupe hits.
- Tracing (OpenTelemetry): spans for publish→process path across hops; inject baggage (tenant, stream, seq).
- Structured logs via `/pkg/logger`: correlation with stream_id, message_id, raft_group.
- Supervisor integration: continuous health monitoring and centralized logging.


## Configuration

- Global config file + overrides via env via `/pkg/config`:
    - WS endpoints to attempt, TLS options, auth, target k-paths (k), cost coefficients (α…ε),
    - Router limits (per-link credits, per-stream window),
    - DSG sizing policy (max members, snapshot interval),
    - QoS classes (system>db-updates>client-data or configurable).
- Dynamic reconfig via SystemUpdate events.


## Failure & Partition Handling

- Network partitions: Raft prevents split-brain at group level; only leader's log commits.
- Partial delivery: DSG will not advance commit for a message until all required targets process it; optional quorum policy (e.g., 2 of 3) per stream.
- Path flaps: hysteresis on path scoring; degrade gracefully to fewer paths.
- Node churn: gossip marks suspect then dead; LSAs withdrawn; router recompute.
- Reconciliation: upon heal, state sync from Raft snapshots and stream replay from outbox starting at committed_seq + 1.


## Acceptance Criteria (what "done" looks like)

1. Connectivity: Any two reachable nodes exchange messages over WS overlay without inbound ports; multi-hop works.
2. Routing: k-shortest paths computed; traffic spreads across multiple live paths with dynamic reweighting.
3. Delivery:
    - System/Internal DB updates reach all nodes, are durable, idempotent.
    - Client data is delivered in-order, exactly-once to all required targets; retry survives restarts.
4. Consensus: Raft groups for MCG and DSGs elect leaders and commit logs; snapshots reduce log size; partitions don't corrupt state.
5. APIs: All gRPC endpoints implemented; backpressure enforced; Supervisor controls start/stop/health/metrics via `/pkg/service`.
6. Persistence: PostgreSQL tables populated via `/pkg/database`; BoltDB stores Raft state; Redis queues active; restart resumes streams without loss/duplication.
7. Observability: Metrics, logs via `/pkg/logger`, traces give per-stream and per-link visibility; GetMeshStatus returns full mesh & topology.


## Performance Targets (Initial Implementation)

### Without VirtualLink
- Per-node: ≥ 5k msgs/sec sustained small messages
- Latency: p50 < 50ms, p99 < 200ms per hop
- Recovery: link failure reroute < 1s

### With VirtualLink (Future Enhancement)
- Per-node: ≥ 10k msgs/sec sustained small messages; ≥ 1 Gbps aggregate throughput on commodity VM
- Latency budget (intra-continent): p50 < 30ms added per hop, p99 < 120ms end-to-end for small messages
- Recovery: link failure reroute < 500ms; node restart resume < 5s


## Implementation Notes & Choices

- Raft lib: HashiCorp Raft with BoltDB storage backend (battle-tested, production-ready)
- Path computation: Dijkstra + Yen for k-shortest; cache results; incremental recomputation on LSA deltas.
- Chunk checksum: CRC32C for speed; end-to-end optional payload hash (BLAKE3) for critical data.
- Message IDs: ULID for monotonic sortable IDs; embed in headers.
- Serialization: Protobuf for control/data headers; payload opaque bytes.
- Outbox pattern: write → enqueue → send; only delete when acknowledged & committed.
- Handlers: ship with default no-op and DB-applier; tenants can register custom processors.
- Shared packages: leverage `/pkg/service`, `/pkg/database`, `/pkg/logger`, `/pkg/health`, `/pkg/encryption`, `/pkg/config`, `/pkg/grpc` for consistency.


## Directory Skeleton (concise)

```
services/mesh/cmd/main.go
services/mesh/internal/engine/{engine.go, server.go, service.go}
services/mesh/internal/transport/ws/{dialer.go, listener.go, frame.go, auth.go}
services/mesh/internal/transport/ws/{virtuallink.go, lanes.go, mux.go, backpressure.go, health.go} # Future enhancement
services/mesh/internal/gossip/{swim.go, membership.go}
services/mesh/internal/consensus/raft/{node.go, storage.go, transport.go, groups.go}
services/mesh/internal/topology/{lsdb.go, probes.go, ksp.go, scoring.go}
services/mesh/internal/router/{router.go, sched.go, credits.go, reroute.go}
services/mesh/internal/streams/{stream.go, reorder.go, chunker.go}
services/mesh/internal/dataplane/{system.go, clientdata.go, dbupdates.go}
services/mesh/internal/controlplane/{fanout.go, dsg_manager.go}
services/mesh/internal/queue/{redis_outbox.go, redis_inbox.go, dedupe.go}
services/mesh/internal/storage/{models.go, migrate.go, outbox.go}
services/mesh/internal/handlers/{registry.go, default.go}
services/mesh/internal/security/{mtls.go, jwt.go, acl.go}
services/mesh/internal/observability/{metrics.go, tracing.go, logging.go}
api/proto/mesh/v1/{mesh.proto}
```


## Testing & Validation

- Unit tests for router (path selection, weights), streams (ordering), dedupe, Raft apply.
- Chaos tests: kill links/nodes; verify DSG commit & replay behavior.
- Scale test: 50 nodes, random asymmetric topology, 1k concurrent streams.
- Soak: 24h with synthetic load; assert no drift, no duplicate deliveries.