# Mesh Microservice Architecture (Updated for Shared Packages)

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
    - No static seed node; all nodes equal, but use Raft for leadership/elections where needed.
- Data Plane:
	- Three traffic classes over gRPC-facing API:
	    1.	System updates (mesh-wide propagation)
	    2.	Tenant client data (one-to-one / one-to-many; critical, ordered)
	    3.	Internal DB updates (one-to-all; ordered)
	- Client-data consensus: prove reception + processing by relevant target nodes; ordered, idempotent, exactly-once semantics at stream level.
    - Encryption:
        - Use tenant-specific encryption keys for all tenant client data (via `/pkg/encryption`)
        - Use mesh/node encryption keys for system and internal DB updates
- Persistence per node: Local PostgreSQL (durable metadata/state) and Redis (queues/ephemeral, dedupe) via `/pkg/database`.
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
            virtuallink.go          # VirtualLink abstraction
            lanes.go                # Lane management and configuration
            mux.go                  # Multiplexing across lanes
            frame.go                # Message framing with lane_id
            auth.go                 # Authentication
            backpressure.go         # Per-lane credit management
            health.go               # Lane health monitoring
        gossip/                     # Membership & health detection
            swim.go                 # SWIM protocol implementation
            membership.go           # Membership management
        consensus/raft/             # Raft consensus engine
            node.go                 # Raft node implementation
            storage.go              # Raft storage interface
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
- PostgreSQL for persistent state via `database.PostgreSQL`
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


## Process Model & Concurrency

- One goroutine per WebSocket peer for read loop; write loop per peer with buffered channel.
- Router worker pool for forwarding (bounded).
- Stream workers per active stream (coalesced by tenant or link to cap concurrency).
- Topology probes periodic; background Raft ticks; gossip timers; retry timers.
- Backpressure via credit-based flow control at stream level + per-link send queues with HWM/LWM.


## Networking & Transport

### WebSocket Transport with VirtualLink Architecture

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

#### Adaptive Lane Management (Optional)
- **Scale-up**: If bulk lane send buffer stays high, RTT inflates, and throughput < target for X seconds, open another bulk lane (up to cap, e.g., 4)
- **Scale-down**: If idle and queues empty for Y seconds, close extra bulk lanes
- **Cool-down**: Between expansions to avoid connection storms
- **Per-peer Cap**: Configurable connection limit for NAT/firewall safety

#### Failure Handling
- **Health Monitoring**: Health-check each lane (heartbeats, application acks)
- **Lane Failure**: Reroute streams to remaining lanes (respect class rules)
- **Control Lane Recovery**: If control lane dies, immediately promote data lane to control (priority flip), spin up new data lane if needed

#### Connection Management
- **Outbound-only Dialer**: Every node dials configured peers (URIs) it can reach
- **Reverse Connections**: For unreachable peers, reverse-accept connections: if A can dial B, B marks A as reachable and forms logical bidirectional link
- **Security**: Secure via wss:// with mTLS where possible; fallback JWT (short-lived) with periodic re-auth
- **Framing**: Internal header with stream_id, msg_type, tenant_id, seq, ack, path_id, hop_count, ttl, priority, lane_id
- **Heartbeats**: Per-lane heartbeat frames (keepalive) & link probes (latency/bandwidth sampling)

### NAT/Relay

- Mesh supports relay forwarding: nodes can act as transit if they have links that shorten path cost. No centralized TURN; relaying is part of routing.


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

### Raft Engine
- Shared Raft implementation for control-plane and data-plane groups.
- Transport for Raft messages uses the same WS overlay (separate control channel).
- Dynamic groups:
    - Mesh Control Group (MCG): all reachable nodes (or an elected quorum subset) participate; stores:
        - Membership authoritative view,
        - LSA version index & digests,
        - Global system/internal DB update logs (commit index).
    - Data Stream Groups (DSG): per-relationship / per-tenant stream group including the source and all targets (can be small, 2–10 nodes). Stores:
        - Stream metadata, committed offsets, ack sets, producer epoch.
- Leaderless startup: any node can bootstrap; Raft elects a leader automatically; no static seeds needed.

### Applying Consensus

- Mesh-level: a system or internal-DB update is appended to MCG log; committed entry triggers guaranteed fanout to every node (with durable outbox & retry).
- Client-data: source appends data events to its DSG log; an entry is committed when the Raft quorum of that DSG acknowledges processing (not just receipt). "Processing" is defined by the target's handler returning success (see §10).

### Data Semantics (Ordering, Exactly-Once, Idempotency)

- Per-stream total order: monotonically increasing seq generated by the source (ULID logical time + counter) and validated by DSG commit order.
- Chunking: large payloads segmented with chunk_seq and stream_seq.
- Exactly-once delivery:
    - Targets maintain stream_offsets in PostgreSQL via `/pkg/database`; only commit offset when the handler completes and transaction commits.
    - Dedupe: Redis inbox_dedupe:{stream_id} stores recently seen message IDs; PostgreSQL delivery_log is authoritative for longer horizon.
- Idempotent handlers required: include message_id in any side effects (DB UPSERT keyed by (stream_id, message_id)).


## Persistence Model (PostgreSQL via `/pkg/database`)

Tables (essential, minimal columns implied):
- mesh(mesh_id, name, desctiption, allow_join, status, created, updated)
- nodes(node_id, name, description, pubkey, last_seen, status, incarnation, meta jsonb, platform, version, region_id, ip_address, port, created, updated)
- links(id, a_node, b_node, latency_ms, bandwidth_mbps, loss, utilization, status, meta jsonb)
- lsa_versions(node_id, version, hash, created)
- raft_groups(id, type enum{MCG, DSG}, members[], term, leader_id, meta jsonb)
- raft_log(group_id, index, term, payload bytea, created)
- streams(id, tenant_id, src_node, dst_nodes[], qos enum, priority, meta jsonb)
- stream_offsets(stream_id, node_id, committed_seq, updated)
- delivery_log(stream_id, message_id, src_node, dst_node, state enum{received, processing, done, failed}, err, updated)
- outbox(stream_id, message_id, payload, headers jsonb, next_attempt, attempts, status, created, updated)
- inbox(stream_id, message_id, payload, headers jsonb, received, processed)
- topology_snapshots(version, graph jsonb, created)
- config_kv(key text primary key, value jsonb, updated)

Tables are created by the supervisor service during the initialization of the application.


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
    - Seed a new mesh (only used to initialize the mesh)
- JoinMesh -> MeshStatus
    - Join an existing mesh by connecting to another node (only used once per node to join a mesh and get all initial details)
- StartMesh -> MeshStatus
    - When the node is already a member of a mesh, start mesh is used when the application is starting
- StopMesh -> MeshStatus
    - Used for shutting down the node and telling all other nodes that the node is going down
- LeaveMesh -> SuccessStatus
    - For a node to leave the mesh, consensus is required
- EvictNode -> MeshStatus
    - Evicting a node requires consensus from the mesh
- AddLink -> TopologyStatus
    - Adds a link between nodes
- DropLink -> TopologyStatus
    - Drop a link between nodes
- EstablishFullLinks -> TopologyStatus
    - Make all nodes try to establish all possible links with each other
- SendNodeUpdate(NodeUpdate) -> BroadcastResult
    - Node updates are authorized by the node
- SendMeshUpdate(MeshUpdate) -> BroadcastResult
    - Mesh updates require consensus from the mesh to be applied
- SendInternalDBUpdate(DBUpdate) -> BroadcastResult
    - Internal DB updates will need to be cached any any node is missing from the mesh
- OpenClientDataStream(OpenClientDataStreamReq) -> StreamHandle
- CloseClientDataStream(StreamHandle) -> Empty
- PublishClientData(Chunk) -> PublishResult
    - Chunk headers include (tenant_id, stream_id, seq, chunk_seq, total_chunks, checksum)
- GetMeshStatus(Empty) -> MeshStatus
    Includes: node list & state, link metrics, LSDB version, Raft group health.
- WatchMeshEvents(Filter) -> stream MeshEvent
    (node join/leave, link up/down, role changes, path changes)

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

### Notes

- All streaming RPCs use application-level acks: client returns Ack{stream_id, seq}. Mesh only advances commit when acked & persisted.
- Backpressure: server respects client WindowUpdate (credit-based) sent via control frames on the stream.


## Control Plane Workflows

### Node Startup

1. Load config via `/pkg/config`; init PostgreSQL/Redis via `/pkg/database`.
2. Start gRPC servers via `/pkg/service`; register with Supervisor.
3. Dial configured peers (WS). Start reader/writer loops.
4. Start gossip (advertise alive), receive membership.
5. Join or create Raft MCG; elect leader.
6. Begin topology probes, emit LSAs; compute k-paths.
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
6. Persistence: PostgreSQL tables populated via `/pkg/database`; Redis queues active; restart resumes streams without loss/duplication.
7. Observability: Metrics, logs via `/pkg/logger`, traces give per-stream and per-link visibility; GetMeshStatus returns full mesh & topology.


## Performance Targets (tunable)

- Per-node: ≥ 10k msgs/sec sustained small messages; ≥ 1 Gbps aggregate throughput on commodity VM.
- Latency budget (intra-continent): p50 < 30ms added per hop, p99 < 120ms end-to-end for small messages.
- Recovery: link failure reroute < 500ms; node restart resume < 5s.


## Implementation Notes & Choices

- Raft lib: use a mature Go Raft implementation (pluggable storage & network); wrap transport over WS control channel.
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
services/mesh/internal/transport/ws/{dialer.go, virtuallink.go, lanes.go, mux.go, frame.go, auth.go, backpressure.go, health.go}
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