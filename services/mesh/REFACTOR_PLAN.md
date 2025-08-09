# Mesh Service Refactor Plan

## Overview

This document provides a comprehensive plan to implement the mesh service from scratch according to the sophisticated architecture defined in the architecture document. The implementation will create a NAT-friendly, multi-path, consensus-based mesh networking solution with tenant-specific encryption and sophisticated routing.

## Target Architecture

### Core Architecture
- **Architecture**: Link-state mesh with k-shortest path routing and multi-path load balancing
- **Consensus**: Unified Raft (HashiCorp Raft with BoltDB) for both Mesh Control Groups (MCG) and Data Stream Groups (DSG)
- **Routing**: SWIM-style membership with link-state advertisements and sophisticated path computation
- **Transport**: WebSocket initially, with VirtualLink architecture as future enhancement
- **Storage**: 
  - PostgreSQL for application data (streams, topology, message logs)
  - BoltDB for Raft consensus data (managed by HashiCorp Raft)
  - Redis for queues and caching
- **API**: Unified gRPC services (MeshService, MeshDataService) with streaming support

### Storage Architecture Separation

#### PostgreSQL (Application Data)
- Mesh configuration and metadata
- Node information and status
- Link state and topology
- Stream metadata and offsets
- Message delivery logs
- Outbox/inbox patterns

#### BoltDB (Raft Consensus)
- Raft log entries
- Raft stable state (term, vote, commit index)
- Raft snapshots
- Fully managed by HashiCorp Raft library

#### Redis (Ephemeral Data)
- Message queues
- Deduplication cache
- Rate limiting tokens
- Path metrics cache

### Initial WebSocket Transport (Phases 1-7)
- Single WebSocket connection per peer
- Full-duplex bidirectional messaging
- Message framing with headers (stream_id, msg_type, tenant_id, seq, priority)
- Automatic reconnection with exponential backoff
- mTLS or JWT authentication

### VirtualLink Architecture (Phase 8 - Optional Enhancement)

#### Lane-Based Connection Management
- **VirtualLink**: Logical connection between two nodes managing K physical WebSocket connections ("lanes")
- **Lane Classes**: 
  - Lane 0: Control (gossip, Raft, acks, window updates) - strict size caps
  - Lane 1: Priority Data (system/internal DB updates) - ordered, small-to-medium
  - Lane 2+: Bulk Data (client-data replication) - big payloads, lower priority
- **Collapsible Mode**: Flag to collapse to 1 lane for testing or constrained environments

#### Stream Placement Strategy
- **Default**: Pin stream to single lane to avoid cross-connection reordering
- **Multi-lane Striping**: Only for very large streams (CDC replication data)
- **Control Protection**: Control/system/DB streams never striped across lanes

#### Adaptive Lane Management
- **Scale-up**: Open additional bulk lanes based on performance metrics
- **Scale-down**: Close idle lanes after cool-down period
- **Per-peer Cap**: Configurable connection limit for NAT/firewall safety

#### Failure Handling
- **Health Monitoring**: Per-lane heartbeats and application acks
- **Lane Failure**: Reroute streams to remaining lanes
- **Control Lane Recovery**: Promote data lane if control lane fails

## Mesh Bootstrap and Join Process

### Initial Mesh Creation
1. **First Node (Seed Process)**:
   - User calls `SeedMesh` via gRPC
   - Generates mesh_id and encryption keys
   - Initializes mesh metadata with join policy (OPEN/KEY_REQUIRED/CLOSED)
   - Starts WebSocket listener
   - Becomes first MCG member

2. **Subsequent Nodes (Join Process)**:
   - User calls `JoinMesh` with target node address:port
   - Establishes WebSocket connection
   - Exchanges mesh configuration and node metadata
   - Becomes equal member (no permanent "seed" concept)

### Recovery After Reset
- Nodes attempt reconnection to previously connected peers
- Mesh shows STATUS_DEGRADED when nodes missing
- Consensus blocked when ≥50% nodes unreachable (except eviction)

## Consensus Architecture

### Mesh Control Group (MCG)
- All reachable nodes participate
- Manages membership, configuration, system updates
- Uses HashiCorp Raft with BoltDB storage
- Consensus required for:
  - Node membership changes
  - Mesh configuration updates
  - System-wide parameter changes

### Data Stream Groups (DSG)
- Per-stream consensus groups (2-10 nodes)
- Ensures all members process writes before completion
- Manages stream offsets and acknowledgments
- Write considered complete only when all DSG members succeed

### Consensus During Degradation
- STATUS_HEALTHY: All nodes reachable
- STATUS_DEGRADED: <50% unreachable
- STATUS_CRITICAL: ≥50% unreachable (only eviction allowed)

## Go Package Dependencies

### Required External Packages
```go
// Raft consensus
"github.com/hashicorp/raft"
"github.com/hashicorp/raft-boltdb/v2" // BoltDB storage backend

// WebSocket transport  
"github.com/gorilla/websocket"
"nhooyr.io/websocket" // alternative modern WebSocket library

// Routing and topology
"github.com/yourbasic/graph" // for k-shortest path algorithms
"gonum.org/v1/gonum/graph" // graph algorithms

// Cryptography and security
"crypto/ed25519" // for message signatures
"golang.org/x/crypto/nacl/box" // for encryption
"github.com/o1egl/paseto/v2" // for JWT alternatives

// Protocol buffers and gRPC
"google.golang.org/grpc"
"google.golang.org/protobuf"

// Observability
"github.com/prometheus/client_golang"
"go.opentelemetry.io/otel"

// Utilities
"github.com/oklog/ulid/v2" // for message IDs
"github.com/rs/zerolog" // structured logging
```

### Shared Packages from /pkg
- `/pkg/service` - BaseService interface and lifecycle management
- `/pkg/database` - PostgreSQL and Redis clients
- `/pkg/logger` - Structured logging
- `/pkg/health` - Health check framework  
- `/pkg/encryption` - TenantEncryptionManager and keyring
- `/pkg/config` - Configuration management
- `/pkg/grpc` - gRPC utilities

## Component Reusability Analysis

### Components to Keep and Refactor
1. **`internal/engine/`** - Keep pattern but completely rewrite logic
2. **`internal/storage/postgres.go`** - Keep database integration pattern, update models  
3. **Basic WebSocket infrastructure** - Keep connection management patterns

### Components to Remove Completely
1. **`internal/mesh/consensus.go`** - Replace with HashiCorp Raft + BoltDB
2. **`internal/routing/strategies.go`** - Replace with link-state routing
3. **`internal/messages/types.go`** - Replace with protobuf-based messaging
4. **All current gRPC service implementations** - Complete API redesign
5. **Any custom Raft implementations** - Use HashiCorp Raft library

### New Components to Build
1. **`internal/engine/`** - Core business logic with engine/server/service pattern
2. **`internal/transport/ws/`** - WebSocket connections (VirtualLink in Phase 8)
3. **`internal/gossip/`** - SWIM membership protocol
4. **`internal/consensus/raft/`** - HashiCorp Raft integration with BoltDB
5. **`internal/topology/`** - Link-state database and k-shortest paths
6. **`internal/router/`** - Multi-path scheduler with flow control
7. **`internal/streams/`** - Stream management and ordering
8. **`internal/dataplane/`** - Traffic class handlers
9. **`internal/controlplane/`** - Message fanout and DSG management
10. **`internal/queue/`** - Redis-based outbox/inbox pattern
11. **`internal/storage/`** - PostgreSQL models and migrations
12. **`internal/handlers/`** - Extensible event handlers
13. **`internal/observability/`** - Metrics, tracing, logging

### Partitioning Strategy
- **Streams table**: Monthly partitions for scalability
- **Delivery log**: Monthly partitions with 90-day retention
- **Outbox**: Consider partitioning by stream_id hash for very large deployments
- **Auto-partition creation**: Implement monthly partition creation job

### BoltDB Storage
- Location: `/var/lib/mesh/raft/` or configured directory
- Files: `raft.db` for log/state, snapshots in subdirectory
- Managed entirely by HashiCorp Raft library

## Implementation Phase Plan

### Phase 1: Foundation
**Goal**: Establish database schema and basic service structure

**Tasks**:
1. **Database Setup**
   - Create PostgreSQL schema with proper indexes and partitioning
   - Initialize BoltDB directory for Raft
   - Set up Redis connection pools
   - Implement storage interfaces and models

2. **Service Framework**
   - Implement `internal/engine/` with engine/server/service pattern
   - Set up configuration management via `/pkg/config`
   - Implement basic gRPC server structure
   - Integrate with supervisor service

3. **Basic WebSocket Transport**
   - Single WebSocket connection per peer
   - Message framing and serialization
   - Connection management and reconnection
   - Basic authentication (mTLS/JWT)

4. **Mesh Bootstrap**
   - Implement SeedMesh for initial mesh creation
   - Implement JoinMesh for node addition
   - Basic start/stop/leave operations
   - Persist mesh configuration

**Deliverables**:
- Database schema active with proper indexes
- Service starts and accepts gRPC connections
- WebSocket connections established between nodes
- Basic mesh operations functional

### Phase 2: Consensus and Membership
**Goal**: Implement Raft consensus and SWIM membership

**Tasks**:
1. **HashiCorp Raft Integration**
   - Set up Raft with BoltDB backend
   - Implement MCG formation
   - Leader election and log replication
   - Raft transport over WebSocket

2. **SWIM Membership**
   - Failure detection protocol
   - Node join/leave procedures
   - Health monitoring and suspicion spreading
   - Integration with PostgreSQL for state

3. **Consensus Boundaries**
   - MCG for mesh-level decisions
   - DSG formation for streams
   - Consensus during degradation handling

**Deliverables**:
- Nodes form consensus groups successfully
- Membership changes propagated via SWIM
- Raft leader election working
- Consensus blocked appropriately during degradation

### Phase 3: Routing and Path Computation
**Goal**: Implement link-state routing with k-shortest paths

**Tasks**:
1. **Link-State Database**
   - LSA generation and flooding
   - Version management
   - Topology graph construction
   - Store in PostgreSQL with caching in Redis

2. **Path Computation**
   - Dijkstra + Yen's algorithm
   - Configurable cost weights (α, β, γ, δ, ε)
   - Path caching and incremental updates
   - Multi-path selection

3. **Basic Routing**
   - Route table construction
   - Forwarding decision logic
   - Next-hop determination
   - Path monitoring

**Deliverables**:
- Multi-path routing functional
- Dynamic path recomputation on topology changes
- Route optimization based on link metrics
- Path cache working efficiently

### Phase 4: Stream Management and Data Plane
**Goal**: Implement stream-based data delivery with exactly-once semantics

**Tasks**:
1. **Stream Management**
   - Stream creation and lifecycle
   - DSG formation for participants
   - Sequence number management
   - Stream offset tracking in PostgreSQL

2. **Data Plane Handlers**
   - System update handlers
   - Internal DB update handlers (CDC replication)
   - Client data handlers with tenant encryption
   - Handler registry and plugins

3. **Outbox/Inbox Pattern**
   - Redis queue implementation
   - PostgreSQL persistence for durability
   - Retry logic with exponential backoff
   - Deduplication via Redis cache

**Deliverables**:
- Reliable stream-based message delivery
- Exactly-once processing semantics
- Tenant-specific data encryption working
- CDC replication data flowing correctly

### Phase 5: Multi-Path Load Balancing
**Goal**: Implement sophisticated traffic scheduling and flow control

**Tasks**:
1. **Multi-Path Scheduler**
   - Weighted round-robin across paths
   - Chunk-based load balancing
   - Out-of-order mitigation
   - Per-class scheduling (system/DB/client)

2. **Flow Control**
   - Credit-based backpressure
   - Per-stream and per-link credits
   - Dynamic credit allocation
   - Redis-based token buckets

3. **Fast Reroute**
   - Link failure detection
   - Automatic failover to backup paths
   - In-flight message recovery
   - Hysteresis for stability

**Deliverables**:
- Traffic spreads across multiple paths
- Automatic failover working
- Backpressure prevents overload
- Performance meets base targets

### Phase 6: Security and Observability
**Goal**: Complete security implementation and operational readiness

**Tasks**:
1. **Security Hardening**
   - mTLS for node authentication
   - JWT for application authentication
   - ACL enforcement for tenant isolation
   - Message signatures for integrity

2. **Observability**
   - Prometheus metrics collection
   - OpenTelemetry tracing
   - Structured logging with correlation IDs
   - Health check implementation

3. **Supervisor Integration**
   - Complete health reporting
   - Metrics collection
   - State persistence for monitoring
   - Event notification

**Deliverables**:
- Production-ready security posture
- Complete observability stack
- Full supervisor integration
- All health checks functional

### Phase 7: Testing and Optimization
**Goal**: Ensure reliability and initial performance targets

**Tasks**:
1. **Comprehensive Testing**
   - Unit tests for all components
   - Integration tests for end-to-end flows
   - Chaos testing for failure scenarios
   - Initial load testing

2. **Performance Optimization**
   - Profiling and bottleneck identification
   - Query optimization for PostgreSQL
   - Redis usage optimization
   - Network protocol tuning

3. **Documentation**
   - API documentation
   - Operational runbooks
   - Configuration reference
   - Architecture documentation

**Deliverables**:
- Test coverage >80%
- Initial performance targets met (5k msgs/sec)
- Complete documentation
- System stable under load

### Phase 8: VirtualLink Implementation (Optional Enhancement)
**Goal**: Implement advanced VirtualLink architecture for heavy CDC replication workloads

**Tasks**:
1. **VirtualLink Core**
   - Implement VirtualLink abstraction
   - Lane management (control, priority, bulk)
   - Lane class configuration
   - Collapsible mode for testing

2. **Adaptive Lane Management**
   - Performance-based lane scaling
   - Cool-down mechanisms
   - Per-peer connection caps
   - Lane health monitoring

3. **Advanced Features**
   - Multi-lane striping for large streams
   - Per-lane credit windows
   - Lane failure recovery
   - Control lane promotion

4. **Performance Validation**
   - Load testing with CDC replication workloads
   - Achieve 10k msgs/sec target
   - Validate 1 Gbps throughput
   - Latency optimization

**Deliverables**:
- VirtualLink fully functional
- Adaptive scaling working
- Performance targets achieved (10k msgs/sec, 1 Gbps)
- CDC replication optimized

## Deployment Strategy

### Initial Deployment
1. **Deploy new mesh service version**
2. **Initialize first node with SeedMesh**
3. **Join additional nodes with JoinMesh**
4. **Monitor mesh formation and health**
5. **Begin data flow testing**

### Configuration Management
- Global config file + env overrides via `/pkg/config`
- Dynamic reconfig via SystemUpdate events
- Hot-reload for non-critical settings
- Supervisor-managed configuration

## Success Criteria

### Functional Requirements (Phases 1-7)
- [x] Asymmetric connectivity with multi-hop routing
- [x] k-shortest path computation and multi-path load balancing
- [x] Exactly-once delivery semantics for all traffic classes
- [x] Tenant-specific encryption for client data
- [x] Consensus-based consistency for all updates
- [x] NAT traversal without inbound port requirements
- [x] Mesh bootstrap and join process
- [x] Recovery after node failures

### Performance Requirements (Without VirtualLink)
- [x] ≥ 5k msgs/sec sustained throughput per node
- [x] p50 < 50ms added latency per hop
- [x] p99 < 200ms end-to-end latency
- [x] < 1s link failure recovery time

### Performance Requirements (With VirtualLink - Phase 8)
- [x] ≥ 10k msgs/sec sustained throughput per node
- [x] ≥ 1 Gbps aggregate throughput
- [x] p50 < 30ms added latency per hop
- [x] p99 < 120ms end-to-end latency
- [x] < 500ms link failure recovery time
- [x] < 5s node restart recovery time

### Operational Requirements
- [x] Complete observability with metrics, logs, traces
- [x] Automated health checks via supervisor
- [x] Configuration management with hot-reload
- [x] Comprehensive API documentation
- [x] Stable operation under degraded conditions

## Implementation Notes

### Development Approach
- **Incremental Development**: Complete each phase before moving to next
- **Testing First**: Write tests alongside implementation
- **Performance Monitoring**: Track performance from Phase 1
- **Code Reviews**: All changes reviewed before merge

### Quality Assurance
- **Unit Testing**: Minimum 80% coverage
- **Integration Testing**: End-to-end test suites
- **Chaos Testing**: Failure injection testing
- **Load Testing**: Performance validation at each phase

### Critical Decisions
- **Use HashiCorp Raft**: Battle-tested, well-documented
- **BoltDB for Raft**: Standard, reliable storage backend
- **PostgreSQL for app data**: Leverages existing infrastructure
- **Defer VirtualLink**: Reduces initial complexity
- **Supervisor integration**: Ensures operational visibility

## Progress Tracking

Each phase includes specific deliverables and acceptance criteria. Progress should be tracked via:
- Completed tasks per phase
- Test coverage metrics
- Performance benchmarks
- Documentation completeness
- Code review status

The implementation is considered complete when:
1. Phases 1-7 are fully implemented and tested
2. All functional requirements are met
3. Initial performance targets achieved
4. Documentation is complete
5. System is stable in production

Phase 8 (VirtualLink) should be implemented only after:
1. The base system is stable in production
2. CDC replication workloads require additional performance
3. Initial deployment has validated the architecture
4. Team has bandwidth for the additional complexity