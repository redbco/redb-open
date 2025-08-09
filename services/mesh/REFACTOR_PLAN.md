# Mesh Service Refactor Plan

## Overview

This document provides a comprehensive plan to implement the mesh service from scratch according to the sophisticated architecture defined in MESH_REFACTOR_TARGET_STATE.md. The implementation will create a NAT-friendly, multi-path, consensus-based mesh networking solution with tenant-specific encryption and sophisticated routing.

## Target Architecture

### Core Architecture
- **Architecture**: Link-state mesh with k-shortest path routing and multi-path load balancing
- **Consensus**: Unified Raft for both Mesh Control Groups (MCG) and Data Stream Groups (DSG)
- **Routing**: SWIM-style membership with link-state advertisements and sophisticated path computation
- **Transport**: WebSocket with VirtualLink architecture (lane-based connections), tenant encryption, and flow control
- **Database**: Comprehensive schema with streams, topology, consensus logs
- **API**: Unified gRPC services (MeshService, MeshDataService) with streaming support

### VirtualLink Architecture

#### Lane-Based Connection Management
- **VirtualLink**: Logical connection between two nodes managing K physical WebSocket connections ("lanes")
- **Lane Classes**: 
  - Lane 0: Control (gossip, Raft, acks, window updates) - strict size caps
  - Lane 1: Priority Data (system/internal DB updates) - ordered, small-to-medium
  - Lane 2+: Bulk Data (client-data replication) - big payloads, lower priority
- **Collapsible Mode**: Flag to collapse to 1 lane for testing or constrained environments
- **WebSocket**: Underlaying WebSocket to support multiplexing

#### Stream Placement Strategy
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

## Raft Consensus Storage Architecture

### PostgreSQL-Based Raft Implementation
The mesh service uses a **custom PostgreSQL-based Raft implementation** instead of bolt-db:

#### Storage Components
- **LogStore**: `stores/postgres_log_store.go` - Stores Raft log entries in PostgreSQL
- **StableStore**: `stores/postgres_stable_store.go` - Stores persistent state in PostgreSQL  
- **SnapshotStore**: `stores/redis_snapshot_store.go` - Uses Redis for snapshots (performance)

#### Advantages of PostgreSQL Approach
- **Unified Database**: All mesh data in one PostgreSQL instance
- **ACID Transactions**: Full transactional consistency
- **Existing Infrastructure**: Leverages existing `/pkg/database` package
- **Operational Simplicity**: Single database to manage and backup
- **Performance**: PostgreSQL is highly optimized for concurrent access

#### Database Schema Requirements

### New Tables Required

```sql
-- Core mesh topology
mesh(mesh_id, name, desctiption, allow_join, status, created, updated)
nodes(node_id, name, description, pubkey, last_seen, status, incarnation, meta jsonb, platform, version, region_id, ip_address, port, created, updated)
links(id, a_node, b_node, latency_ms, bandwidth_mbps, loss, utilization, status, meta jsonb) 
lsa_versions(node_id, version, hash, created)

-- Raft consensus system
raft_groups(id, type enum{MCG, DSG}, members[], term, leader_id, meta jsonb)
raft_log(group_id, index, term, payload bytea, created)

-- Stream management
streams(id, tenant_id, src_node, dst_nodes[], qos enum, priority, meta jsonb)
stream_offsets(stream_id, node_id, committed_seq, updated) 
delivery_log(stream_id, message_id, src_node, dst_node, state enum{received, processing, done, failed}, err, updated)

-- Message queuing (outbox pattern)
outbox(stream_id, message_id, payload, headers jsonb, next_attempt, attempts, status, created, updated)
inbox(stream_id, message_id, payload, headers jsonb, received, processed)

-- Topology and routing
topology_snapshots(version, graph jsonb, created)
config_kv(key text primary key, value jsonb, updated)
```

## gRPC API Definitions

### Unified API Structure

```protobuf
// MeshService - Control plane operations and status
service MeshService {
  // Mesh lifecycle
  rpc SeedMesh(SeedMeshReq) returns (MeshStatus)
  rpc JoinMesh(JoinMeshReq) returns (MeshStatus)
  rpc StartMesh(StartMeshReq) returns (MeshStatus)
  rpc StopMesh(StopMeshReq) returns (MeshStatus)
  rpc LeaveMesh(LeaveMeshReq) returns (SuccessStatus)
  rpc EvictNode(EvictNodeReq) returns (MeshStatus)
  
  // Topology management
  rpc AddRoute(AddRouteReq) returns (TopologyStatus)
  rpc DropRoute(DropRouteReq) returns (TopologyStatus)
  
  // Data publishing
  rpc SendNodeUpdate(NodeUpdate) returns (BroadcastResult)
  rpc SendMeshUpdate(MeshUpdate) returns (BroadcastResult)
  rpc SendInternalDBUpdate(DBUpdate) returns (BroadcastResult)
  
  // Client data streams
  rpc OpenClientDataStream(OpenClientDataStreamReq) returns (StreamHandle)
  rpc CloseClientDataStream(StreamHandle) returns (Empty)
  rpc PublishClientData(Chunk) returns (PublishResult)
  
  // Status and monitoring
  rpc GetMeshStatus(Empty) returns (MeshStatus)
  rpc WatchMeshEvents(Filter) returns (stream MeshEvent)
}

// MeshDataService - Data plane subscriptions (server-side streams)
service MeshDataService {
  rpc SubscribeNodeUpdates(SubscribeReq) returns (stream NodeUpdate)
  rpc SubscribeMeshUpdates(SubscribeReq) returns (stream MeshUpdate)
  rpc SubscribeInternalDBUpdates(SubscribeReq) returns (stream DBUpdate)
  rpc SubscribeClientData(StreamSelector) returns (stream ClientData)
}
```

### New Message Types
- `NodeUpdate`, `MeshUpdate`, `DBUpdate`, `ClientData` with encryption headers
- `Chunk` with stream_id, seq, chunk_seq, tenant_id, checksum
- `MeshStatus` with node lists, link metrics, LSDB version, Raft health
- `MeshEvent` for topology changes, node join/leave events
- `StreamHandle`, `StreamSelector` for stream management
- `BroadcastResult`, `PublishResult` for operation results

## Go Package Dependencies

### Required External Packages
```go
// Raft consensus
"github.com/hashicorp/raft"
// Note: Using custom PostgreSQL transport (already implemented in stores/)

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
1. **`internal/mesh/consensus.go`** - Replace with unified Raft implementation
2. **`internal/routing/strategies.go`** - Replace with link-state routing
3. **`internal/messages/types.go`** - Replace with protobuf-based messaging
4. **`internal/grpc/consensus_service.go`** - Replace with new API structure
5. **All current gRPC service implementations** - Complete API redesign

### New Components to Build
1. **`internal/engine/`** - Core business logic with engine/server/service pattern
2. **`internal/transport/ws/`** - VirtualLink architecture with lane-based WebSocket connections
   - `virtuallink.go` - VirtualLink abstraction and management
   - `lanes.go` - Lane configuration and class management
   - `backpressure.go` - Per-lane credit management
   - `health.go` - Lane health monitoring
   - `mux.go` - Multiplexing across lanes
   - `frame.go` - Message framing with lane_id
3. **`internal/gossip/`** - SWIM membership protocol
4. **`internal/consensus/raft/`** - Unified Raft implementation for MCG/DSG
   - **Note**: Already has PostgreSQL-based LogStore and StableStore implementations
   - Uses existing `stores/postgres_log_store.go` and `stores/postgres_stable_store.go`
   - Redis-based snapshot store for performance
5. **`internal/topology/`** - Link-state database and k-shortest paths
6. **`internal/router/`** - Multi-path scheduler with flow control
7. **`internal/streams/`** - Stream management and ordering
8. **`internal/dataplane/`** - Traffic class handlers (system, client, DB updates)
9. **`internal/controlplane/`** - Message fanout and DSG management
10. **`internal/queue/`** - Redis-based outbox/inbox pattern
11. **`internal/storage/`** - PostgreSQL models and migrations
12. **`internal/handlers/`** - Extensible event handlers
13. **`internal/security/`** - mTLS, JWT, ACL implementation
14. **`internal/observability/`** - Metrics, tracing, logging

## Implementation Phase Plan

### Phase 1: Foundation
**Goal**: Establish database schema and basic service structure

**Tasks**:
1. **Database Setup**
   - Create new table schemas
   - Implement storage interfaces and models
   - Set up database migrations

2. **Service Framework**
   - Implement `internal/engine/` with engine/server/service pattern
   - Set up new configuration management via `/pkg/config`
   - Implement basic gRPC server structure

3. **VirtualLink Transport**
   - Implement VirtualLink abstraction with lane management
   - Set up baseline lane configuration (control, priority, bulk)
   - Implement per-lane credit windows and backpressure
   - Basic message serialization/deserialization with lane_id

4. **Basic Mesh Control**
   - Implement basic mesh control over gRPC
      - Seeding a new mesh
      - Joining an existing mesh
      - Starting the mesh
      - Stopping the mesh
      - Leaving the mesh
      - Evicting a node from the mesh

**Deliverables**:
- New database schema active
- Service starts and accepts gRPC connections
- VirtualLink connections established with baseline lane configuration
- Basic gRPC services implemented

### Phase 2: Consensus and Membership
**Goal**: Implement unified Raft consensus and SWIM membership

**Tasks**:
1. **Raft Implementation**
   - Set up Raft groups (MCG/DSG) 
   - Implement PostgreSQL log/stable store
   - Set up leader election and log replication

2. **SWIM Membership**
   - Implement failure detection protocol
   - Node join/leave procedures
   - Health monitoring and suspicion spreading

3. **Basic Topology**
   - Node and link state tracking
   - Basic LSA generation and flooding
   - gRPC services for adding and removing links

**Deliverables**:
- Nodes can form consensus groups
- Membership changes propagated via SWIM
- Basic topology awareness

### Phase 3: Routing and Path Computation
**Goal**: Implement link-state routing with k-shortest paths

**Tasks**:
1. **Link-State Database**
   - LSA processing and storage
   - Version management and conflict resolution  
   - Topology graph construction

2. **Path Computation**
   - Dijkstra + Yen's algorithm for k-shortest paths
   - Path cost calculation with configurable weights
   - Path caching and incremental updates

3. **Basic Routing**
   - Route table construction
   - Forwarding decision logic
   - Next-hop determination

**Deliverables**:
- Multi-path routing between any two nodes
- Dynamic path recomputation on topology changes
- Route optimization based on link metrics

### Phase 4: Stream Management and Data Plane
**Goal**: Implement stream-based data delivery with exactly-once semantics

**Tasks**:
1. **Stream Management**
   - Stream creation and lifecycle
   - DSG formation for stream participants
   - Sequence number management

2. **Data Plane Handlers**
   - System update handlers
   - Internal DB update handlers
   - Client data handlers with tenant encryption

3. **Outbox/Inbox Pattern**
   - Redis-based queue implementation
   - Reliable delivery with retry logic
   - Deduplication and exactly-once processing

**Deliverables**:  
- Reliable stream-based message delivery
- Exactly-once processing semantics
- Tenant-specific data encryption

### Phase 5: VirtualLink Advanced Features
**Goal**: Implement adaptive lane management and advanced VirtualLink features

**Tasks**:
1. **Adaptive Lane Management**
   - Scale-up logic for bulk lanes based on performance metrics
   - Scale-down logic for idle lanes
   - Cool-down mechanisms to prevent connection storms
   - Per-peer connection caps for NAT/firewall safety

2. **Advanced Failure Handling**
   - Lane health monitoring with heartbeats and application acks
   - Lane failure detection and stream rerouting
   - Control lane recovery with priority promotion
   - Graceful degradation to fewer lanes

3. **Multi-lane Striping**
   - Large stream striping across multiple lanes
   - Stream-level sequencing and reassembly
   - Enlarged reorder buffers for striped streams
   - Control stream protection (never striped)

**Deliverables**:
- Adaptive lane scaling based on performance
- Robust failure handling with lane recovery
- Multi-lane striping for large streams

### Phase 6: Multi-Path Load Balancing
**Goal**: Implement sophisticated traffic scheduling and flow control

**Tasks**:
1. **Multi-Path Scheduler**
   - Weighted round-robin across paths
   - Chunk-based load balancing
   - Out-of-order mitigation

2. **Flow Control**
   - Credit-based backpressure
   - Per-stream and per-link credit management
   - Dynamic credit allocation based on capacity

3. **Fast Reroute**
   - Link failure detection
   - Automatic failover to backup paths
   - In-flight message recovery

**Deliverables**:
- Traffic spreads across multiple paths
- Automatic failover on link failures  
- Backpressure prevents resource exhaustion

### Phase 7: Security and Observability
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

3. **API Completion**
   - All gRPC endpoints fully implemented
   - Streaming APIs with proper backpressure
   - Error handling and recovery

**Deliverables**:
- Production-ready security posture
- Complete observability stack
- All APIs functional and documented

### Phase 8: Testing and Optimization
**Goal**: Ensure reliability and performance targets

**Tasks**:
1. **Comprehensive Testing**
   - Unit tests for all components
   - Integration tests for end-to-end flows
   - Chaos testing for failure scenarios
   - Load testing for performance validation

2. **Performance Optimization**
   - Profiling and bottleneck identification
   - Memory and CPU optimization
   - Network protocol tuning
   - Database query optimization

3. **Documentation**
   - API documentation
   - Operational runbooks  
   - Configuration reference
   - Troubleshooting guides

**Deliverables**:
- All acceptance criteria met
- Performance targets achieved
- Complete documentation suite

## Deployment Strategy

### Initial Deployment
1. **Deploy new mesh service version**
2. **Gradual rollout with canary testing**
3. **Monitor metrics and rollback if needed**
4. **Complete rollout to all nodes**

### Configuration Management
- Global config file + overrides via env via `/pkg/config`
- Dynamic reconfig via SystemUpdate events
- Hot-reload support for non-critical settings

## Risk Mitigation

### Technical Risks
- **Raft consensus complexity**: Use mature library (HashiCorp Raft)
- **Performance degradation**: Extensive load testing and profiling
- **Network partition handling**: Thorough chaos testing
- **Security vulnerabilities**: Comprehensive security review

### Timeline Risks
- **Complex routing algorithms**: Start with simpler implementation, optimize later
- **Integration challenges**: Early integration testing with other services
- **Unforeseen complexity**: Build 20% buffer into each phase

## Success Criteria

### Functional Requirements
- [ ] Asymmetric connectivity with multi-hop routing
- [ ] k-shortest path computation and multi-path load balancing  
- [ ] Exactly-once delivery semantics for all traffic classes
- [ ] Tenant-specific encryption for client data
- [ ] Consensus-based consistency for all updates
- [ ] NAT traversal without inbound port requirements

### Performance Requirements  
- [ ] ≥ 10k msgs/sec sustained throughput per node
- [ ] ≥ 1 Gbps aggregate throughput
- [ ] p50 < 30ms added latency per hop
- [ ] p99 < 120ms end-to-end latency
- [ ] < 500ms link failure recovery time
- [ ] < 5s node restart recovery time

### Operational Requirements
- [ ] Complete observability with metrics, logs, traces
- [ ] Automated health checks and alerting
- [ ] Rolling upgrades without service disruption  
- [ ] Configuration management with hot-reload
- [ ] Comprehensive API documentation

## Implementation Notes

### Development Approach
- **Component-first development**: Build and test each component independently
- **Integration testing**: Early and continuous integration testing
- **Performance from day one**: Performance considerations in all design decisions
- **Security by design**: Security controls built into every component

### Quality Assurance
- **Code reviews**: All code changes reviewed by team members
- **Automated testing**: Comprehensive test suite with high coverage
- **Static analysis**: Security scanning and code quality checks
- **Performance monitoring**: Continuous performance regression detection

### Documentation Requirements
- **Architecture Decision Records (ADRs)**: Document all major design decisions
- **API documentation**: OpenAPI specs for all gRPC services  
- **Operational guides**: Deployment, monitoring, troubleshooting procedures
- **Developer documentation**: Code organization, contribution guidelines

## Progress Tracking

### Phase 1: Foundation
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Database Setup
- [ ] Create new table schemas
- [ ] Implement storage interfaces and models
- [ ] Set up database migrations
- [ ] Test database connectivity and operations

#### Service Framework
- [ ] Implement `internal/engine/` with engine/server/service pattern
- [ ] Set up new configuration management via `/pkg/config`
- [ ] Implement basic gRPC server structure
- [ ] Test service lifecycle (start/stop/health)

#### Basic Transport
- [ ] Implement new WebSocket framing protocol
- [ ] Set up connection management
- [ ] Basic message serialization/deserialization
- [ ] Test WebSocket connectivity between nodes

**Implementation Notes**:
- Ensure database migrations are idempotent and can be run multiple times safely
- Use `/pkg/service.BaseService` pattern consistently with other microservices
- VirtualLink should support collapsible mode for testing and constrained environments
- Lane health monitoring should be implemented from the start to support failure handling

### Phase 2: Consensus and Membership
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Raft Implementation
- [ ] Set up Raft groups (MCG/DSG)
- [ ] Implement PostgreSQL log/stable store
- [ ] Set up leader election and log replication
- [ ] Test consensus group formation and leadership changes

#### SWIM Membership
- [ ] Implement failure detection protocol
- [ ] Node join/leave procedures
- [ ] Health monitoring and suspicion spreading
- [ ] Test membership propagation and failure detection

#### Basic Topology
- [ ] Node and link state tracking
- [ ] Basic LSA generation and flooding
- [ ] Test topology discovery and state synchronization

**Implementation Notes**:
- Use existing PostgreSQL-based Raft implementation (already implemented in `stores/`)
- HashiCorp Raft library is battle-tested and well-documented
- SWIM protocol should be configurable for different network environments
- LSA flooding should be efficient to avoid network congestion in large meshes

### Phase 3: Routing and Path Computation
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Link-State Database
- [ ] LSA processing and storage
- [ ] Version management and conflict resolution
- [ ] Topology graph construction
- [ ] Test LSA propagation and database consistency

#### Path Computation
- [ ] Dijkstra + Yen's algorithm for k-shortest paths
- [ ] Path cost calculation with configurable weights
- [ ] Path caching and incremental updates
- [ ] Test path computation accuracy and performance

#### Basic Routing
- [ ] Route table construction
- [ ] Forwarding decision logic
- [ ] Next-hop determination
- [ ] Test routing decisions and path selection

**Implementation Notes**:
- Path computation should be cached and only recomputed when topology changes
- Consider using a mature graph library like `gonum.org/v1/gonum/graph` for path algorithms
- Path cost weights (α, β, γ, δ, ε) should be configurable per traffic class

### Phase 4: Stream Management and Data Plane
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Stream Management
- [ ] Stream creation and lifecycle
- [ ] DSG formation for stream participants
- [ ] Sequence number management
- [ ] Test stream creation and DSG formation

#### Data Plane Handlers
- [ ] System update handlers
- [ ] Internal DB update handlers
- [ ] Client data handlers with tenant encryption
- [ ] Test all handler types with various payloads

#### Outbox/Inbox Pattern
- [ ] Redis-based queue implementation
- [ ] Reliable delivery with retry logic
- [ ] Deduplication and exactly-once processing
- [ ] Test message delivery reliability and deduplication

**Implementation Notes**:
- Tenant encryption via `/pkg/encryption` is critical for client data security
- Exactly-once processing requires careful transaction management
- Redis queues should have proper error handling and recovery mechanisms

### Phase 5: VirtualLink Advanced Features
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Adaptive Lane Management
- [ ] Scale-up logic for bulk lanes based on performance metrics
- [ ] Scale-down logic for idle lanes
- [ ] Cool-down mechanisms to prevent connection storms
- [ ] Per-peer connection caps for NAT/firewall safety
- [ ] Test adaptive lane scaling

#### Advanced Failure Handling
- [ ] Lane health monitoring with heartbeats and application acks
- [ ] Lane failure detection and stream rerouting
- [ ] Control lane recovery with priority promotion
- [ ] Graceful degradation to fewer lanes
- [ ] Test lane failure scenarios

#### Multi-lane Striping
- [ ] Large stream striping across multiple lanes
- [ ] Stream-level sequencing and reassembly
- [ ] Enlarged reorder buffers for striped streams
- [ ] Control stream protection (never striped)
- [ ] Test multi-lane striping performance

**Implementation Notes**:
- Adaptive lane scaling should be based on actual performance metrics
- Lane health monitoring is critical for failure detection
- Multi-lane striping should only be used for large streams that can tolerate reassembly latency

### Phase 6: Multi-Path Load Balancing
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Multi-Path Scheduler
- [ ] Weighted round-robin across paths
- [ ] Chunk-based load balancing
- [ ] Out-of-order mitigation
- [ ] Test load balancing across multiple paths

#### Flow Control
- [ ] Credit-based backpressure
- [ ] Per-stream and per-link credit management
- [ ] Dynamic credit allocation based on capacity
- [ ] Test flow control under various load conditions

#### Fast Reroute
- [ ] Link failure detection
- [ ] Automatic failover to backup paths
- [ ] In-flight message recovery
- [ ] Test failover scenarios and recovery times

**Implementation Notes**:
- Fast reroute should complete within 500ms to meet performance targets
- Credit allocation should be dynamic based on link capacity and utilization
- Out-of-order mitigation is crucial for maintaining stream ordering

### Phase 7: Security and Observability
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Security Hardening
- [ ] mTLS for node authentication
- [ ] JWT for application authentication
- [ ] ACL enforcement for tenant isolation
- [ ] Message signatures for integrity
- [ ] Test all security mechanisms

#### Observability
- [ ] Prometheus metrics collection
- [ ] OpenTelemetry tracing
- [ ] Structured logging with correlation IDs
- [ ] Health check implementation
- [ ] Test observability stack

#### API Completion
- [ ] All gRPC endpoints fully implemented
- [ ] Streaming APIs with proper backpressure
- [ ] Error handling and recovery
- [ ] Test all API endpoints

**Implementation Notes**:
- Security should be implemented early and tested thoroughly
- Observability is crucial for production operations - don't skimp on this
- API backpressure is essential to prevent resource exhaustion

### Phase 8: Testing and Optimization
**Status**: [ ] Not Started [ ] In Progress [ ] Complete

#### Comprehensive Testing
- [ ] Unit tests for all components
- [ ] Integration tests for end-to-end flows
- [ ] Chaos testing for failure scenarios
- [ ] Load testing for performance validation
- [ ] Test coverage should exceed 80%

#### Performance Optimization
- [ ] Profiling and bottleneck identification
- [ ] Memory and CPU optimization
- [ ] Network protocol tuning
- [ ] Database query optimization
- [ ] Meet all performance targets

#### Documentation
- [ ] API documentation
- [ ] Operational runbooks
- [ ] Configuration reference
- [ ] Troubleshooting guides
- [ ] Developer documentation

**Implementation Notes**:
- Performance optimization should be data-driven based on profiling results
- Chaos testing is essential for validating failure scenarios
- Documentation should be comprehensive and kept up-to-date

### Overall Progress Summary

#### Functional Requirements
- [ ] Asymmetric connectivity with multi-hop routing
- [ ] k-shortest path computation and multi-path load balancing
- [ ] Exactly-once delivery semantics for all traffic classes
- [ ] Tenant-specific encryption for client data
- [ ] Consensus-based consistency for all updates
- [ ] NAT traversal without inbound port requirements
- [ ] VirtualLink architecture with adaptive lane management

#### Performance Requirements
- [ ] ≥ 10k msgs/sec sustained throughput per node
- [ ] ≥ 1 Gbps aggregate throughput
- [ ] p50 < 30ms added latency per hop
- [ ] p99 < 120ms end-to-end latency
- [ ] < 500ms link failure recovery time
- [ ] < 5s node restart recovery time

#### Operational Requirements
- [ ] Complete observability with metrics, logs, traces
- [ ] Automated health checks and alerting
- [ ] Rolling upgrades without service disruption
- [ ] Configuration management with hot-reload
- [ ] Comprehensive API documentation

### Critical Implementation Notes

#### Architecture Decisions
- **Raft Consensus**: Using HashiCorp Raft with custom PostgreSQL storage (LogStore/StableStore) and Redis snapshots
- **VirtualLink Transport**: Lane-based WebSocket connections with adaptive scaling
- **Database**: PostgreSQL for persistent state, Redis for queues and caching
- **Encryption**: Tenant-specific encryption via `/pkg/encryption` for client data
- **Routing**: Link-state with k-shortest path computation

#### Dependencies and Integration
- **Shared Packages**: Leverage `/pkg/service`, `/pkg/database`, `/pkg/logger`, `/pkg/health`, `/pkg/encryption`, `/pkg/config`, `/pkg/grpc`
- **External Libraries**: HashiCorp Raft, Gorilla WebSocket, Prometheus, OpenTelemetry
- **Supervisor Integration**: gRPC both ways for service management

#### Risk Mitigation
- **Performance**: Extensive load testing and profiling throughout development
- **Security**: Security review and penetration testing before production
- **Reliability**: Chaos testing and failure scenario validation
- **Complexity**: Start simple and optimize incrementally

#### Success Criteria
- All functional, performance, and operational requirements met
- Comprehensive test coverage (>80%)
- Production-ready security posture
- Complete documentation suite
- Successful deployment and operation in test environment

This implementation plan provides a comprehensive roadmap for building the mesh service from scratch according to the sophisticated architecture defined in MESH_REFACTOR_TARGET_STATE.md while leveraging existing shared packages and maintaining operational excellence.