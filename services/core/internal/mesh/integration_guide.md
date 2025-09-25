# Core-Mesh Service Integration Guide

## Overview

This document describes the integration between the `core` service (Go) and the `mesh` service (Rust) in the reDB system. The integration enables the core service to send messages to other nodes in the mesh and receive messages from them for distributed operations.

## Architecture

### Components

1. **MeshCommunicationManager**: Central component that handles all mesh communication
2. **CoreMessage**: Structured message format for inter-node communication
3. **Message Handlers**: Pluggable handlers for different message types
4. **Subscription Management**: Handles incoming message streams from the mesh

### Message Flow

```
Core Service A → MeshCommunicationManager → Mesh Service A → Mesh Network → Mesh Service B → MeshCommunicationManager → Core Service B
```

## Message Types

### 1. Database Updates (`db_update`)
- **Purpose**: Notify other nodes about database changes
- **Usage**: Data replication, cache invalidation
- **QoS**: High priority (class 2)
- **Partition**: Data partition (1)

### 2. Anchor Queries (`anchor_query`)
- **Purpose**: Query anchor services on remote nodes
- **Usage**: Cross-node database queries
- **QoS**: Medium priority (class 1)
- **Partition**: Query partition (2)

### 3. Commands (`command`)
- **Purpose**: Execute administrative commands on remote nodes
- **Usage**: Cluster management, configuration updates
- **QoS**: Medium priority (class 1)
- **Partition**: Data partition (1)

### 4. Responses (`response`)
- **Purpose**: Response to synchronous requests
- **Usage**: Request-response patterns
- **QoS**: Inherits from original request
- **Partition**: Inherits from original request

## Integration Points

### 1. Engine Integration

The `Engine` struct in the core service has been extended with:

```go
type Engine struct {
    // ... existing fields ...
    meshManager *mesh.MeshCommunicationManager
    nodeID      uint64
}
```

### 2. Initialization

During engine startup:

1. gRPC clients are created for mesh service
2. Node ID is determined from configuration
3. MeshCommunicationManager is initialized
4. Subscription to mesh messages is established
5. Default message handlers are registered

**Note**: Heartbeat and keepalive functionality is handled automatically by the mesh service at the session level, so no additional heartbeat implementation is needed in the core service.

### 3. Message Handling

The system supports both:

- **Fire-and-forget**: Messages sent without waiting for response
- **Request-response**: Messages with correlation IDs for synchronous communication

## Usage Examples

### Broadcasting Database Updates

```go
func (s *Server) CreateWorkspace(ctx context.Context, req *corev1.CreateWorkspaceRequest) (*corev1.CreateWorkspaceResponse, error) {
    // ... create workspace in local database ...
    
    // Broadcast update to other nodes
    if s.engine.GetMeshManager() != nil {
        data := map[string]interface{}{
            "workspace_id": workspace.ID,
            "workspace_name": workspace.Name,
            "operation": "create",
        }
        
        go func() {
            ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
            defer cancel()
            s.engine.GetMeshManager().BroadcastDBUpdate(ctx, "workspace_created", data)
        }()
    }
    
    return response, nil
}
```

### Querying Remote Anchor Services

```go
func (s *Server) QueryRemoteDatabase(ctx context.Context, targetNodeID uint64, query string) (interface{}, error) {
    if s.engine.GetMeshManager() == nil {
        return nil, fmt.Errorf("mesh manager not available")
    }
    
    queryData := map[string]interface{}{
        "database_id": "some-db-id",
        "query": query,
    }
    
    result, err := s.engine.GetMeshManager().QueryAnchorService(ctx, targetNodeID, queryData)
    if err != nil {
        return nil, fmt.Errorf("remote query failed: %w", err)
    }
    
    return result, nil
}
```

### Custom Message Handlers

```go
func (e *Engine) registerCustomHandlers() {
    if e.meshManager == nil {
        return
    }
    
    // Register workspace synchronization handler
    e.meshManager.RegisterMessageHandler("workspace_sync", func(ctx context.Context, msg *meshv1.Received) error {
        var coreMsg mesh.CoreMessage
        if err := json.Unmarshal(msg.Payload, &coreMsg); err != nil {
            return err
        }
        
        // Handle workspace synchronization
        return e.handleWorkspaceSync(ctx, &coreMsg)
    })
}
```

## Configuration

### Node ID Configuration

The node ID can be configured in several ways:

1. **Configuration file**: `node.id` setting
2. **Database**: Retrieved from local identity table
3. **Default**: Falls back to node ID 1

### Mesh Service Connection

The mesh service connection is configured via:

- `services.mesh.grpc_address` (default: `localhost:50056`)

## Error Handling

### Connection Failures

- Mesh manager gracefully handles mesh service unavailability
- Operations continue locally when mesh is unavailable
- Automatic reconnection attempts

### Message Delivery Failures

- Failed messages are logged but don't block operations
- Timeout handling for synchronous requests
- Acknowledgment tracking for critical messages

## Monitoring

### Message Metrics

```go
// Get message delivery metrics
metrics, err := meshManager.meshControlClient.GetMessageMetrics(ctx, &meshv1.GetMessageMetricsRequest{})
```

### Topology Information

```go
// Get current mesh topology
topology, err := meshManager.meshControlClient.GetTopology(ctx, &meshv1.GetTopologyRequest{})
```

## Best Practices

### 1. Asynchronous Operations

Use goroutines for non-critical mesh operations to avoid blocking main operations:

```go
go func() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    meshManager.BroadcastDBUpdate(ctx, operation, data)
}()
```

### 2. Timeout Management

Always use contexts with timeouts for mesh operations:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
```

### 3. Error Handling

Don't fail main operations due to mesh communication errors:

```go
if err := meshManager.BroadcastDBUpdate(ctx, operation, data); err != nil {
    logger.Warnf("Failed to broadcast update: %v", err)
    // Continue with main operation
}
```

### 4. Message Partitioning

Use appropriate partitions and QoS classes based on message importance:

- Commands: partition 0, QoS 1
- Data updates: partition 1, QoS 2
- Queries: partition 2, QoS 1

## Security Considerations

### Message Authentication

- Messages include source node information
- Mesh service handles node authentication
- Message integrity is ensured by the mesh layer

### Data Encryption

- End-to-end encryption can be enabled via `end_to_end_encrypt` flag
- TLS encryption is handled at the mesh transport layer

## Performance Considerations

### Message Size

- Keep message payloads reasonable (< 1MB recommended)
- Use compression for large data sets
- Consider pagination for bulk operations

### Batching

- Batch related operations when possible
- Use appropriate send modes based on urgency
- Consider message aggregation for high-frequency updates

## Troubleshooting

### Common Issues

1. **Mesh service not available**: Check mesh service status and configuration
2. **Node ID conflicts**: Ensure unique node IDs across the mesh
3. **Message delivery failures**: Check network connectivity and mesh topology
4. **Handler registration**: Ensure handlers are registered before starting subscriptions

### Debugging

- Enable debug logging for mesh operations
- Monitor message metrics for delivery success rates
- Check mesh topology for connectivity issues
- Verify message handler registration

## Future Enhancements

### Planned Features

1. **Message Persistence**: Store critical messages for retry
2. **Load Balancing**: Distribute queries across multiple nodes
3. **Circuit Breaker**: Automatic fallback for failed nodes
4. **Message Compression**: Reduce network overhead
5. **Metrics Integration**: Enhanced monitoring and alerting
