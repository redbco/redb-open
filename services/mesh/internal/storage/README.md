# Mesh Storage Package

This package provides storage abstraction for the mesh service, with proper integration to the shared `/pkg/database` package.

## Overview

The storage package has been refactored to:
- Use the shared `/pkg/database` package for PostgreSQL connections
- Implement a comprehensive interface for all mesh storage operations
- Provide proper transaction support
- Include proper error handling and validation
- Follow consistent patterns and logging

## Architecture

### Interface (`interface.go`)
Defines the storage interface with operations for:
- Message storage and retrieval
- State management
- Node state operations
- Consensus log operations
- Route management
- Configuration storage
- Transaction support
- Administrative operations

### PostgreSQL Implementation (`postgres.go`)
- Uses the shared database package for connections
- Implements all interface methods
- Provides transaction support
- Includes proper table initialization
- Uses appropriate indexes for performance

## Configuration

The storage configuration supports multiple formats:

```go
// Basic configuration
config := storage.Config{
    Type: "postgres",
    PostgreSQL: database.PostgreSQLConfig{
        User:              "redb",
        Password:          "redb", 
        Host:              "localhost",
        Port:              5432,
        Database:          "redb_mesh",
        SSLMode:           "disable",
        MaxConnections:    10,
        ConnectionTimeout: 5 * time.Second,
    },
}

// From connection string
config, err := storage.ConfigFromConnectionString("postgres://user:pass@localhost/meshdb")

// From global config (with defaults)
config := storage.ConfigFromGlobal(globalConfig)
```

## Usage

### Basic Storage Operations

```go
import (
    "context"
    "github.com/redbco/redb-open/services/mesh/internal/storage"
    "go.uber.org/zap"
)

// Initialize storage
ctx := context.Background()
config := storage.ConfigFromGlobal(nil) // Uses defaults
logger := zap.NewDevelopment()

store, err := storage.NewStorage(ctx, config, logger)
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Store a message
msg := &storage.Message{
    ID:        "msg-001",
    From:      "node-a",
    To:        "node-b", 
    Content:   []byte("hello"),
    Timestamp: time.Now().Unix(),
}

err = store.StoreMessage(ctx, msg)
if err != nil {
    log.Printf("Failed to store message: %v", err)
}

// Retrieve a message
retrievedMsg, err := store.GetMessage(ctx, "msg-001")
if err != nil {
    log.Printf("Failed to get message: %v", err)
}
```

### State Operations

```go
// Store state
key := "node_status"
value := []byte(`{"status": "active", "last_seen": "2023-01-01T00:00:00Z"}`)

err = store.StoreState(ctx, key, value)
if err != nil {
    log.Printf("Failed to store state: %v", err)
}

// Get state
retrievedValue, err := store.GetState(ctx, key)
if err != nil {
    log.Printf("Failed to get state: %v", err)
}
```

### Transaction Support

```go
// Create transaction
tx, err := store.CreateTransaction(ctx)
if err != nil {
    log.Printf("Failed to create transaction: %v", err)
    return
}

// Perform operations within transaction
err = tx.StoreMessage(ctx, msg1)
if err != nil {
    tx.Rollback()
    return
}

err = tx.StoreState(ctx, "key", []byte("value"))
if err != nil {
    tx.Rollback()
    return
}

// Commit transaction
err = tx.Commit()
if err != nil {
    log.Printf("Failed to commit transaction: %v", err)
}
```

### Consensus Operations

```go
// Append to consensus log
term := uint64(1)
index := uint64(1)
entry := map[string]interface{}{
    "command": "set",
    "key":     "consensus_state",
    "value":   "active",
}

err = store.AppendLog(ctx, term, index, entry)
if err != nil {
    log.Printf("Failed to append log: %v", err)
}

// Get log entries
entries, err := store.GetLogs(ctx, 1, 10)
if err != nil {
    log.Printf("Failed to get logs: %v", err)
}
```

## Database Schema

The storage implementation creates the following tables:

- `mesh_messages`: Message storage with indexes on timestamp, from_node, to_node
- `mesh_state`: Key-value state storage
- `mesh_node_state`: Node-specific state storage (JSONB)
- `mesh_consensus_log`: Consensus log entries with term/index primary key
- `mesh_routes`: Route information (JSONB)
- `mesh_config`: Configuration storage (JSONB)

All tables include proper timestamps and are prefixed with `mesh_` to avoid conflicts.

## Error Handling

The storage implementation provides comprehensive error handling:
- Input validation (non-empty keys, non-nil messages, etc.)
- Database connection errors
- Transaction errors
- Not found errors vs other database errors
- Proper error wrapping for debugging

## Integration with Mesh Engine

The mesh engine now properly initializes storage:

```go
// In engine.Start()
storageConfig := storage.ConfigFromGlobal(e.config)
e.storage, err = storage.NewStorage(ctx, storageConfig, e.logger)
if err != nil {
    e.logger.Error("Failed to initialize storage", zap.Error(err))
    e.storage = nil
} else {
    e.logger.Info("Storage initialized successfully", zap.String("type", storageConfig.Type))
}

// Storage is passed to mesh node
e.meshNode, err = mesh.NewNode(meshConfig, e.storage, e.logger)
```

## Health Checks

The storage includes health check functionality:

```go
func (e *Engine) CheckStorage() error {
    if e.storage == nil {
        return fmt.Errorf("storage not initialized")
    }

    // Test storage connectivity
    testKey := "health_check"
    testValue := []byte("test")
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := e.storage.StoreState(ctx, testKey, testValue); err != nil {
        return fmt.Errorf("storage write test failed: %w", err)
    }

    if _, err := e.storage.GetState(ctx, testKey); err != nil {
        return fmt.Errorf("storage read test failed: %w", err)
    }

    // Clean up test data
    e.storage.DeleteState(ctx, testKey)
    return nil
}
```

## Key Improvements

1. **Proper shared package usage**: Uses `/pkg/database` correctly with proper configuration
2. **Complete interface implementation**: All methods from the interface are implemented
3. **Transaction support**: Full transaction support with commit/rollback
4. **Error handling**: Comprehensive error handling and validation
5. **Logging**: Proper structured logging throughout
6. **Performance**: Appropriate indexes and optimized queries
7. **Type safety**: Proper type definitions and JSON marshaling/unmarshaling
8. **Resource management**: Proper connection pooling and cleanup

## Migration Notes

When migrating from the old implementation:
1. Update import statements to use the new factory function
2. Update configuration to use the new Config struct
3. Update error handling to handle the new error types
4. Update any direct database access to use the interface methods
5. Test all storage operations thoroughly

## Future Enhancements

1. Add Redis implementation for caching
2. Add memory implementation for testing
3. Add metrics collection for storage operations
4. Add connection string parsing for more flexible configuration
5. Add backup/restore functionality implementation
6. Add schema migration support

# Mesh Storage Architecture

This document explains the database table structure and relationships for the mesh microservice storage system.

## Table Architecture Overview

The mesh storage system uses a two-layer approach:

1. **Infrastructure/Configuration Layer** - Persistent administrative configuration
2. **Runtime/Operational Layer** - Dynamic operational data and state

## Infrastructure/Configuration Layer Tables

These tables are managed by the core system and contain persistent configuration:

### `mesh` Table
- **Purpose**: High-level mesh network definition and administrative configuration
- **Contains**: Mesh identity, authentication keys, join permissions, administrative status
- **Usage**: Managed by mesh administrators, defines the overall mesh network
- **Example**: A mesh named "production-mesh" with specific join keys and permissions

### `nodes` Table  
- **Purpose**: Physical/logical node definitions in the infrastructure topology
- **Contains**: Node identity, connection details, platform info, regional assignment
- **Usage**: Defines the infrastructure nodes that can participate in the mesh
- **Example**: A node "node-us-east-1" running on AWS in the us-east-1 region

### `routes` Table
- **Purpose**: Static network routes between nodes with performance characteristics
- **Contains**: Configured network paths with latency, bandwidth, and cost metrics
- **Usage**: Defines the underlying network topology and preferred paths
- **Example**: A route from "node-us-east-1" to "node-eu-west-1" with 150ms latency

## Runtime/Operational Layer Tables

These tables are managed by the mesh microservice for dynamic operations:

### `mesh_messages` Table
- **Purpose**: Inter-node message passing and communication
- **Contains**: Message ID, sender/receiver nodes, content, timestamps
- **Usage**: Stores messages being routed through the mesh network
- **Example**: A data replication message from node A to node B

### `mesh_state` Table
- **Purpose**: Distributed state management across the mesh
- **Contains**: Key-value pairs for shared state information
- **Usage**: Stores distributed configuration and state that all nodes need to access
- **Example**: Current leader election state, shared configuration values

### `mesh_node_state` Table
- **Purpose**: Individual node runtime state tracking
- **Contains**: Per-node runtime state and health information
- **Usage**: Tracks the current operational state of each node
- **Relationship**: Complements the `nodes` table (static) with dynamic state
- **Example**: Current CPU usage, memory state, active connections for a node

### `mesh_consensus_log` Table
- **Purpose**: Distributed consensus algorithm operations (Raft log)
- **Contains**: Consensus terms, log entries, and consensus operations
- **Usage**: Implements distributed consensus for the mesh network
- **Example**: Raft log entries for leader election and state synchronization

### `mesh_routing_table` Table
- **Purpose**: Dynamic routing table for message routing decisions
- **Contains**: Destination-to-route mappings for dynamic message routing
- **Usage**: Runtime routing decisions based on current network conditions
- **Relationship**: Complements the `routes` table (static topology) with dynamic routing
- **Example**: Current best path to reach a specific service, updated based on network conditions

### `mesh_runtime_config` Table
- **Purpose**: Runtime configuration storage for operational parameters
- **Contains**: Key-value pairs for ephemeral configuration
- **Usage**: Stores runtime settings that can change during mesh operation
- **Relationship**: Complements the `mesh` table (admin config) with operational config
- **Example**: Current timeouts, retry counts, feature flags that change during runtime

## Key Relationships and Distinctions

### Static vs Dynamic
- **Static Tables** (`mesh`, `nodes`, `routes`): Administrative configuration, rarely changes
- **Dynamic Tables** (`mesh_*`): Operational data, changes frequently during runtime

### Network Routing
- **`routes` table**: "This is the physical network topology we have configured"
- **`mesh_routing_table` table**: "This is the best path to route a message right now"

### Configuration
- **`mesh` table**: "This is how the mesh is administratively configured" 
- **`mesh_runtime_config` table**: "These are the current operational settings"

### Node Information
- **`nodes` table**: "These are the nodes that exist in our infrastructure"
- **`mesh_node_state` table**: "This is the current runtime state of each node"

## Usage Patterns

### Mesh Microservice Operations
The mesh microservice primarily operates on the runtime tables:
- Stores and retrieves messages via `mesh_messages`
- Manages distributed state via `mesh_state` 
- Tracks node health via `mesh_node_state`
- Implements consensus via `mesh_consensus_log`
- Makes routing decisions via `mesh_routing_table`
- Manages runtime config via `mesh_runtime_config`

### Administrative Operations
System administrators work with the infrastructure tables:
- Define mesh networks in `mesh`
- Register nodes in `nodes` 
- Configure network topology in `routes`

### Integration Points
The mesh microservice can query infrastructure tables for:
- Current mesh configuration from `mesh`
- Available nodes from `nodes`
- Network topology from `routes`

This architecture provides clear separation between persistent infrastructure configuration and dynamic operational state, allowing for better maintainability and performance optimization. 