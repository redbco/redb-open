# Database Adapter File Structure

This document describes the standard file structure and organization for database adapters in ReDB. Following this structure ensures consistency across all database implementations and makes the codebase easier to maintain.

## Overview

Each database adapter is a self-contained package within `services/anchor/internal/database/` that implements the adapter interfaces defined in `pkg/anchor/adapter`. The adapter pattern provides a consistent way to interact with different databases while encapsulating database-specific logic.

## Directory Structure

Each database adapter follows this standard structure:

```
services/anchor/internal/database/
└── <database_name>/
    ├── adapter.go           # Main adapter implementation
    ├── init.go              # Adapter registration
    ├── connection.go        # Connection management
    ├── schema_ops.go        # Schema operations
    ├── data_ops.go          # Data operations
    ├── metadata_ops.go      # Metadata collection
    ├── replication_ops.go   # Replication (optional)
    ├── types.go             # Database-specific types (optional)
    ├── utils.go             # Helper functions (optional)
    └── *_test.go            # Tests
```

## Core Files (Required)

### `adapter.go` - Main Adapter Implementation

**Purpose:** Implements the `adapter.DatabaseAdapter` interface and defines the Connection/InstanceConnection types.

**Key Components:**
- `Adapter` struct implementing `adapter.DatabaseAdapter`
- `Connection` struct implementing `adapter.Connection`
- `InstanceConnection` struct implementing `adapter.InstanceConnection`
- Connection factory methods (`Connect`, `ConnectInstance`)

**Example Structure:**
```go
package yourdb

import (
    "context"
    "sync/atomic"
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Adapter implements adapter.DatabaseAdapter
type Adapter struct{}

func NewAdapter() adapter.DatabaseAdapter {
    return &Adapter{}
}

func (a *Adapter) Type() dbcapabilities.DatabaseType {
    return dbcapabilities.YourDatabase
}

func (a *Adapter) Capabilities() dbcapabilities.Capability {
    return dbcapabilities.MustGet(dbcapabilities.YourDatabase)
}

func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
    // Implementation
}

func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
    // Implementation
}

// Connection implements adapter.Connection
type Connection struct {
    id        string
    db        *YourDatabaseClient
    config    adapter.ConnectionConfig
    adapter   *Adapter
    connected int32
}

// Implement Connection methods (ID, Type, IsConnected, Ping, Close)
// Implement operation getters (SchemaOperations, DataOperations, etc.)

// InstanceConnection implements adapter.InstanceConnection
type InstanceConnection struct {
    id        string
    client    *YourInstanceClient
    config    adapter.InstanceConfig
    adapter   *Adapter
    connected int32
}

// Implement InstanceConnection methods
```

**Typical Size:** 200-400 lines

---

### `init.go` - Adapter Registration

**Purpose:** Automatically registers the adapter with the global registry when the package is imported.

**Content:**
```go
package yourdb

import (
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

func init() {
    adapter.GlobalRegistry().Register(
        dbcapabilities.YourDatabase,
        NewAdapter(),
    )
}
```

**Typical Size:** 10-15 lines

---

### `connection.go` - Connection Management

**Purpose:** Handles the actual database connection logic, including connection string building, authentication, and SSL configuration.

**Key Functions:**
- `Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error)` - Database connection
- `ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error)` - Instance connection
- `buildConnectionString()` - Connection string builder
- Connection pooling configuration
- SSL/TLS setup

**Example Functions:**
```go
package yourdb

import (
    "fmt"
    "github.com/redbco/redb-open/pkg/encryption"
    "github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Connect establishes a connection to a specific database
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
    // 1. Decrypt password
    decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
    if err != nil {
        return nil, err
    }

    // 2. Build connection string
    connString := buildConnectionString(config, decryptedPassword)

    // 3. Establish connection
    db, err := yourdriver.Connect(connString)
    if err != nil {
        return nil, err
    }

    // 4. Test connection
    if err := db.Ping(); err != nil {
        db.Close()
        return nil, err
    }

    return &dbclient.DatabaseClient{
        DB:           db,
        DatabaseType: "yourdb",
        DatabaseID:   config.DatabaseID,
        Config:       config,
        IsConnected:  1,
    }, nil
}

// ConnectInstance establishes a connection to the database instance
func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
    // Similar implementation for instance-level connection
}

func buildConnectionString(config dbclient.DatabaseConfig, password string) string {
    // Build connection string with SSL support
}
```

**Typical Size:** 100-300 lines

---

### `schema_ops.go` - Schema Operations

**Purpose:** Implements the `adapter.SchemaOperator` interface for schema discovery and manipulation.

**Key Components:**
- `SchemaOps` struct implementing `adapter.SchemaOperator`
- Schema discovery (query system tables/metadata)
- Structure creation (tables, indexes, constraints)
- Schema modification operations

**Required Methods:**
```go
package yourdb

import (
    "context"
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/unifiedmodel"
)

type SchemaOps struct {
    conn *Connection
}

func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
    // Query system tables to discover schema
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
    // Create tables/collections from UnifiedModel
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
    // List all tables/collections
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
    // Get schema for specific table
}
```

**Typical Size:** 200-600 lines depending on database complexity

---

### `data_ops.go` - Data Operations

**Purpose:** Implements the `adapter.DataOperator` interface for CRUD operations.

**Key Components:**
- `DataOps` struct implementing `adapter.DataOperator`
- Data fetching (SELECT/query operations)
- Data insertion (INSERT operations)
- Data updates (UPDATE operations)
- Data deletion (DELETE operations)
- Query execution

**Required Methods:**
```go
package yourdb

import (
    "context"
    "github.com/redbco/redb-open/pkg/anchor/adapter"
)

type DataOps struct {
    conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
    // Fetch data from table
}

func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
    // Fetch specific columns
}

func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
    // Insert data
}

func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
    // Update data
}

func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
    // Upsert data
}

func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
    // Delete data
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
    // Execute custom query
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
    // Stream large datasets (optional, can return UnsupportedOperationError)
}
```

**Typical Size:** 200-500 lines

---

### `metadata_ops.go` - Metadata Operations

**Purpose:** Implements the `adapter.MetadataOperator` interface for collecting database/instance metadata.

**Key Components:**
- `MetadataOps` struct for database metadata
- `InstanceMetadataOps` struct for instance metadata
- Version information collection
- Size and statistics collection
- Configuration information

**Required Methods:**
```go
package yourdb

import (
    "context"
)

// MetadataOps for database-level metadata
type MetadataOps struct {
    conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
    metadata := make(map[string]interface{})
    
    // Collect version, size, table count, etc.
    if version, err := getDatabaseVersion(ctx, m.conn.db); err == nil {
        metadata["version"] = version
    }
    
    if size, err := getDatabaseSize(ctx, m.conn.db); err == nil {
        metadata["size_bytes"] = size
    }
    
    if tableCount, err := getTableCount(ctx, m.conn.db); err == nil {
        metadata["table_count"] = tableCount
    }
    
    return metadata, nil
}

func (m *MetadataOps) GetVersion() (string, error) {
    // Get database version
}

func (m *MetadataOps) GetUniqueIdentifier() string {
    // Return unique identifier
}

// InstanceMetadataOps for instance-level metadata
type InstanceMetadataOps struct {
    conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
    metadata := make(map[string]interface{})
    
    // Collect instance version, uptime, connections, etc.
    if version, err := getInstanceVersion(ctx, i.conn.client); err == nil {
        metadata["version"] = version
    }
    
    if uptime, err := getInstanceUptime(ctx, i.conn.client); err == nil {
        metadata["uptime_seconds"] = uptime
    }
    
    if databases, err := i.conn.ListDatabases(ctx); err == nil {
        metadata["total_databases"] = len(databases)
    }
    
    return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion() (string, error) {
    // Get instance version
}

func (i *InstanceMetadataOps) GetUniqueIdentifier() string {
    // Return unique identifier
}
```

**Typical Size:** 100-300 lines

---

## Optional Files

### `replication_ops.go` - Replication Operations (Optional)

**Purpose:** Implements the `adapter.ReplicationOperator` interface for CDC/replication features.

**When Required:**
- Database supports change data capture (CDC)
- Database supports logical replication
- Real-time change streaming is needed

**When to Skip:**
- Database doesn't support CDC
- Replication is not a core feature
- Use `adapter.NewUnsupportedReplicationOperator()` instead

**Example (Unsupported):**
```go
package yourdb

import (
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

// In adapter.go Connection implementation
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
    return adapter.NewUnsupportedReplicationOperator(dbcapabilities.YourDatabase)
}
```

**Example (Supported):**
```go
package yourdb

import (
    "context"
    "github.com/redbco/redb-open/pkg/anchor/adapter"
)

type ReplicationOps struct {
    conn *Connection
}

func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationConnection, error) {
    // Implement CDC connection setup
}

func (r *ReplicationOps) ListSlots(ctx context.Context) ([]adapter.ReplicationSlot, error) {
    // List replication slots
}

func (r *ReplicationOps) CreateSlot(ctx context.Context, slotName string) error {
    // Create replication slot
}

func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
    // Drop replication slot
}

func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
    // Get replication status
}
```

**Typical Size:** 100-500 lines (if implemented)

---

### `types.go` - Database-Specific Types (Optional)

**Purpose:** Defines database-specific types, structures, and interfaces that don't fit in other files.

**When Needed:**
- Custom result types
- Database-specific configuration structs
- Replication source details
- Helper types for internal operations

**Example:**
```go
package yourdb

import (
    "github.com/redbco/redb-open/pkg/unifiedmodel"
)

// Custom result type
type QueryResult struct {
    Rows     []map[string]interface{}
    Metadata map[string]interface{}
}

// Database-specific schema representation
type NativeSchema struct {
    Tables      []NativeTable
    Indexes     []NativeIndex
    Constraints []NativeConstraint
}

// Replication source details (if CDC is supported)
type ReplicationSourceDetails struct {
    SourceID   string
    DatabaseID string
    SlotName   string
    Status     string
}
```

**Typical Size:** 50-200 lines

---

### `utils.go` - Helper Functions (Optional)

**Purpose:** Utility functions used across multiple operation files.

**Common Utilities:**
- Type conversion helpers
- Query builders
- Error formatters
- Common validation logic

**Example:**
```go
package yourdb

import (
    "fmt"
    "strings"
)

// buildSelectQuery constructs a SELECT query
func buildSelectQuery(table string, columns []string, limit int) string {
    cols := "*"
    if len(columns) > 0 {
        cols = strings.Join(columns, ", ")
    }
    return fmt.Sprintf("SELECT %s FROM %s LIMIT %d", cols, table, limit)
}

// convertToGoType converts database types to Go types
func convertToGoType(dbType string) interface{} {
    // Type conversion logic
}

// formatError wraps errors with database context
func formatError(operation string, err error) error {
    return fmt.Errorf("yourdb %s failed: %w", operation, err)
}
```

**Typical Size:** 50-200 lines

---

## Test Files

### Naming Convention

Test files follow Go's standard naming: `<file>_test.go`

**Common Test Files:**
- `connection_test.go` - Connection tests
- `schema_ops_test.go` - Schema operation tests
- `data_ops_test.go` - Data operation tests
- `adapter_test.go` - Adapter integration tests

### Test Structure

```go
package yourdb

import (
    "context"
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/redbco/redb-open/pkg/anchor/adapter"
)

func TestAdapter_Connect(t *testing.T) {
    adapter := NewAdapter()
    
    config := adapter.ConnectionConfig{
        DatabaseID:   "test-db",
        Host:         "localhost",
        Port:         5432,
        Username:     "test",
        Password:     "test",
        DatabaseName: "testdb",
    }
    
    conn, err := adapter.Connect(context.Background(), config)
    assert.NoError(t, err)
    assert.NotNil(t, conn)
    assert.Equal(t, "test-db", conn.ID())
}

func TestDataOps_Fetch(t *testing.T) {
    // Test data fetching
}

func TestSchemaOps_DiscoverSchema(t *testing.T) {
    // Test schema discovery
}
```

---

## File Size Guidelines

| File Type | Typical Size | Complexity |
|-----------|--------------|------------|
| `adapter.go` | 200-400 lines | Medium |
| `init.go` | 10-15 lines | Very Low |
| `connection.go` | 100-300 lines | Medium |
| `schema_ops.go` | 200-600 lines | High |
| `data_ops.go` | 200-500 lines | Medium-High |
| `metadata_ops.go` | 100-300 lines | Low-Medium |
| `replication_ops.go` | 100-500 lines | High (optional) |
| `types.go` | 50-200 lines | Low (optional) |
| `utils.go` | 50-200 lines | Low (optional) |

**Total Expected Size:** 1,000-3,000 lines per adapter

---

## Organization Best Practices

### 1. Separation of Concerns

- **Connection logic** stays in `connection.go`
- **Schema operations** stay in `schema_ops.go`
- **Data operations** stay in `data_ops.go`
- **Metadata** stays in `metadata_ops.go`
- **Replication** stays in `replication_ops.go` (if supported)

### 2. Adapter File Structure

The `adapter.go` file should be lightweight and focused on:
- Implementing adapter interfaces
- Delegating to operation-specific files
- Managing connection lifecycle

**Don't put business logic in `adapter.go`**

### 3. Connection Management

- Always decrypt passwords using `encryption.DecryptPassword()`
- Support SSL/TLS configuration
- Test connections before returning
- Implement proper connection pooling
- Handle connection timeouts

### 4. Error Handling

Use standard adapter error types:
```go
// Connection errors
return adapter.NewConnectionError(dbType, host, port, err)

// Configuration errors
return adapter.NewConfigurationError(dbType, field, reason)

// Unsupported operations
return adapter.NewUnsupportedOperationError(operation, dbType)

// Not found errors
return adapter.NewNotFoundError(resource, identifier)
```

### 5. Type Assertions

Always use comma-ok idiom:
```go
db, ok := client.DB.(*YourClient)
if !ok {
    return adapter.NewConfigurationError(
        dbcapabilities.YourDatabase,
        "connection",
        "invalid connection type",
    )
}
```

### 6. Atomic Operations

Use `sync/atomic` for connection status:
```go
type Connection struct {
    // ... other fields
    connected int32 // Use atomic operations
}

func (c *Connection) IsConnected() bool {
    return atomic.LoadInt32(&c.connected) == 1
}

func (c *Connection) Close() error {
    atomic.StoreInt32(&c.connected, 0)
    return c.db.Close()
}
```

### 7. Context Handling

Always respect context cancellation:
```go
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
    // Check context
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    
    // Perform operation with context
    return d.conn.db.QueryContext(ctx, query)
}
```

---

## Common Patterns

### Pattern 1: Operation Struct

All operation types follow this pattern:

```go
type SchemaOps struct {
    conn *Connection  // Reference to connection
}

func (s *SchemaOps) SomeOperation(ctx context.Context) error {
    // Access database via s.conn.db
    return performOperation(ctx, s.conn.db)
}
```

### Pattern 2: Metadata Collection

Return `map[string]interface{}` for flexibility:

```go
func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
    metadata := make(map[string]interface{})
    
    // Only add if successfully collected
    if version, err := getVersion(ctx); err == nil {
        metadata["version"] = version
    }
    
    return metadata, nil  // Return partial data on error
}
```

### Pattern 3: UnifiedModel Conversion

Convert native schema to UnifiedModel:

```go
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
    // 1. Query native schema
    nativeSchema := queryNativeSchema(ctx, s.conn.db)
    
    // 2. Convert to UnifiedModel
    um := &unifiedmodel.UnifiedModel{
        DatabaseID:   s.conn.id,
        DatabaseType: string(s.conn.Type()),
    }
    
    for _, nativeTable := range nativeSchema.Tables {
        um.Tables = append(um.Tables, convertTable(nativeTable))
    }
    
    return um, nil
}
```

---

## Quick Reference Checklist

When creating a new database adapter, ensure you have:

### Required Files
- [ ] `adapter.go` - Main adapter with Connection types
- [ ] `init.go` - Registry registration
- [ ] `connection.go` - Connection management
- [ ] `schema_ops.go` - Schema operations
- [ ] `data_ops.go` - Data operations
- [ ] `metadata_ops.go` - Metadata collection

### Optional Files
- [ ] `replication_ops.go` - If CDC is supported
- [ ] `types.go` - If custom types are needed
- [ ] `utils.go` - If helper functions are needed
- [ ] `*_test.go` - Unit tests

### Implementation
- [ ] All required interfaces implemented
- [ ] Error handling uses adapter error types
- [ ] Context is respected in all operations
- [ ] Atomic operations for connection state
- [ ] SSL/TLS configuration supported
- [ ] Password decryption handled
- [ ] UnifiedModel conversion correct

### Registration
- [ ] `init()` function registers adapter
- [ ] Blank import added to `main.go`
- [ ] Database type defined in `pkg/dbcapabilities`

---

## Example: Minimal Complete Adapter

Here's the minimum set of files needed for a functional adapter:

```
yourdb/
├── adapter.go           (300 lines) - Adapter, Connection, InstanceConnection
├── init.go              (10 lines)  - Registration
├── connection.go        (150 lines) - Connect, ConnectInstance
├── schema_ops.go        (250 lines) - Schema discovery and creation
├── data_ops.go          (300 lines) - CRUD operations
└── metadata_ops.go      (150 lines) - Metadata collection
```

**Total:** ~1,160 lines for a basic adapter

For databases with advanced features (replication, custom types, etc.), expect 1,500-3,000 lines.

---

## References

- **Adapter Interfaces:** `pkg/anchor/adapter/interface.go`
- **Error Types:** `pkg/anchor/adapter/errors.go`
- **Database Capabilities:** `pkg/dbcapabilities/capabilities.go`
- **UnifiedModel:** `pkg/unifiedmodel/unifiedmodel.go`
- **Implementation Guide:** `docs/ADDING_NEW_DATABASE_SUPPORT.md`

---

## Summary

A well-structured database adapter:

1. ✅ **Follows the standard file organization**
2. ✅ **Separates concerns** (connection, schema, data, metadata)
3. ✅ **Implements all required interfaces**
4. ✅ **Uses standard error types**
5. ✅ **Handles contexts properly**
6. ✅ **Registers automatically via `init()`**
7. ✅ **Is testable and maintainable**

By following this structure, adapters remain consistent, maintainable, and easy to understand across the entire codebase.

---

*Last Updated: October 5, 2025*