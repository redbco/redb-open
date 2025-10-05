# Adding New Database Support to ReDB

This document outlines the process for adding support for a new database to the ReDB system using the adapter pattern architecture.

## Overview

Adding a new database to ReDB requires implementing a database adapter that conforms to the standard interfaces. The adapter pattern ensures consistency across all database implementations and makes it easy to add new databases.

**Required Components:**

1. **Database Capabilities** (`pkg/dbcapabilities`) - Define database characteristics and features
2. **Database Adapter** (`services/anchor/internal/database/yourdb/`) - Implement the adapter interfaces
3. **Adapter Registration** (`services/anchor/cmd/main.go`) - Import the adapter to trigger registration
4. **Testing** - Ensure proper integration

## Prerequisites

Before adding a new database, ensure you have:

### Database Knowledge
- [ ] Database documentation for connection protocols and authentication
- [ ] Understanding of the database's data model (relational, document, graph, etc.)
- [ ] Knowledge of schema discovery mechanisms (system tables, APIs, etc.)
- [ ] Understanding of supported data types and their mappings

### Development Environment
- [ ] Access to the database for testing
- [ ] Database driver/client library available in Go
- [ ] Understanding of the database's connection string format
- [ ] SSL/TLS configuration requirements (if applicable)

### ReDB System Knowledge
- [ ] Familiarity with the adapter pattern interfaces (`pkg/anchor/adapter`)
- [ ] Understanding of database capabilities system (`pkg/dbcapabilities`)
- [ ] Knowledge of the UnifiedModel for schema representation (`pkg/unifiedmodel`)

## Step 1: Define Database Capabilities

### 1.1 Add Database Type Constant

In `pkg/dbcapabilities/capabilities.go`, add your database type:

```go
const (
    // Existing databases...
    YourDatabase DatabaseType = "your_database"
)
```

### 1.2 Add to AllSupportedDatabases

```go
var AllSupportedDatabases = []DatabaseType{
    // Existing databases...
    YourDatabase,
}
```

### 1.3 Define Database Capability

Add comprehensive capability information in the `init()` function:

```go
func init() {
    // Existing capabilities...
    
    capabilities[YourDatabase] = Capability{
        Name:                     "Your Database",
        ID:                       YourDatabase,
        HasSystemDatabase:        true,
        SystemDatabases:          []string{"system", "admin"},
        SupportsCDC:              true,
        SupportsInstanceConnect:  true,
        HasUniqueIdentifier:      true,
        SupportsClustering:       false,
        SupportedVendors:         []string{"custom", "cloud-provider"},
        DefaultPort:              5432,
        DefaultSSLPort:           5432,
        ConnectionStringTemplate: "yourdb://{username}:{password}@{host}:{port}/{database}",
        Paradigms:                []DataParadigm{ParadigmRelational}, // or appropriate paradigm
    }
}
```

## Step 2: Create Adapter Structure

Create the adapter directory:

```bash
mkdir services/anchor/internal/database/yourdb
cd services/anchor/internal/database/yourdb
```

Create the following files:

```bash
touch adapter.go           # Main adapter implementation
touch init.go             # Adapter registration
touch connection.go       # Connection management
touch schema_ops.go       # Schema operations
touch data_ops.go         # Data operations  
touch metadata_ops.go     # Metadata collection
touch replication_ops.go  # Replication (optional)
```

## Step 3: Implement Main Adapter (`adapter.go`)

```go
package yourdb

import (
    "context"
    "sync/atomic"

    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
    "github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
    // Your database driver
)

// Adapter implements adapter.DatabaseAdapter for YourDatabase
type Adapter struct{}

// NewAdapter creates a new adapter instance
func NewAdapter() adapter.DatabaseAdapter {
    return &Adapter{}
}

// Type returns the database type identifier
func (a *Adapter) Type() dbcapabilities.DatabaseType {
    return dbcapabilities.YourDatabase
}

// Capabilities returns the capability metadata
func (a *Adapter) Capabilities() dbcapabilities.Capability {
    return dbcapabilities.MustGet(dbcapabilities.YourDatabase)
}

// Connect establishes a database connection
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
    // Convert adapter config to legacy config
    legacyConfig := dbclient.DatabaseConfig{
        DatabaseID:       config.DatabaseID,
        WorkspaceID:      config.WorkspaceID,
        TenantID:         config.TenantID,
        Host:             config.Host,
        Port:             config.Port,
        Username:         config.Username,
        Password:         config.Password,
        DatabaseName:     config.DatabaseName,
        SSL:              config.SSL,
        SSLMode:          config.SSLMode,
        // ... other fields
    }

    // Use existing Connect function
    client, err := Connect(legacyConfig)
    if err != nil {
        return nil, adapter.NewConnectionError(
            dbcapabilities.YourDatabase,
            config.Host,
            config.Port,
            err,
        )
    }

    // Extract the native connection type
    db, ok := client.DB.(*YourDatabaseClient)
    if !ok {
        return nil, adapter.NewConfigurationError(
            dbcapabilities.YourDatabase,
            "connection",
            "invalid connection type",
        )
    }

    conn := &Connection{
        id:        config.DatabaseID,
        db:        db,
        config:    config,
        adapter:   a,
        connected: 1,
    }

    return conn, nil
}

// ConnectInstance establishes an instance connection
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
    // Similar to Connect but returns InstanceConnection
    // Note: Store the client/connection pool, not a specific database
}
```

## Step 4: Implement Connection Types

### Database Connection (`adapter.go` continued)

```go
// Connection implements adapter.Connection for YourDatabase
type Connection struct {
    id        string
    db        *YourDatabaseClient
    config    adapter.ConnectionConfig
    adapter   *Adapter
    connected int32
}

func (c *Connection) ID() string {
    return c.id
}

func (c *Connection) Type() dbcapabilities.DatabaseType {
    return dbcapabilities.YourDatabase
}

func (c *Connection) IsConnected() bool {
    return atomic.LoadInt32(&c.connected) == 1
}

func (c *Connection) Ping(ctx context.Context) error {
    return c.db.Ping(ctx)
}

func (c *Connection) Close() error {
    atomic.StoreInt32(&c.connected, 0)
    return c.db.Close()
}

func (c *Connection) SchemaOperations() adapter.SchemaOperator {
    return &SchemaOps{conn: c}
}

func (c *Connection) DataOperations() adapter.DataOperator {
    return &DataOps{conn: c}
}

func (c *Connection) MetadataOperations() adapter.MetadataOperator {
    return &MetadataOps{conn: c}
}

func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
    // Return actual implementation if CDC is supported
    return adapter.NewUnsupportedReplicationOperator(dbcapabilities.YourDatabase)
}
```

### Instance Connection

```go
// InstanceConnection implements adapter.InstanceConnection
type InstanceConnection struct {
    id        string
    client    *YourDatabaseInstanceClient
    config    adapter.InstanceConfig
    adapter   *Adapter
    connected int32
}

func (i *InstanceConnection) ID() string {
    return i.id
}

func (i *InstanceConnection) Type() dbcapabilities.DatabaseType {
    return dbcapabilities.YourDatabase
}

func (i *InstanceConnection) IsConnected() bool {
    return atomic.LoadInt32(&i.connected) == 1
}

func (i *InstanceConnection) Ping(ctx context.Context) error {
    return i.client.Ping(ctx)
}

func (i *InstanceConnection) Close() error {
    atomic.StoreInt32(&i.connected, 0)
    return i.client.Close()
}

func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
    // Query system tables to list databases
    return listDatabases(i.client)
}

func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
    return createDatabase(i.client, name, options)
}

func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
    return dropDatabase(i.client, name, options)
}

func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
    return &InstanceMetadataOps{conn: i}
}
```

## Step 5: Implement Connection Management (`connection.go`)

```go
package yourdb

import (
    "context"
    "fmt"

    "github.com/redbco/redb-open/pkg/encryption"
    "github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
    // Your database driver
)

// Connect establishes a connection to a specific database
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
    // 1. Decrypt password
    var decryptedPassword string
    if config.Password != "" {
        dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
        if err != nil {
            return nil, fmt.Errorf("error decrypting password: %v", err)
        }
        decryptedPassword = dp
    }

    // 2. Build connection string
    connString := buildConnectionString(config, decryptedPassword)

    // 3. Create database connection
    db, err := yourdriver.Connect(connString)
    if err != nil {
        return nil, fmt.Errorf("error connecting to database: %v", err)
    }

    // 4. Test connection
    if err := db.Ping(); err != nil {
        db.Close()
        return nil, fmt.Errorf("error pinging database: %v", err)
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
    // Similar to Connect, but connects to the instance level
    // Important: Return the client/pool, not a specific database
    
    var decryptedPassword string
    if config.Password != "" {
        dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
        if err != nil {
            return nil, fmt.Errorf("error decrypting password: %v", err)
        }
        decryptedPassword = dp
    }

    connString := buildInstanceConnectionString(config, decryptedPassword)
    client, err := yourdriver.ConnectInstance(connString)
    if err != nil {
        return nil, fmt.Errorf("error connecting to instance: %v", err)
    }

    if err := client.Ping(); err != nil {
        client.Close()
        return nil, fmt.Errorf("error pinging instance: %v", err)
    }

    return &dbclient.InstanceClient{
        DB:           client, // The client/pool, not a specific database
        InstanceType: "yourdb",
        InstanceID:   config.InstanceID,
        Config:       config,
        IsConnected:  1,
    }, nil
}

func buildConnectionString(config dbclient.DatabaseConfig, password string) string {
    // Build connection string with SSL support
    connStr := fmt.Sprintf("yourdb://%s:%s@%s:%d/%s",
        config.Username,
        password,
        config.Host,
        config.Port,
        config.DatabaseName,
    )
    
    if config.SSL {
        connStr += "?ssl=true"
        if config.SSLMode != "" {
            connStr += "&sslmode=" + config.SSLMode
        }
    }
    
    return connStr
}
```

## Step 6: Implement Schema Operations (`schema_ops.go`)

```go
package yourdb

import (
    "context"

    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/unifiedmodel"
)

// SchemaOps implements adapter.SchemaOperator
type SchemaOps struct {
    conn *Connection
}

func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
    // Query system tables to discover schema
    tables, err := discoverTables(ctx, s.conn.db)
    if err != nil {
        return nil, err
    }

    um := &unifiedmodel.UnifiedModel{
        DatabaseID:   s.conn.id,
        DatabaseType: string(s.conn.Type()),
        Tables:       tables,
        // Add other schema elements
    }

    return um, nil
}

func (s *SchemaOps) CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error {
    // Create tables from UnifiedModel
    for _, table := range model.Tables {
        if err := createTable(ctx, s.conn.db, table); err != nil {
            return err
        }
    }
    return nil
}

func (s *SchemaOps) ListTables(ctx context.Context) ([]string, error) {
    return listTableNames(ctx, s.conn.db)
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error) {
    return getTableSchema(ctx, s.conn.db, tableName)
}
```

## Step 7: Implement Data Operations (`data_ops.go`)

```go
package yourdb

import (
    "context"

    "github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DataOps implements adapter.DataOperator
type DataOps struct {
    conn *Connection
}

func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
    return fetchData(ctx, d.conn.db, table, limit)
}

func (d *DataOps) FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error) {
    return fetchDataWithColumns(ctx, d.conn.db, table, columns, limit)
}

func (d *DataOps) Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error) {
    return insertData(ctx, d.conn.db, table, data)
}

func (d *DataOps) Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error) {
    return updateData(ctx, d.conn.db, table, data, whereColumns)
}

func (d *DataOps) Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error) {
    return upsertData(ctx, d.conn.db, table, data, uniqueColumns)
}

func (d *DataOps) Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error) {
    return deleteData(ctx, d.conn.db, table, conditions)
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
    // Implement streaming for large datasets
    return adapter.StreamResult{}, adapter.NewUnsupportedOperationError("Stream", d.conn.Type())
}

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
    return executeQuery(ctx, d.conn.db, query, args...)
}
```

## Step 8: Implement Metadata Operations (`metadata_ops.go`)

```go
package yourdb

import (
    "context"

    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

// MetadataOps implements adapter.MetadataOperator for database connections
type MetadataOps struct {
    conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
    metadata := make(map[string]interface{})
    
    // Collect database version
    version, err := getDatabaseVersion(ctx, m.conn.db)
    if err == nil {
        metadata["version"] = version
    }
    
    // Collect database size
    size, err := getDatabaseSize(ctx, m.conn.db)
    if err == nil {
        metadata["size_bytes"] = size
    }
    
    // Collect table count
    tableCount, err := getTableCount(ctx, m.conn.db)
    if err == nil {
        metadata["table_count"] = tableCount
    }
    
    return metadata, nil
}

func (m *MetadataOps) GetVersion() (string, error) {
    return getDatabaseVersion(context.Background(), m.conn.db)
}

func (m *MetadataOps) GetUniqueIdentifier() string {
    // Return a unique identifier for this database
    return m.conn.config.Host + ":" + string(m.conn.config.Port)
}

// InstanceMetadataOps implements adapter.MetadataOperator for instance connections
type InstanceMetadataOps struct {
    conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
    metadata := make(map[string]interface{})
    
    // Collect instance version
    version, err := getInstanceVersion(ctx, i.conn.client)
    if err == nil {
        metadata["version"] = version
    }
    
    // Collect uptime
    uptime, err := getInstanceUptime(ctx, i.conn.client)
    if err == nil {
        metadata["uptime_seconds"] = uptime
    }
    
    // Collect total databases
    databases, err := i.conn.ListDatabases(ctx)
    if err == nil {
        metadata["total_databases"] = len(databases)
    }
    
    // Collect connection stats
    if stats, err := getConnectionStats(ctx, i.conn.client); err == nil {
        metadata["total_connections"] = stats.Total
        metadata["max_connections"] = stats.Max
    }
    
    return metadata, nil
}

func (i *InstanceMetadataOps) GetVersion() (string, error) {
    return getInstanceVersion(context.Background(), i.conn.client)
}

func (i *InstanceMetadataOps) GetUniqueIdentifier() string {
    return i.conn.config.Host + ":" + string(i.conn.config.Port)
}
```

## Step 9: Implement Replication Operations (Optional, `replication_ops.go`)

If your database supports CDC/replication:

```go
package yourdb

import (
    "context"

    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ReplicationOps implements adapter.ReplicationOperator
type ReplicationOps struct {
    conn *Connection
}

func (r *ReplicationOps) Connect(ctx context.Context, config adapter.ReplicationConfig) (adapter.ReplicationConnection, error) {
    // Implement CDC connection setup
    return nil, adapter.NewUnsupportedOperationError("Replication", dbcapabilities.YourDatabase)
}

func (r *ReplicationOps) ListSlots(ctx context.Context) ([]adapter.ReplicationSlot, error) {
    // List replication slots if supported
    return nil, adapter.NewUnsupportedOperationError("ListSlots", dbcapabilities.YourDatabase)
}

func (r *ReplicationOps) CreateSlot(ctx context.Context, slotName string) error {
    return adapter.NewUnsupportedOperationError("CreateSlot", dbcapabilities.YourDatabase)
}

func (r *ReplicationOps) DropSlot(ctx context.Context, slotName string) error {
    return adapter.NewUnsupportedOperationError("DropSlot", dbcapabilities.YourDatabase)
}

func (r *ReplicationOps) GetStatus(ctx context.Context) (map[string]interface{}, error) {
    return nil, adapter.NewUnsupportedOperationError("GetStatus", dbcapabilities.YourDatabase)
}
```

If replication is not supported, return the unsupported operator:

```go
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
    return adapter.NewUnsupportedReplicationOperator(dbcapabilities.YourDatabase)
}
```

## Step 10: Register Adapter (`init.go`)

Create an `init()` function to automatically register your adapter:

```go
package yourdb

import (
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

func init() {
    // Register adapter with the global registry
    adapter.GlobalRegistry().Register(
        dbcapabilities.YourDatabase,
        NewAdapter(),
    )
}
```

This `init()` function will automatically run when the package is imported.

## Step 11: Import Adapter in Main

Add a blank import to trigger adapter registration in `services/anchor/cmd/main.go`:

```go
import (
    // ... existing imports
    
    // Import all database adapters to trigger their init() registration
    _ "github.com/redbco/redb-open/services/anchor/internal/database/postgres"
    _ "github.com/redbco/redb-open/services/anchor/internal/database/mysql"
    _ "github.com/redbco/redb-open/services/anchor/internal/database/yourdb" // Add your adapter
    // ... other adapters
)
```

The blank import (`_`) ensures the `init()` function runs even though the package isn't directly used.

## Step 12: Testing

### Unit Tests

Create comprehensive tests for your adapter:

```go
// connection_test.go
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

func TestSchemaOps_DiscoverSchema(t *testing.T) {
    // Test schema discovery
}

func TestDataOps_CRUD(t *testing.T) {
    // Test data operations
}
```

### Integration Tests

Test the complete flow:

```bash
# Build the anchor service
make build-anchor

# Run tests
go test ./services/anchor/internal/database/yourdb/... -v

# Test actual connection (requires database)
go test ./services/anchor/internal/database/yourdb/... -v -integration
```

## Best Practices

### 1. Error Handling
- Use the provided error types from `pkg/anchor/adapter`:
  - `adapter.NewConnectionError()` for connection failures
  - `adapter.NewConfigurationError()` for configuration issues
  - `adapter.NewUnsupportedOperationError()` for unsupported features
- Always wrap errors with context: `fmt.Errorf("context: %w", err)`

### 2. Connection Management
- Always test connections with `Ping()` before returning
- Implement proper connection cleanup in `Close()`
- Handle SSL/TLS configuration consistently
- Use connection pooling where appropriate

### 3. Schema Discovery
- Query system tables efficiently
- Cache schema information when possible
- Handle database-specific schema structures (schemas, databases, namespaces)
- Convert native types to `unifiedmodel.Table` format

### 4. Data Operations
- Use parameterized queries to prevent SQL injection
- Handle NULL values appropriately
- Support batch operations for performance
- Implement proper type conversions

### 5. Metadata Collection
- Collect only essential metadata to avoid performance impact
- Handle errors gracefully (return partial data if needed)
- Use appropriate data types (int64 for sizes, int for counts)

### 6. Type Assertions
- Always use type assertions with the comma-ok idiom:
  ```go
  db, ok := client.DB.(*YourClient)
  if !ok {
      return adapter.NewConfigurationError(...)
  }
  ```

### 7. Instance vs Database Connections
- **Database Connection**: Connects to a specific database, stores database handle
- **Instance Connection**: Connects to the database server, stores client/pool that can access multiple databases

### 8. Atomic Operations
- Use `sync/atomic` for connection status flags
- Ensure thread-safe operations where needed

## Common Pitfalls

❌ **Storing database handle in instance connection**
```go
// WRONG - Don't do this for instance connections
return &dbclient.InstanceClient{
    DB: client.Database("specific_db"), // ❌ Tied to one database
}
```

✅ **Store the client/pool**
```go
// CORRECT - Store the client that can access all databases
return &dbclient.InstanceClient{
    DB: client, // ✅ Can access any database
}
```

❌ **Not handling nil pointers**
```go
// WRONG
metadata["size"] = getSize() // getSize() might return nil
```

✅ **Check before adding**
```go
// CORRECT
if size, err := getSize(); err == nil {
    metadata["size"] = size
}
```

❌ **Forgetting to register the adapter**
```go
// WRONG - No init() function means adapter won't be registered
```

✅ **Always include init.go**
```go
// CORRECT
func init() {
    adapter.GlobalRegistry().Register(
        dbcapabilities.YourDatabase,
        NewAdapter(),
    )
}
```

## Validation Checklist

Before submitting your adapter:

### Implementation
- [ ] Adapter implements all required interfaces
- [ ] `init.go` registers the adapter
- [ ] Connection and InstanceConnection types implemented
- [ ] All operation types implemented (Schema, Data, Metadata)
- [ ] Replication operations implemented or marked as unsupported
- [ ] Proper error handling using adapter error types
- [ ] Type assertions use comma-ok idiom

### Registration
- [ ] Database type added to `pkg/dbcapabilities`
- [ ] Capability information complete and accurate
- [ ] Adapter imported in `services/anchor/cmd/main.go`
- [ ] Init function registers adapter correctly

### Testing
- [ ] Unit tests for all operations
- [ ] Connection tests pass
- [ ] Schema discovery works correctly
- [ ] Data operations (CRUD) work correctly
- [ ] Metadata collection returns expected data
- [ ] Integration tests pass (if applicable)

### Code Quality
- [ ] No database-specific imports outside adapter package
- [ ] Follows existing adapter patterns
- [ ] Code is well-documented
- [ ] Error messages are descriptive
- [ ] No hardcoded values (use config)

## Example: Complete Minimal Adapter

Here's a minimal but complete adapter example:

```go
// adapter.go
package mydb

import (
    "context"
    "sync/atomic"
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
    "github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

type Adapter struct{}

func NewAdapter() adapter.DatabaseAdapter {
    return &Adapter{}
}

func (a *Adapter) Type() dbcapabilities.DatabaseType {
    return dbcapabilities.MyDB
}

func (a *Adapter) Capabilities() dbcapabilities.Capability {
    return dbcapabilities.MustGet(dbcapabilities.MyDB)
}

func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
    legacyConfig := convertToLegacyConfig(config)
    client, err := Connect(legacyConfig)
    if err != nil {
        return nil, adapter.NewConnectionError(dbcapabilities.MyDB, config.Host, config.Port, err)
    }
    
    db, ok := client.DB.(*MyDBClient)
    if !ok {
        return nil, adapter.NewConfigurationError(dbcapabilities.MyDB, "connection", "invalid type")
    }
    
    return &Connection{
        id: config.DatabaseID,
        db: db,
        config: config,
        adapter: a,
        connected: 1,
    }, nil
}

func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
    // Similar implementation for instance connection
}

type Connection struct {
    id        string
    db        *MyDBClient
    config    adapter.ConnectionConfig
    adapter   *Adapter
    connected int32
}

func (c *Connection) ID() string { return c.id }
func (c *Connection) Type() dbcapabilities.DatabaseType { return dbcapabilities.MyDB }
func (c *Connection) IsConnected() bool { return atomic.LoadInt32(&c.connected) == 1 }
func (c *Connection) Ping(ctx context.Context) error { return c.db.Ping(ctx) }
func (c *Connection) Close() error {
    atomic.StoreInt32(&c.connected, 0)
    return c.db.Close()
}

func (c *Connection) SchemaOperations() adapter.SchemaOperator {
    return &SchemaOps{conn: c}
}

func (c *Connection) DataOperations() adapter.DataOperator {
    return &DataOps{conn: c}
}

func (c *Connection) MetadataOperations() adapter.MetadataOperator {
    return &MetadataOps{conn: c}
}

func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
    return adapter.NewUnsupportedReplicationOperator(dbcapabilities.MyDB)
}
```

```go
// init.go
package mydb

import (
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

func init() {
    adapter.GlobalRegistry().Register(dbcapabilities.MyDB, NewAdapter())
}
```

## Support

For questions or issues:

1. **Reference Implementations**: Study the PostgreSQL and MySQL adapters as examples
2. **Adapter Interfaces**: Review `pkg/anchor/adapter/interface.go` for complete interface definitions
3. **Error Types**: Check `pkg/anchor/adapter/errors.go` for standard error handling
4. **Capabilities**: See `pkg/dbcapabilities/capabilities.go` for capability definitions

## Summary

The adapter pattern provides a clean, maintainable way to add database support to ReDB:

1. **Define capabilities** in `pkg/dbcapabilities`
2. **Implement adapter** in `services/anchor/internal/database/yourdb/`
3. **Register adapter** via `init()` function
4. **Import adapter** in `main.go` to trigger registration
5. **Test thoroughly** to ensure correct operation

The adapter pattern ensures consistency, maintainability, and easy extensibility across all supported databases.