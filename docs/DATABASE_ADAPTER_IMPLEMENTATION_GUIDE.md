# Database Adapter Implementation Guide

This guide provides comprehensive instructions for implementing new database adapters in the reDB system. The reDB architecture uses a standardized adapter pattern with well-defined interfaces that allows for consistent integration of various database technologies while respecting their unique characteristics.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Database Capabilities](#database-capabilities)
3. [Adapter Interfaces](#adapter-interfaces)
4. [Required File Structure](#required-file-structure)
5. [Implementation Steps](#implementation-steps)
6. [Testing and Validation](#testing-and-validation)
7. [Best Practices](#best-practices)
8. [Examples](#examples)

## Architecture Overview

The reDB database adapter system is built around several key components:

- **Adapter Registry** (`pkg/anchor/adapter/registry.go`): Global registry for all database adapters
- **Adapter Interfaces** (`pkg/anchor/adapter/interface.go`): Standard interfaces that all adapters must implement
- **Database Capabilities** (`pkg/dbcapabilities/`): Metadata about database features and limitations
- **Individual Adapters** (`services/anchor/internal/database/{database_name}/`): Database-specific implementations
- **Connection Manager** (`services/anchor/internal/database/connection_manager.go`): Manages adapter connections

### Adapter Pattern Benefits

The adapter pattern provides:
- **Consistency**: All databases are accessed through the same interfaces
- **Modularity**: Each database is self-contained and independent
- **Extensibility**: New databases can be added without modifying existing code
- **Automatic Registration**: Adapters register themselves via `init()` functions
- **Type Safety**: Interfaces ensure all required methods are implemented

### Adapter Responsibilities

Each database adapter implements interfaces for:
- **Connection Management**: Database and instance-level connections
- **Schema Operations**: Discovery and structure manipulation
- **Data Operations**: CRUD operations and queries
- **Metadata Collection**: Version, size, and statistics
- **Replication**: CDC and change streaming (optional)

## Database Capabilities

Before implementing an adapter, define the database's capabilities in `pkg/dbcapabilities/capabilities.go`:

```go
func init() {
    capabilities[YourDatabase] = Capability{
        Name:                     "Your Database",
        ID:                       YourDatabase,
        HasSystemDatabase:        true,
        SystemDatabases:          []string{"system", "admin"},
        SupportsCDC:              true,
        CDCMechanisms:            []string{"logical-replication"},
        SupportsInstanceConnect:  true,
        HasUniqueIdentifier:      true,
        SupportsClustering:       false,
        SupportedVendors:         []string{"custom", "cloud"},
        DefaultPort:              5432,
        DefaultSSLPort:           5432,
        ConnectionStringTemplate: "yourdb://{username}:{password}@{host}:{port}/{database}",
        Paradigms:                []DataParadigm{ParadigmRelational},
    }
}
```

### Data Paradigms

Choose the appropriate paradigm(s):
- `ParadigmRelational`: SQL databases (PostgreSQL, MySQL, etc.)
- `ParadigmDocument`: Document stores (MongoDB, CouchDB)
- `ParadigmKeyValue`: Key-value stores (Redis, Memcached)
- `ParadigmGraph`: Graph databases (Neo4j, ArangoDB)
- `ParadigmColumnar`: Columnar databases (ClickHouse, Snowflake)
- `ParadigmWideColumn`: Wide-column stores (Cassandra, HBase)
- `ParadigmSearchIndex`: Search engines (Elasticsearch, Meilisearch)
- `ParadigmVector`: Vector databases (Pinecone, Milvus, Weaviate)
- `ParadigmTimeSeries`: Time-series databases (InfluxDB, TimescaleDB)
- `ParadigmObjectStore`: Object storage (S3, MinIO)

## Adapter Interfaces

All adapters must implement these core interfaces from `pkg/anchor/adapter/interface.go`:

### 1. DatabaseAdapter Interface

```go
type DatabaseAdapter interface {
    // Type returns the database type identifier
    Type() dbcapabilities.DatabaseType
    
    // Capabilities returns the capability metadata
    Capabilities() dbcapabilities.Capability
    
    // Connect establishes a database connection
    Connect(ctx context.Context, config ConnectionConfig) (Connection, error)
    
    // ConnectInstance establishes an instance connection
    ConnectInstance(ctx context.Context, config InstanceConfig) (InstanceConnection, error)
}
```

### 2. Connection Interface

```go
type Connection interface {
    // Identity
    ID() string
    Type() dbcapabilities.DatabaseType
    
    // Lifecycle
    IsConnected() bool
    Ping(ctx context.Context) error
    Close() error
    
    // Operations
    SchemaOperations() SchemaOperator
    DataOperations() DataOperator
    MetadataOperations() MetadataOperator
    ReplicationOperations() ReplicationOperator
}
```

### 3. InstanceConnection Interface

```go
type InstanceConnection interface {
    // Identity
    ID() string
    Type() dbcapabilities.DatabaseType
    
    // Lifecycle
    IsConnected() bool
    Ping(ctx context.Context) error
    Close() error
    
    // Database Management
    ListDatabases(ctx context.Context) ([]string, error)
    CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error
    DropDatabase(ctx context.Context, name string, options map[string]interface{}) error
    
    // Metadata
    MetadataOperations() MetadataOperator
}
```

### 4. SchemaOperator Interface

```go
type SchemaOperator interface {
    // Discovery
    DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error)
    ListTables(ctx context.Context) ([]string, error)
    GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error)
    
    // Structure Management
    CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error
}
```

### 5. DataOperator Interface

```go
type DataOperator interface {
    // Read Operations
    Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error)
    FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error)
    
    // Write Operations
    Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error)
    Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error)
    Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error)
    Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error)
    
    // Query Execution
    ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error)
    
    // Streaming (optional)
    Stream(ctx context.Context, params StreamParams) (StreamResult, error)
}
```

### 6. MetadataOperator Interface

```go
type MetadataOperator interface {
    // Collection
    CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error)
    CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error)
    
    // Version & Identity
    GetVersion() (string, error)
    GetUniqueIdentifier() string
}
```

### 7. ReplicationOperator Interface (Optional)

```go
type ReplicationOperator interface {
    // Connection
    Connect(ctx context.Context, config ReplicationConfig) (ReplicationConnection, error)
    
    // Slot Management
    ListSlots(ctx context.Context) ([]ReplicationSlot, error)
    CreateSlot(ctx context.Context, slotName string) error
    DropSlot(ctx context.Context, slotName string) error
    
    // Status
    GetStatus(ctx context.Context) (map[string]interface{}, error)
}
```

## Required File Structure

Each adapter must be organized in `services/anchor/internal/database/{database_name}/`:

```
{database_name}/
├── adapter.go           # Main adapter & Connection types
├── init.go              # Auto-registration
├── connection.go        # Connection management
├── schema_ops.go        # SchemaOperator implementation
├── data_ops.go          # DataOperator implementation
├── metadata_ops.go      # MetadataOperator implementation
├── replication_ops.go   # ReplicationOperator (optional)
├── types.go             # Database-specific types (optional)
├── utils.go             # Helper functions (optional)
└── *_test.go            # Tests
```

See `docs/DATABASE_ADAPTER_FILE_STRUCTURE.md` for detailed file descriptions.

## Implementation Steps

### Step 1: Define Database Capabilities

Add your database constant and capabilities to `pkg/dbcapabilities/capabilities.go`:

```go
const (
    // ... existing databases
    YourDatabase DatabaseType = "yourdb"
)

var AllSupportedDatabases = []DatabaseType{
    // ... existing databases
    YourDatabase,
}

func init() {
    capabilities[YourDatabase] = Capability{
        Name:             "Your Database",
        ID:               YourDatabase,
        DefaultPort:      5432,
        // ... other capabilities
    }
}
```

### Step 2: Create Adapter Directory

```bash
mkdir services/anchor/internal/database/yourdb
cd services/anchor/internal/database/yourdb
touch adapter.go init.go connection.go schema_ops.go data_ops.go metadata_ops.go
```

### Step 3: Implement Main Adapter (`adapter.go`)

```go
package yourdb

import (
    "context"
    "sync/atomic"
    
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
    "github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
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
    // Convert to legacy config
    legacyConfig := dbclient.DatabaseConfig{
        DatabaseID:   config.DatabaseID,
        Host:         config.Host,
        Port:         config.Port,
        Username:     config.Username,
        Password:     config.Password,
        DatabaseName: config.DatabaseName,
        SSL:          config.SSL,
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

    // Extract native connection
    db, ok := client.DB.(*YourDatabaseClient)
    if !ok {
        return nil, adapter.NewConfigurationError(
            dbcapabilities.YourDatabase,
            "connection",
            "invalid connection type",
        )
    }

    return &Connection{
        id:        config.DatabaseID,
        db:        db,
        config:    config,
        adapter:   a,
        connected: 1,
    }, nil
}

func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
    // Similar implementation for instance connection
    // Important: Store client/pool, not a specific database
}

// Connection implements adapter.Connection
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
    // Return actual implementation or unsupported
    return adapter.NewUnsupportedReplicationOperator(dbcapabilities.YourDatabase)
}

// InstanceConnection implements adapter.InstanceConnection
type InstanceConnection struct {
    id        string
    client    *YourInstanceClient
    config    adapter.InstanceConfig
    adapter   *Adapter
    connected int32
}

// Implement InstanceConnection methods...
```

### Step 4: Implement Registration (`init.go`)

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

### Step 5: Implement Connection Management (`connection.go`)

```go
package yourdb

import (
    "fmt"
    
    "github.com/redbco/redb-open/pkg/encryption"
    "github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
    // Your database driver
)

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

    // 3. Connect to database
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

func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
    // Similar implementation
    // Important: Return client/pool, NOT a specific database
}

func buildConnectionString(config dbclient.DatabaseConfig, password string) string {
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

### Step 6: Implement Schema Operations (`schema_ops.go`)

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
    tables, err := discoverTables(ctx, s.conn.db)
    if err != nil {
        return nil, err
    }

    um := &unifiedmodel.UnifiedModel{
        DatabaseID:   s.conn.id,
        DatabaseType: string(s.conn.Type()),
        Tables:       tables,
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

### Step 7: Implement Data Operations (`data_ops.go`)

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

func (d *DataOps) ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error) {
    return executeQuery(ctx, d.conn.db, query, args...)
}

func (d *DataOps) Stream(ctx context.Context, params adapter.StreamParams) (adapter.StreamResult, error) {
    // Implement streaming or return unsupported
    return adapter.StreamResult{}, adapter.NewUnsupportedOperationError("Stream", d.conn.Type())
}
```

### Step 8: Implement Metadata Operations (`metadata_ops.go`)

```go
package yourdb

import (
    "context"
)

type MetadataOps struct {
    conn *Connection
}

func (m *MetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
    metadata := make(map[string]interface{})
    
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

func (m *MetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
    // Not used for database connections
    return nil, adapter.NewUnsupportedOperationError("CollectInstanceMetadata", m.conn.Type())
}

func (m *MetadataOps) GetVersion() (string, error) {
    return getDatabaseVersion(context.Background(), m.conn.db)
}

func (m *MetadataOps) GetUniqueIdentifier() string {
    return m.conn.config.Host + ":" + string(m.conn.config.Port)
}

// InstanceMetadataOps for instance connections
type InstanceMetadataOps struct {
    conn *InstanceConnection
}

func (i *InstanceMetadataOps) CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error) {
    // Not used for instance connections
    return nil, adapter.NewUnsupportedOperationError("CollectDatabaseMetadata", i.conn.Type())
}

func (i *InstanceMetadataOps) CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error) {
    metadata := make(map[string]interface{})
    
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
    return getInstanceVersion(context.Background(), i.conn.client)
}

func (i *InstanceMetadataOps) GetUniqueIdentifier() string {
    return i.conn.config.Host + ":" + string(i.conn.config.Port)
}
```

### Step 9: Add Blank Import to `main.go`

In `services/anchor/cmd/main.go`, add your adapter to trigger registration:

```go
import (
    // ... existing imports
    
    // Import all database adapters to trigger their init() registration
    _ "github.com/redbco/redb-open/services/anchor/internal/database/postgres"
    _ "github.com/redbco/redb-open/services/anchor/internal/database/mysql"
    _ "github.com/redbco/redb-open/services/anchor/internal/database/yourdb" // Add here
    // ... other adapters
)
```

## Testing and Validation

### Unit Tests

Create comprehensive tests for each component:

```go
// adapter_test.go
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

// schema_ops_test.go
func TestSchemaOps_DiscoverSchema(t *testing.T) {
    // Test schema discovery
}

// data_ops_test.go
func TestDataOps_CRUD(t *testing.T) {
    // Test data operations
}
```

### Integration Tests

Test with real database instances:

```bash
# Set up test database
docker run -d -p 5432:5432 your-database:latest

# Run tests
go test ./services/anchor/internal/database/yourdb/... -v

# Run integration tests
go test ./services/anchor/internal/database/yourdb/... -v -tags=integration
```

### Validation Checklist

- [ ] All adapter interfaces implemented
- [ ] `init()` function registers adapter
- [ ] Blank import added to `main.go`
- [ ] Connection tests pass
- [ ] Schema discovery works
- [ ] Data operations work
- [ ] Metadata collection works
- [ ] Error handling uses adapter error types
- [ ] Type assertions use comma-ok idiom
- [ ] Context is respected in all operations

## Best Practices

### 1. Error Handling

Use adapter error types:

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

### 2. Type Assertions

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

### 3. Context Handling

Respect context cancellation:

```go
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    
    // Perform operation with context
    return d.conn.db.QueryContext(ctx, query)
}
```

### 4. Atomic Operations

Use `sync/atomic` for connection state:

```go
type Connection struct {
    connected int32  // Use atomic operations
}

func (c *Connection) IsConnected() bool {
    return atomic.LoadInt32(&c.connected) == 1
}

func (c *Connection) Close() error {
    atomic.StoreInt32(&c.connected, 0)
    return c.db.Close()
}
```

### 5. Instance vs Database Connections

- **Database Connection**: Connects to specific database, stores database handle
- **Instance Connection**: Connects to server, stores client/pool that can access multiple databases

```go
// WRONG for instance connection
return &dbclient.InstanceClient{
    DB: client.Database("specific_db"),  // ❌ Tied to one database
}

// CORRECT for instance connection
return &dbclient.InstanceClient{
    DB: client,  // ✅ Can access any database
}
```

### 6. Security

- Always use parameterized queries
- Support SSL/TLS encryption
- Handle credentials securely via `encryption.DecryptPassword()`
- Validate all input parameters

### 7. Performance

- Use prepared statements where possible
- Implement connection pooling
- Support batch operations
- Optimize schema discovery queries

### 8. Logging

- Use appropriate log levels
- Include relevant context
- Log connection attempts and failures
- Don't log sensitive data (passwords, keys)

## Examples

### PostgreSQL Adapter (Reference Implementation)

The PostgreSQL adapter serves as the reference. Key patterns:

**Connection String Building:**
```go
fmt.Fprintf(&connString, "postgres://%s:%s@%s:%d/%s",
    config.Username, decryptedPassword, config.Host, config.Port, config.DatabaseName)

if config.SSL {
    sslMode := getSslMode(config)
    fmt.Fprintf(&connString, "?sslmode=%s", sslMode)
}
```

**Schema Discovery:**
```go
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
    tables, err := discoverTables(ctx, s.conn.pool)
    if err != nil {
        return nil, fmt.Errorf("error discovering tables: %v", err)
    }
    
    um := &unifiedmodel.UnifiedModel{
        DatabaseID:   s.conn.id,
        DatabaseType: "postgres",
        Tables:       tables,
    }
    
    return um, nil
}
```

**Data Operations:**
```go
func (d *DataOps) Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error) {
    query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", quoteIdentifier(table), limit)
    
    rows, err := d.conn.pool.Query(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("error querying table: %v", err)
    }
    defer rows.Close()
    
    return processRows(rows)
}
```

### MongoDB Adapter (NoSQL Example)

For document databases:

**Schema Discovery:**
```go
func (s *SchemaOps) DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error) {
    collections, err := discoverCollections(ctx, s.conn.db)
    if err != nil {
        return nil, err
    }
    
    um := &unifiedmodel.UnifiedModel{
        DatabaseID:   s.conn.id,
        DatabaseType: "mongodb",
    }
    
    // Convert collections to UnifiedModel format
    for _, coll := range collections {
        table := &unifiedmodel.Table{
            Name:    coll.Name,
            Columns: inferColumnsFromSamples(coll.SampleDocs),
        }
        um.Tables = append(um.Tables, table)
    }
    
    return um, nil
}
```

## Conclusion

The adapter pattern provides a clean, maintainable architecture for database support. By implementing the standard interfaces and following established patterns, new databases integrate seamlessly with reDB.

### Key Takeaways

1. **Implement Standard Interfaces**: All required interfaces must be implemented
2. **Automatic Registration**: Use `init()` for self-registration
3. **Consistent Error Handling**: Use adapter error types
4. **Type Safety**: Always use comma-ok idiom for type assertions
5. **Context Awareness**: Respect context cancellation
6. **Security First**: Handle credentials and queries securely
7. **Test Thoroughly**: Unit and integration tests are essential

### Next Steps

1. Study the PostgreSQL adapter as a reference
2. Review `docs/ADDING_NEW_DATABASE_SUPPORT.md` for step-by-step guide
3. Check `docs/DATABASE_ADAPTER_FILE_STRUCTURE.md` for file organization
4. Start implementing your adapter!

The modular adapter architecture ensures that new database support can be added without affecting existing functionality, while maintaining consistency across all database operations.

---

*For questions or issues, refer to existing adapter implementations or consult the development team.*