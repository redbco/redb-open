# Database Adapter Implementation Guide

This guide provides comprehensive instructions for implementing new database adapters in the reDB system. The reDB architecture uses a standardized adapter pattern that allows for consistent integration of various database technologies while respecting their unique characteristics.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Database Capabilities Matrix](#database-capabilities-matrix)
3. [Required Adapter Structure](#required-adapter-structure)
4. [Core Interface Functions](#core-interface-functions)
5. [Implementation Steps](#implementation-steps)
6. [Testing and Validation](#testing-and-validation)
7. [Integration with Manager](#integration-with-manager)
8. [Best Practices](#best-practices)
9. [Examples](#examples)

## Architecture Overview

The reDB database adapter system is built around several key components:

- **Database Manager** (`services/anchor/internal/database/manager.go`): Central orchestrator for all database operations
- **Common Types** (`services/anchor/internal/database/common/`): Shared interfaces and data structures
- **Database Capabilities** (`pkg/dbcapabilities/`): Metadata about database features and limitations
- **Individual Adapters** (`services/anchor/internal/database/{database_name}/`): Database-specific implementations

### Adapter Responsibilities

Each database adapter is responsible for:
- Connection management (database and instance level)
- Schema discovery and structure operations
- Data operations (CRUD)
- Replication setup and management (if supported)
- Health monitoring and metadata collection

## Database Capabilities Matrix

Before implementing an adapter, you must define the database's capabilities in `pkg/dbcapabilities/capabilities.go`. This includes:

```go
DatabaseType: {
    Name:                     "Your Database Name",
    ID:                       YourDatabaseType,
    HasSystemDatabase:        true/false,
    SystemDatabases:          []string{"system_db_names"},
    SupportsCDC:              true/false,
    CDCMechanisms:            []string{"replication_methods"},
    HasUniqueIdentifier:      true/false,
    SupportsClustering:       true/false,
    ClusteringMechanisms:     []string{"clustering_types"},
    SupportedVendors:         []string{"vendor_names"},
    DefaultPort:              1234,
    DefaultSSLPort:           1234,
    ConnectionStringTemplate: "protocol://{username}:{password}@{host}:{port}/{database}?options",
    Paradigms:                []DataParadigm{ParadigmRelational}, // or other paradigms
    Aliases:                  []string{"alternative_names"},
}
```

### Data Paradigms

Choose the appropriate paradigm(s) for your database:
- `ParadigmRelational`: SQL databases with tables and schemas
- `ParadigmDocument`: Document stores like MongoDB
- `ParadigmKeyValue`: Key-value stores like Redis
- `ParadigmGraph`: Graph databases like Neo4j
- `ParadigmColumnar`: Columnar analytics databases
- `ParadigmWideColumn`: Wide-column stores like Cassandra
- `ParadigmSearchIndex`: Search engines like Elasticsearch
- `ParadigmVector`: Vector databases for AI/ML
- `ParadigmTimeSeries`: Time-series databases
- `ParadigmObjectStore`: Object/blob storage

## Required Adapter Structure

Each adapter must be organized in a dedicated directory under `services/anchor/internal/database/{database_name}/` with the following files:

```
{database_name}/
├── connection.go   # Connection management
├── types.go        # Database-specific types
├── schema.go       # Schema discovery and structure operations
├── data.go         # Data operations (CRUD)
└── replication.go  # Replication management (if supported)
```

## Core Interface Functions

### Connection Management (`connection.go`)

#### Required Functions:

```go
// Connect establishes a connection to a database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error)

// ConnectInstance establishes a connection to a database instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error)

// DiscoverDetails fetches the details of a database
func DiscoverDetails(db interface{}) (*YourDatabaseDetails, error)

// CollectDatabaseMetadata collects metadata from a database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error)

// CollectInstanceMetadata collects metadata from a database instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error)

// ExecuteCommand executes a command on a database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error)

// CreateDatabase creates a new database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error

// DropDatabase drops a database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error
```

#### Connection Configuration

Your `Connect` function should:
1. Decrypt passwords using `encryption.DecryptPassword(config.TenantID, config.Password)`
2. Build appropriate connection strings based on SSL settings
3. Handle SSL certificate configuration
4. Test the connection before returning
5. Return a properly configured `common.DatabaseClient`

### Schema Operations (`schema.go`)

#### Required Functions:

```go
// DiscoverSchema fetches the current schema of a database
func DiscoverSchema(db interface{}) (*YourDatabaseSchema, error)

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(db interface{}, params common.StructureParams) error
```

#### Schema Discovery

Your schema discovery should populate relevant fields from `common.StructureParams`:
- `Tables`: Table information including columns, constraints, indexes
- `Views`: View definitions
- `Functions`: Stored procedures/functions
- `Triggers`: Database triggers
- `Sequences`: Auto-increment sequences
- `Extensions`: Database extensions
- `EnumTypes`: Custom enum types
- `CustomTypes`: User-defined types

### Data Operations (`data.go`)

#### Required Functions:

```go
// FetchData retrieves data from a specified table/collection
func FetchData(db interface{}, tableName string, limit int) ([]map[string]interface{}, error)

// InsertData inserts data into a specified table/collection
func InsertData(db interface{}, tableName string, data []map[string]interface{}) (int64, error)

// UpdateData updates data in a specified table/collection
func UpdateData(db interface{}, tableName string, data []map[string]interface{}, conditions map[string]interface{}) (int64, error)

// DeleteData deletes data from a specified table/collection
func DeleteData(db interface{}, tableName string, conditions map[string]interface{}) (int64, error)
```

### Replication Management (`replication.go`)

If your database supports CDC/replication:

```go
// ConnectReplication creates a new replication client and connection
func ConnectReplication(config common.ReplicationConfig) (*common.ReplicationClient, common.ReplicationSourceInterface, error)

// CreateReplicationSource creates a replication source using an existing database client
func CreateReplicationSource(db interface{}, config common.ReplicationConfig) (common.ReplicationSourceInterface, error)
```

#### Replication Source Interface

Implement `common.ReplicationSourceInterface`:

```go
type YourReplicationSourceDetails struct {
    // Database-specific replication details
    DatabaseID      string
    // ... other fields
}

func (r *YourReplicationSourceDetails) GetSourceID() string
func (r *YourReplicationSourceDetails) GetDatabaseID() string
func (r *YourReplicationSourceDetails) GetStatus() map[string]interface{}
func (r *YourReplicationSourceDetails) Start() error
func (r *YourReplicationSourceDetails) Stop() error
func (r *YourReplicationSourceDetails) IsActive() bool
func (r *YourReplicationSourceDetails) GetMetadata() map[string]interface{}
func (r *YourReplicationSourceDetails) Close() error
```

### Database-Specific Types (`types.go`)

Define your database-specific types:

```go
// YourDatabaseDetails contains information about your database
type YourDatabaseDetails struct {
    UniqueIdentifier string `json:"uniqueIdentifier"`
    DatabaseType     string `json:"databaseType"`
    DatabaseEdition  string `json:"databaseEdition"`
    Version          string `json:"version"`
    DatabaseSize     int64  `json:"databaseSize"`
    // ... other database-specific fields
}

// YourDatabaseSchema represents the schema structure
type YourDatabaseSchema struct {
    Tables    []common.TableInfo          `json:"tables"`
    // ... other schema elements as applicable
}
```

## Implementation Steps

### Step 1: Define Database Capabilities

Add your database to `pkg/dbcapabilities/capabilities.go`:

```go
const (
    YourDatabase DatabaseType = "yourdatabase"
)

var All = map[DatabaseType]Capability{
    // ... existing databases
    YourDatabase: {
        Name: "Your Database",
        ID:   YourDatabase,
        // ... other capabilities
    },
}
```

### Step 2: Create Adapter Directory Structure

```bash
mkdir services/anchor/internal/database/yourdatabase
touch services/anchor/internal/database/yourdatabase/{connection,types,schema,data,replication}.go
```

### Step 3: Implement Connection Management

Start with `connection.go` - this is the foundation:

```go
package yourdatabase

import (
    "context"
    "fmt"
    "github.com/redbco/redb-open/pkg/encryption"
    "github.com/redbco/redb-open/services/anchor/internal/database/common"
    // Your database driver import
)

func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
    // 1. Decrypt password
    var decryptedPassword string
    if config.Password != "" {
        dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
        if err != nil {
            return nil, fmt.Errorf("error decrypting password: %v", err)
        }
        decryptedPassword = dp
    }

    // 2. Build connection string with SSL support
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

    return &common.DatabaseClient{
        DB:           db,
        DatabaseType: "yourdatabase",
        DatabaseID:   config.DatabaseID,
        Config:       config,
        IsConnected:  1,
    }, nil
}
```

### Step 4: Implement Schema Discovery

In `schema.go`, implement schema discovery appropriate for your database type:

```go
func DiscoverSchema(db interface{}) (*YourDatabaseSchema, error) {
    client, ok := db.(*YourDatabaseClient)
    if !ok {
        return nil, fmt.Errorf("invalid database connection type")
    }

    schema := &YourDatabaseSchema{}
    
    // Discover tables/collections
    tables, err := discoverTables(client)
    if err != nil {
        return nil, fmt.Errorf("error discovering tables: %v", err)
    }
    schema.Tables = tables

    // Discover other schema elements as applicable
    // ...

    return schema, nil
}
```

### Step 5: Implement Data Operations

In `data.go`, implement CRUD operations:

```go
func FetchData(db interface{}, tableName string, limit int) ([]map[string]interface{}, error) {
    client, ok := db.(*YourDatabaseClient)
    if !ok {
        return nil, fmt.Errorf("invalid database connection type")
    }

    // Build and execute query
    query := buildSelectQuery(tableName, limit)
    rows, err := client.Query(query)
    if err != nil {
        return nil, fmt.Errorf("error querying table %s: %v", tableName, err)
    }
    defer rows.Close()

    // Process results into standard format
    return processRows(rows)
}
```

### Step 6: Implement Replication (if supported)

If your database supports CDC, implement replication in `replication.go`:

```go
func ConnectReplication(config common.ReplicationConfig) (*common.ReplicationClient, common.ReplicationSourceInterface, error) {
    // Create replication connection
    // Set up CDC mechanisms
    // Return client and source interface
}
```

### Step 7: Register with Manager

Add your adapter to the manager's switch statements in:
- `manager_connect.go` (ConnectDatabase function)
- `manager_structure.go` (GetDatabaseStructure function)
- `manager_data.go` (data operation functions)
- `manager.go` (ExecuteCommand function)

```go
case string(dbcapabilities.YourDatabase):
    client, err = yourdatabase.Connect(config)
```

## Testing and Validation

### Unit Tests

Create comprehensive tests for each adapter function:

```go
func TestConnect(t *testing.T) {
    config := common.DatabaseConfig{
        Host:         "localhost",
        Port:         1234,
        Username:     "test",
        Password:     "test",
        DatabaseName: "test",
        // ... other config
    }

    client, err := Connect(config)
    assert.NoError(t, err)
    assert.NotNil(t, client)
    assert.Equal(t, "yourdatabase", client.DatabaseType)
}
```

### Integration Tests

Test with actual database instances:
- Connection establishment
- Schema discovery accuracy
- Data operations correctness
- Replication functionality (if applicable)

### Compatibility Testing

Verify compatibility with different:
- Database versions
- SSL configurations
- Authentication methods
- Clustering setups

## Integration with Manager

### Manager Registration

The DatabaseManager uses a switch-case pattern to route operations to appropriate adapters. You must add your database to several manager files:

1. **Connection Management** (`manager_connect.go`):
```go
case string(dbcapabilities.YourDatabase):
    client, err = yourdatabase.Connect(config)
```

2. **Schema Operations** (`manager_structure.go`):
```go
case string(dbcapabilities.YourDatabase):
    return yourdatabase.DiscoverSchema(client.DB)
```

3. **Data Operations** (`manager_data.go`):
```go
case string(dbcapabilities.YourDatabase):
    return yourdatabase.FetchData(client.DB, tableName, limit)
```

4. **Command Execution** (`manager.go`):
```go
case string(dbcapabilities.YourDatabase):
    return yourdatabase.ExecuteCommand(context.Background(), client.DB, command)
```

### Import Statements

Add your adapter import to all relevant manager files:

```go
import (
    // ... existing imports
    "github.com/redbco/redb-open/services/anchor/internal/database/yourdatabase"
)
```

## Best Practices

### Error Handling

- Use descriptive error messages with context
- Wrap errors with `fmt.Errorf` to maintain error chains
- Handle database-specific error codes appropriately
- Always clean up resources in error cases

### Connection Management

- Always test connections before returning clients
- Implement proper connection pooling where applicable
- Handle SSL configuration consistently
- Support all authentication methods your database offers

### Data Type Mapping

- Map database-specific types to Go types consistently
- Handle NULL values appropriately
- Preserve precision for numeric types
- Support large objects and binary data

### Performance Considerations

- Use prepared statements where possible
- Implement connection pooling
- Support batch operations
- Optimize schema discovery queries

### Security

- Always use parameterized queries
- Support SSL/TLS encryption
- Handle credentials securely
- Validate input parameters

### Logging

- Use the provided logger for consistent logging
- Log connection attempts, successes, and failures
- Include relevant context in log messages
- Use appropriate log levels

## Examples

### PostgreSQL Adapter Reference

The PostgreSQL adapter (`services/anchor/internal/database/postgres/`) serves as the reference implementation. Key patterns to follow:

1. **Connection String Building**:
```go
fmt.Fprintf(&connString, "postgres://%s:%s@%s:%d/%s",
    config.Username, decryptedPassword, config.Host, config.Port, config.DatabaseName)

if config.SSL {
    sslMode := getSslMode(config)
    fmt.Fprintf(&connString, "?sslmode=%s", sslMode)
}
```

2. **Schema Discovery Pattern**:
```go
func DiscoverSchema(pool *pgxpool.Pool) (*PostgresSchema, error) {
    schema := &PostgresSchema{}
    
    // Discover each schema component
    tables, err := discoverTables(pool)
    if err != nil {
        return nil, fmt.Errorf("error discovering tables: %v", err)
    }
    schema.Tables = tables
    
    return schema, nil
}
```

3. **Data Operation Pattern**:
```go
func FetchData(pool *pgxpool.Pool, tableName string, limit int) ([]map[string]interface{}, error) {
    query := fmt.Sprintf("SELECT * FROM %s", common.QuoteIdentifier(tableName))
    if limit > 0 {
        query += fmt.Sprintf(" LIMIT %d", limit)
    }
    
    rows, err := pool.Query(context.Background(), query)
    if err != nil {
        return nil, fmt.Errorf("error querying table %s: %v", tableName, err)
    }
    defer rows.Close()
    
    return processRows(rows)
}
```

### NoSQL Adapter Example (MongoDB)

For document databases, adapt the patterns:

```go
func DiscoverSchema(db *mongo.Database) (*MongoSchema, error) {
    schema := &MongoSchema{}
    
    // Get collections instead of tables
    collections, err := discoverCollections(db)
    if err != nil {
        return nil, fmt.Errorf("error discovering collections: %v", err)
    }
    
    // Convert to common table format
    for _, collection := range collections {
        table := common.TableInfo{
            Name:    collection.Name,
            Schema:  "default",
            Columns: inferColumnsFromSamples(collection.SampleDocs),
        }
        schema.Tables = append(schema.Tables, table)
    }
    
    return schema, nil
}
```

## Conclusion

Implementing a new database adapter requires careful attention to the established patterns and interfaces. By following this guide and using the PostgreSQL adapter as a reference, you can create robust, consistent database adapters that integrate seamlessly with the reDB system.

Remember to:
- Define capabilities accurately in the capabilities matrix
- Implement all required interface functions
- Handle errors gracefully and consistently
- Test thoroughly with real database instances
- Follow the established patterns for consistency
- Document any database-specific considerations

The modular adapter architecture ensures that new database support can be added without affecting existing functionality, while maintaining a consistent interface for all database operations.
