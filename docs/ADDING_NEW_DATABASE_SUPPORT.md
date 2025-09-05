# Adding New Database Support to ReDB

This document outlines the comprehensive process for adding support for a new database to the ReDB system. The process involves multiple components and requires careful attention to type conversion compatibility.

## Overview

Adding a new database to ReDB requires updates to four main areas:
1. **Database Capabilities** (`pkg/dbcapabilities`) - Define the database's fundamental characteristics
2. **Unified Model** (`pkg/unifiedmodel`) - Add type conversion and feature metadata
3. **Database Adapter** (`services/anchor/internal/database`) - Implement connectivity and operations
4. **Testing** - Ensure proper integration across all components

The process involves both the **type conversion system** (for schema translation) and the **anchor service** (for actual database connectivity). Both components must work together to provide complete database support.

## Prerequisites

Before adding a new database, ensure you have:

### Database Knowledge
- [ ] Database documentation for supported data types and their precision/scale limits
- [ ] Understanding of the database's paradigm (Relational, Document, Graph, etc.)
- [ ] Knowledge of the database's unique features and constraints
- [ ] Understanding of the database's connection protocols and authentication methods
- [ ] Knowledge of the database's schema discovery mechanisms (system tables, APIs, etc.)

### Development Environment
- [ ] Access to the database for testing (recommended for thorough validation)
- [ ] Database driver/client library available in Go
- [ ] Understanding of the database's connection string format
- [ ] SSL/TLS configuration requirements (if applicable)

### reDB System Knowledge
- [ ] Familiarity with the unified type conversion system
- [ ] Understanding of the anchor service adapter pattern
- [ ] Knowledge of the database manager registration process
- [ ] Understanding of reDB's testing framework and patterns

## Step 1: Database Capabilities (`pkg/dbcapabilities/capabilities.go`)

### 1.1 Add Database Type Constant

Add the new database to the `DatabaseType` constants:

```go
const (
    // Existing databases...
    YourNewDatabase DatabaseType = "your_new_database"
)
```

### 1.2 Update Database Lists

Add the database to the appropriate category list:

```go
var (
    RelationalDatabases = []DatabaseType{
        // Add here if relational
        YourNewDatabase,
    }
    
    DocumentDatabases = []DatabaseType{
        // Add here if document-based
        YourNewDatabase,
    }
    
    // Add to other categories as appropriate
)
```

### 1.3 Add to AllSupportedDatabases

```go
var AllSupportedDatabases = []DatabaseType{
    // Existing databases...
    YourNewDatabase,
}
```

### 1.4 Define Database Paradigms

Add paradigm mapping in the `GetDatabaseParadigms` function:

```go
func GetDatabaseParadigms(dbType DatabaseType) []DataParadigm {
    switch dbType {
    // Existing cases...
    case YourNewDatabase:
        return []DataParadigm{ParadigmRelational} // or appropriate paradigm(s)
    }
}
```

### 1.5 Add Feature Support

Update the `GetDatabaseFeatures` function:

```go
func GetDatabaseFeatures(dbType DatabaseType) DatabaseFeatures {
    switch dbType {
    // Existing cases...
    case YourNewDatabase:
        return DatabaseFeatures{
            SupportsTransactions:    true,
            SupportsACID:           true,
            SupportsIndexes:        true,
            SupportsConstraints:    true,
            SupportsStoredProcedures: false,
            SupportsTriggers:       false,
            SupportsViews:          true,
            SupportsPartitioning:   false,
            SupportsReplication:    true,
            SupportsSharding:       false,
            SupportsJSON:           false,
            SupportsXML:            false,
            SupportsFullTextSearch: false,
            SupportsGIS:            false,
            MaxConnections:         1000,
            MaxDatabaseSize:        "unlimited",
            MaxTableSize:           "256TB",
            MaxIndexSize:           "32TB",
            MaxRowSize:             "1GB",
        }
    }
}
```

## Step 2: Unified Model Database Metadata (`pkg/unifiedmodel/database_metadata_registry.go`)

### 2.1 Add Metadata Creation Function

Create a comprehensive metadata function for your database:

```go
func (stc *ScalableTypeConverter) createYourNewDatabaseMetadata() DatabaseTypeMetadata {
    return DatabaseTypeMetadata{
        DatabaseType: dbcapabilities.YourNewDatabase,
        PrimitiveTypes: map[string]PrimitiveTypeInfo{
            // Define all native data types
            "integer": {
                NativeName:       "integer",
                UnifiedType:      UnifiedTypeInt32,
                HasLength:        false,
                HasPrecision:     false,
                HasScale:         false,
                SupportsNull:     true,
                SupportsDefault:  true,
                DefaultValue:     func() *string { v := "0"; return &v }(),
                Aliases:          []string{"int"},
            },
            "bigint": {
                NativeName:       "bigint",
                UnifiedType:      UnifiedTypeInt64,
                SupportsNull:     true,
                SupportsDefault:  true,
                Aliases:          []string{"long"},
            },
            "varchar": {
                NativeName:       "varchar",
                UnifiedType:      UnifiedTypeVarchar,
                HasLength:        true,
                MaxLength:        func() *int64 { v := int64(65535); return &v }(),
                DefaultLength:    func() *int64 { v := int64(255); return &v }(),
                SupportsNull:     true,
                SupportsDefault:  true,
            },
            "text": {
                NativeName:       "text",
                UnifiedType:      UnifiedTypeString,
                HasLength:        false,
                SupportsNull:     true,
                SupportsDefault:  false,
            },
            "decimal": {
                NativeName:       "decimal",
                UnifiedType:      UnifiedTypeDecimal,
                HasPrecision:     true,
                HasScale:         true,
                DefaultPrecision: func() *int64 { v := int64(10); return &v }(),
                MaxPrecision:     func() *int64 { v := int64(38); return &v }(),
                DefaultScale:     func() *int64 { v := int64(0); return &v }(),
                MaxScale:         func() *int64 { v := int64(38); return &v }(),
                SupportsNull:     true,
                SupportsDefault:  true,
                Aliases:          []string{"numeric"},
            },
            "boolean": {
                NativeName:       "boolean",
                UnifiedType:      UnifiedTypeBoolean,
                SupportsNull:     true,
                SupportsDefault:  true,
                DefaultValue:     func() *string { v := "false"; return &v }(),
                Aliases:          []string{"bool"},
            },
            "timestamp": {
                NativeName:       "timestamp",
                UnifiedType:      UnifiedTypeTimestamp,
                SupportsNull:     true,
                SupportsDefault:  true,
                HasTimezone:      true,
            },
            "date": {
                NativeName:       "date",
                UnifiedType:      UnifiedTypeDate,
                SupportsNull:     true,
                SupportsDefault:  true,
            },
            // Add more types as needed...
        },
        CustomTypeSupport: CustomTypeSupportInfo{
            SupportsEnum:      true,
            SupportsComposite: false,
            SupportsDomain:    true,
            SupportsArray:     true,
            SupportsJSON:      true,
            EnumImplementation: CustomTypeImplementation{
                IsNative: true,
                Syntax:   "CREATE TYPE enum_name AS ENUM ('value1', 'value2')",
            },
            ArrayImplementation: CustomTypeImplementation{
                IsNative: true,
                Syntax:   "column_name datatype[]",
            },
            JSONImplementation: CustomTypeImplementation{
                IsNative: true,
                Syntax:   "column_name JSON",
            },
        },
        ConstraintSupport: ConstraintSupportInfo{
            SupportsPrimaryKey:    true,
            SupportsForeignKey:    true,
            SupportsUnique:        true,
            SupportsCheck:         true,
            SupportsNotNull:       true,
            SupportsDefault:       true,
            SupportsAutoIncrement: true,
        },
        DefaultMappings: map[UnifiedDataType]string{
            // Map unified types to native types
            UnifiedTypeInt32:     "integer",
            UnifiedTypeInt64:     "bigint",
            UnifiedTypeFloat32:   "real",
            UnifiedTypeFloat64:   "double",
            UnifiedTypeString:    "text",
            UnifiedTypeVarchar:   "varchar",
            UnifiedTypeBoolean:   "boolean",
            UnifiedTypeDate:      "date",
            UnifiedTypeTimestamp: "timestamp",
            UnifiedTypeDecimal:   "decimal",
            UnifiedTypeJSON:      "json",
            UnifiedTypeArray:     "text", // or native array syntax
            UnifiedTypeBinary:    "bytea",
            UnifiedTypeUUID:      "uuid",
        },
    }
}
```

### 2.2 Register the Metadata

Add the database to the `initializeMetadata` function:

```go
func (stc *ScalableTypeConverter) initializeMetadata() {
    // Existing databases...
    stc.metadata[dbcapabilities.YourNewDatabase] = stc.createYourNewDatabaseMetadata()
}
```

## Step 3: Database Features (`pkg/unifiedmodel/database_features.go`)

### 3.1 Add Feature Definitions

Add comprehensive feature support information:

```go
func (um *UnifiedModel) getYourNewDatabaseFeatures() DatabaseFeatureSet {
    return DatabaseFeatureSet{
        // SQL Features
        SupportsSQL:              true,
        SQLDialect:               "your_database_sql",
        SupportsStoredProcedures: false,
        SupportsTriggers:         false,
        SupportsViews:            true,
        SupportsCTE:              true,
        SupportsWindowFunctions:  true,
        SupportsRecursiveQueries: false,
        
        // Transaction Features
        SupportsTransactions:     true,
        SupportsNestedTransactions: false,
        SupportsSavepoints:       true,
        IsolationLevels:          []string{"READ_COMMITTED", "SERIALIZABLE"},
        
        // Index Features
        SupportsIndexes:          true,
        IndexTypes:               []string{"btree", "hash"},
        SupportsPartialIndexes:   true,
        SupportsExpressionIndexes: true,
        
        // Constraint Features
        SupportsConstraints:      true,
        ConstraintTypes:          []string{"PRIMARY_KEY", "FOREIGN_KEY", "UNIQUE", "CHECK"},
        SupportsDeferredConstraints: true,
        
        // Data Type Features
        SupportsJSON:             true,
        SupportsXML:              false,
        SupportsArrays:           true,
        SupportsUUID:             true,
        SupportsEnums:            true,
        SupportsCustomTypes:      true,
        
        // Advanced Features
        SupportsFullTextSearch:   false,
        SupportsGIS:              false,
        SupportsPartitioning:     false,
        SupportsSharding:         false,
        SupportsReplication:      true,
        
        // Limits
        MaxTableNameLength:       63,
        MaxColumnNameLength:      63,
        MaxIndexNameLength:       63,
        MaxColumnsPerTable:       1600,
        MaxIndexesPerTable:       unlimited,
        MaxRowSize:               "1GB",
        MaxDatabaseSize:          "unlimited",
    }
}
```

## Step 4: Testing

### 4.1 Add Type Conversion Tests

Create tests in `pkg/unifiedmodel/type_conversion_test.go`:

```go
func TestTypeConverter_YourNewDatabase(t *testing.T) {
    converter := NewTypeConverter()
    
    tests := []struct {
        name           string
        sourceDB       dbcapabilities.DatabaseType
        targetDB       dbcapabilities.DatabaseType
        sourceType     string
        expectedTarget string
        expectError    bool
    }{
        {
            name:           "YourNewDatabase integer to PostgreSQL",
            sourceDB:       dbcapabilities.YourNewDatabase,
            targetDB:       dbcapabilities.PostgreSQL,
            sourceType:     "integer",
            expectedTarget: "integer",
            expectError:    false,
        },
        {
            name:           "PostgreSQL to YourNewDatabase varchar",
            sourceDB:       dbcapabilities.PostgreSQL,
            targetDB:       dbcapabilities.YourNewDatabase,
            sourceType:     "varchar(255)",
            expectedTarget: "varchar(255)",
            expectError:    false,
        },
        // Add more test cases...
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := converter.ConvertDataType(tt.sourceDB, tt.targetDB, tt.sourceType)
            
            if tt.expectError {
                assert.Error(t, err)
                return
            }
            
            assert.NoError(t, err)
            assert.Equal(t, tt.expectedTarget, result.ConvertedType)
        })
    }
}
```

### 4.2 Add Integration Tests

Add translator tests for your database in the appropriate translator test files.

### 4.3 Run Test Suite

```bash
# Test type conversion
cd pkg/unifiedmodel
go test -v -run "TestTypeConverter.*YourNewDatabase"

# Test full translator suite
cd services/unifiedmodel/internal/translator
./run_tests.sh
```

## Step 5: Implement Database Adapter (Anchor Service)

To enable actual database connectivity, you must implement a database adapter in the anchor service. This allows the application to connect to and interact with your new database.

### 5.1 Create Adapter Directory Structure

Create the adapter directory and files:

```bash
mkdir services/anchor/internal/database/your_new_database
touch services/anchor/internal/database/your_new_database/{connection,types,schema,data,replication}.go
```

### 5.2 Implement Core Adapter Files

#### Connection Management (`connection.go`)

```go
package your_new_database

import (
    "context"
    "fmt"
    "github.com/redbco/redb-open/pkg/encryption"
    "github.com/redbco/redb-open/services/anchor/internal/database/common"
    // Your database driver import
)

// Connect establishes a connection to a database
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
        DatabaseType: "your_new_database",
        DatabaseID:   config.DatabaseID,
        Config:       config,
        IsConnected:  1,
    }, nil
}

// ConnectInstance establishes a connection to a database instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
    // Similar to Connect but for instance-level operations
}

// Additional required functions...
func DiscoverDetails(db interface{}) (*YourDatabaseDetails, error) { /* ... */ }
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) { /* ... */ }
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) { /* ... */ }
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) { /* ... */ }
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error { /* ... */ }
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error { /* ... */ }
```

#### Database-Specific Types (`types.go`)

```go
package your_new_database

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// YourDatabaseDetails contains information about your database
type YourDatabaseDetails struct {
    UniqueIdentifier string `json:"uniqueIdentifier"`
    DatabaseType     string `json:"databaseType"`
    DatabaseEdition  string `json:"databaseEdition"`
    Version          string `json:"version"`
    DatabaseSize     int64  `json:"databaseSize"`
    // Add database-specific fields
}

// YourDatabaseSchema represents the schema structure
type YourDatabaseSchema struct {
    Tables    []common.TableInfo    `json:"tables"`
    Views     []common.ViewInfo     `json:"views,omitempty"`
    Functions []common.FunctionInfo `json:"functions,omitempty"`
    // Add other schema elements as applicable
}
```

#### Schema Discovery (`schema.go`)

```go
package your_new_database

import (
    "fmt"
    "github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// DiscoverSchema fetches the current schema of a database
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

    // Discover views (if applicable)
    views, err := discoverViews(client)
    if err != nil {
        return nil, fmt.Errorf("error discovering views: %v", err)
    }
    schema.Views = views

    // Discover other schema elements...

    return schema, nil
}

// CreateStructure creates database objects based on the provided parameters
func CreateStructure(db interface{}, params common.StructureParams) error {
    client, ok := db.(*YourDatabaseClient)
    if !ok {
        return fmt.Errorf("invalid database connection type")
    }

    // Create tables
    for _, table := range params.Tables {
        if err := createTable(client, table); err != nil {
            return fmt.Errorf("error creating table %s: %v", table.Name, err)
        }
    }

    // Create other structures...
    return nil
}
```

#### Data Operations (`data.go`)

```go
package your_new_database

import (
    "fmt"
    "github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// FetchData retrieves data from a specified table/collection
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

// InsertData inserts data into a specified table/collection
func InsertData(db interface{}, tableName string, data []map[string]interface{}) (int64, error) {
    // Implementation for inserting data
}

// UpdateData updates data in a specified table/collection
func UpdateData(db interface{}, tableName string, data []map[string]interface{}, conditions map[string]interface{}) (int64, error) {
    // Implementation for updating data
}

// DeleteData deletes data from a specified table/collection
func DeleteData(db interface{}, tableName string, conditions map[string]interface{}) (int64, error) {
    // Implementation for deleting data
}
```

#### Replication Management (`replication.go`) - Optional

If your database supports CDC/replication:

```go
package your_new_database

import "github.com/redbco/redb-open/services/anchor/internal/database/common"

// ConnectReplication creates a new replication client and connection
func ConnectReplication(config common.ReplicationConfig) (*common.ReplicationClient, common.ReplicationSourceInterface, error) {
    // Create replication connection
    // Set up CDC mechanisms
    // Return client and source interface
}

// CreateReplicationSource creates a replication source using an existing database client
func CreateReplicationSource(db interface{}, config common.ReplicationConfig) (common.ReplicationSourceInterface, error) {
    // Implementation for creating replication source
}

// YourReplicationSourceDetails implements common.ReplicationSourceInterface
type YourReplicationSourceDetails struct {
    DatabaseID string
    // Add replication-specific fields
}

func (r *YourReplicationSourceDetails) GetSourceID() string { /* ... */ }
func (r *YourReplicationSourceDetails) GetDatabaseID() string { /* ... */ }
func (r *YourReplicationSourceDetails) GetStatus() map[string]interface{} { /* ... */ }
func (r *YourReplicationSourceDetails) Start() error { /* ... */ }
func (r *YourReplicationSourceDetails) Stop() error { /* ... */ }
func (r *YourReplicationSourceDetails) IsActive() bool { /* ... */ }
func (r *YourReplicationSourceDetails) GetMetadata() map[string]interface{} { /* ... */ }
func (r *YourReplicationSourceDetails) Close() error { /* ... */ }
```

### 5.3 Register Adapter with Manager

Add your adapter to the database manager's switch statements in the following files:

#### `services/anchor/internal/database/manager_connect.go`

```go
import (
    // ... existing imports
    "github.com/redbco/redb-open/services/anchor/internal/database/your_new_database"
)

// In ConnectDatabase function
case string(dbcapabilities.YourNewDatabase):
    client, err = your_new_database.Connect(config)
```

#### `services/anchor/internal/database/manager_structure.go`

```go
// In GetDatabaseStructure function
case string(dbcapabilities.YourNewDatabase):
    return your_new_database.DiscoverSchema(client.DB)
```

#### `services/anchor/internal/database/manager_data.go`

```go
// In FetchData function
case string(dbcapabilities.YourNewDatabase):
    return your_new_database.FetchData(client.DB, tableName, limit)

// Similar additions for InsertData, UpdateData, DeleteData
```

#### `services/anchor/internal/database/manager.go`

```go
// In ExecuteCommand function
case string(dbcapabilities.YourNewDatabase):
    return your_new_database.ExecuteCommand(context.Background(), client.DB, command)
```

### 5.4 Adapter Testing

Create comprehensive tests for your adapter:

```go
// services/anchor/internal/database/your_new_database/connection_test.go
func TestConnect(t *testing.T) {
    config := common.DatabaseConfig{
        Host:         "localhost",
        Port:         5432, // your database's default port
        Username:     "test",
        Password:     "test",
        DatabaseName: "test",
        TenantID:     "test-tenant",
    }

    client, err := Connect(config)
    assert.NoError(t, err)
    assert.NotNil(t, client)
    assert.Equal(t, "your_new_database", client.DatabaseType)
}

func TestDiscoverSchema(t *testing.T) {
    // Test schema discovery
}

func TestDataOperations(t *testing.T) {
    // Test CRUD operations
}
```

## Step 6: Documentation Updates

### 6.1 Update README

Add the new database to the supported databases list in the main README.

### 6.2 Update Database Adapter Guide

If applicable, update `docs/DATABASE_ADAPTER_IMPLEMENTATION_GUIDE.md` with any specific adapter requirements.

## Common Pitfalls and Best Practices

### ⚠️ Common Mistakes

1. **Missing Type Mappings**: Ensure all commonly used types have `UnifiedDataType` mappings
2. **Incorrect Precision/Scale**: Verify numeric type limits match the database's actual capabilities
3. **Case Sensitivity**: Some databases are case-sensitive for type names
4. **Alias Handling**: Don't forget to include common type aliases
5. **NULL Support**: Verify which types support NULL values
6. **Default Values**: Check which types can have default values

### ✅ Best Practices

#### Type Conversion Best Practices
1. **Comprehensive Testing**: Test conversions to/from major databases (PostgreSQL, MySQL, MongoDB)
2. **Documentation**: Document any database-specific quirks or limitations
3. **Precision Mapping**: Map precision/scale accurately to avoid data loss
4. **Lossy Conversions**: Mark conversions as lossy when precision is lost
5. **Performance**: Consider the performance impact of type conversions
6. **Version Support**: Document which database versions are supported

#### Adapter Implementation Best Practices
1. **Error Handling**: Use descriptive error messages with context and wrap errors with `fmt.Errorf`
2. **Connection Management**: Always test connections before returning clients and implement proper connection pooling
3. **Security**: Always use parameterized queries, support SSL/TLS encryption, and handle credentials securely
4. **Data Type Mapping**: Map database-specific types to Go types consistently and handle NULL values appropriately
5. **Performance**: Use prepared statements where possible and optimize schema discovery queries
6. **Logging**: Use the provided logger for consistent logging with appropriate log levels
7. **Resource Cleanup**: Always clean up resources in error cases and implement proper connection closing
8. **SSL Configuration**: Handle SSL certificate configuration consistently across all databases
9. **Authentication**: Support all authentication methods your database offers
10. **Batch Operations**: Support batch operations for better performance where applicable

## Validation Checklist

Before submitting your new database support:

### Database Capabilities & Type Conversion
- [ ] Database type constant added to `capabilities.go`
- [ ] Database added to appropriate paradigm lists
- [ ] Feature support defined comprehensively
- [ ] All primitive types mapped with correct metadata
- [ ] `DefaultMappings` includes all `UnifiedDataType` values used by other databases
- [ ] Custom type support properly configured
- [ ] Constraint support accurately reflects database capabilities
- [ ] Type conversion tests pass for major database pairs
- [ ] Integration tests pass

### Database Adapter Implementation
- [ ] Adapter directory structure created in `services/anchor/internal/database/`
- [ ] `connection.go` implements all required connection functions
- [ ] `types.go` defines database-specific types and schema structures
- [ ] `schema.go` implements schema discovery and structure creation
- [ ] `data.go` implements all CRUD operations
- [ ] `replication.go` implemented (if database supports CDC)
- [ ] Adapter registered in all manager files (`manager_connect.go`, `manager_structure.go`, etc.)
- [ ] Import statements added to manager files
- [ ] Adapter tests created and passing
- [ ] Connection string building handles SSL and authentication properly
- [ ] Error handling follows established patterns

### Documentation & Integration
- [ ] Documentation updated
- [ ] No breaking changes to existing functionality
- [ ] Full end-to-end testing completed (capabilities → type conversion → adapter → manager)

## Example: Adding SQLite Support

Here's a condensed example of adding SQLite:

```go
// In capabilities.go
const SQLite DatabaseType = "sqlite"

// In database_metadata_registry.go
func (stc *ScalableTypeConverter) createSQLiteMetadata() DatabaseTypeMetadata {
    return DatabaseTypeMetadata{
        DatabaseType: dbcapabilities.SQLite,
        PrimitiveTypes: map[string]PrimitiveTypeInfo{
            "INTEGER": {
                NativeName:   "INTEGER",
                UnifiedType:  UnifiedTypeInt64, // SQLite INTEGER is 64-bit
                SupportsNull: true,
            },
            "TEXT": {
                NativeName:   "TEXT",
                UnifiedType:  UnifiedTypeString,
                SupportsNull: true,
            },
            "REAL": {
                NativeName:   "REAL",
                UnifiedType:  UnifiedTypeFloat64,
                SupportsNull: true,
            },
            "BLOB": {
                NativeName:   "BLOB",
                UnifiedType:  UnifiedTypeBinary,
                SupportsNull: true,
            },
        },
        DefaultMappings: map[UnifiedDataType]string{
            UnifiedTypeInt32:     "INTEGER",
            UnifiedTypeInt64:     "INTEGER",
            UnifiedTypeFloat64:   "REAL",
            UnifiedTypeString:    "TEXT",
            UnifiedTypeVarchar:   "TEXT",
            UnifiedTypeBoolean:   "INTEGER", // SQLite uses INTEGER for boolean
            UnifiedTypeTimestamp: "TEXT",    // SQLite stores dates as text
            UnifiedTypeDecimal:   "TEXT",    // No native decimal support
            UnifiedTypeBinary:    "BLOB",
        },
    }
}
```

## Support and Troubleshooting

If you encounter issues while adding database support:

### Type Conversion Issues
1. **Check Existing Examples**: Look at similar databases for reference
2. **Verify Type Mappings**: Ensure all unified types have appropriate native mappings
3. **Test Incrementally**: Add basic types first, then advanced features
4. **Review Error Messages**: Type conversion errors often indicate missing mappings
5. **Consult Database Documentation**: Verify type capabilities and limits

### Adapter Implementation Issues
1. **Reference Existing Adapters**: Use PostgreSQL adapter as the reference implementation
2. **Check Manager Registration**: Ensure your adapter is properly registered in all manager files
3. **Verify Import Statements**: Make sure all necessary imports are added to manager files
4. **Test Connection Strings**: Verify SSL and authentication configurations work correctly
5. **Debug Schema Discovery**: Use database-specific tools to verify schema queries
6. **Check Error Patterns**: Follow established error handling patterns from existing adapters

### Additional Resources
- **Detailed Adapter Guide**: See `docs/DATABASE_ADAPTER_IMPLEMENTATION_GUIDE.md` for comprehensive adapter implementation details
- **Existing Implementations**: Reference PostgreSQL (`services/anchor/internal/database/postgres/`) and MongoDB (`services/anchor/internal/database/mongodb/`) adapters
- **Type Conversion Examples**: Check `pkg/unifiedmodel/type_conversion_test.go` for test patterns

For additional help, refer to the existing database implementations in the codebase or consult the development team.
