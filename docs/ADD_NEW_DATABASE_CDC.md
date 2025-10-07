# Adding CDC Support for a New Database

This guide shows how to add CDC/replication support for a new database type. The architecture is designed to make this process straightforward and require zero changes to the service layer.

## Prerequisites

- Database adapter already exists in `services/anchor/internal/database/{dbname}/`
- Adapter implements basic `ReplicationOperator` interface methods
- Understanding of the database's CDC mechanism (binlog, WAL, oplog, etc.)

## Step-by-Step Guide

### Step 1: Create CDC Operations File

Create `services/anchor/internal/database/{dbname}/cdc_ops.go`:

```go
package dbname

import (
    "context"
    "github.com/redbco/redb-open/pkg/anchor/adapter"
    "github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Implement three core methods:

// 1. ParseEvent - Convert database-specific event to universal CDCEvent
func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
    event := &adapter.CDCEvent{
        Timestamp: time.Now(),
        Metadata:  make(map[string]interface{}),
    }
    
    // Extract operation (INSERT, UPDATE, DELETE)
    if op, ok := rawEvent["operation"].(string); ok {
        event.Operation = adapter.CDCOperation(strings.ToUpper(op))
    }
    
    // Extract table name
    if tableName, ok := rawEvent["table_name"].(string); ok {
        event.TableName = tableName
    }
    
    // Extract new data
    if data, ok := rawEvent["data"].(map[string]interface{}); ok {
        event.Data = data
    }
    
    // Extract old data (for UPDATE/DELETE)
    if oldData, ok := rawEvent["old_data"].(map[string]interface{}); ok {
        event.OldData = oldData
    }
    
    // Add database-specific metadata
    // Example: LSN, transaction ID, commit timestamp, etc.
    
    return event, nil
}

// 2. ApplyCDCEvent - Apply universal CDCEvent to this database
func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
    switch event.Operation {
    case adapter.CDCInsert:
        return r.applyCDCInsert(ctx, event)
    case adapter.CDCUpdate:
        return r.applyCDCUpdate(ctx, event)
    case adapter.CDCDelete:
        return r.applyCDCDelete(ctx, event)
    default:
        return fmt.Errorf("unsupported operation: %s", event.Operation)
    }
}

// 3. TransformData - Apply transformations (basic implementation)
func (r *ReplicationOps) TransformData(ctx context.Context, data map[string]interface{}, rules []adapter.TransformationRule) (map[string]interface{}, error) {
    if len(rules) == 0 {
        return data, nil
    }
    
    transformedData := make(map[string]interface{})
    for _, rule := range rules {
        sourceValue, exists := data[rule.SourceColumn]
        if !exists {
            continue
        }
        
        // Apply transformation based on type
        transformedValue := sourceValue
        switch rule.TransformationType {
        case adapter.TransformDirect:
            transformedValue = sourceValue
        case adapter.TransformUppercase:
            if str, ok := sourceValue.(string); ok {
                transformedValue = strings.ToUpper(str)
            }
        // Add more transformation types as needed
        }
        
        transformedData[rule.TargetColumn] = transformedValue
    }
    
    return transformedData, nil
}
```

### Step 2: Implement Helper Methods

In the same file, add database-specific operations:

```go
// applyCDCInsert handles INSERT operations
func (r *ReplicationOps) applyCDCInsert(ctx context.Context, event *adapter.CDCEvent) error {
    // Build INSERT statement using database-specific syntax
    // Example for SQL databases:
    
    columns := []string{}
    values := []interface{}{}
    
    for col, val := range event.Data {
        if r.isMetadataField(col) {
            continue
        }
        columns = append(columns, col)
        values = append(values, val)
    }
    
    // Build query with database-specific placeholders
    query := buildInsertQuery(event.TableName, columns)
    
    // Execute
    _, err := r.conn.Execute(ctx, query, values...)
    return err
}

// applyCDCUpdate handles UPDATE operations
func (r *ReplicationOps) applyCDCUpdate(ctx context.Context, event *adapter.CDCEvent) error {
    // Build UPDATE statement
    // Use event.Data for SET clause
    // Use event.OldData for WHERE clause
    
    // ... implementation ...
}

// applyCDCDelete handles DELETE operations
func (r *ReplicationOps) applyCDCDelete(ctx context.Context, event *adapter.CDCEvent) error {
    // Build DELETE statement
    // Use event.OldData for WHERE clause
    
    // ... implementation ...
}

// Helper methods
func (r *ReplicationOps) isMetadataField(fieldName string) bool {
    // Define fields that should be skipped
    metadataFields := map[string]bool{
        "timestamp":   true,
        "operation":   true,
        "table_name":  true,
        // Add database-specific metadata fields
    }
    return metadataFields[fieldName]
}

func (r *ReplicationOps) quoteIdentifier(identifier string) string {
    // Database-specific identifier quoting
    // MySQL: backticks `identifier`
    // PostgreSQL: double quotes "identifier"
    // SQL Server: square brackets [identifier]
    // MongoDB: no quoting needed
}
```

### Step 3: Update ReplicationOps Connection

Ensure `replication_ops.go` returns your implementation:

```go
// In services/anchor/internal/database/{dbname}/adapter.go

func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
    return &ReplicationOps{conn: c}
}
```

### Step 4: Register Metadata in dbcapabilities (if needed)

If this is a completely new database, update `pkg/dbcapabilities/capabilities.go`:

```go
const (
    NewDatabase DatabaseType = "newdb"
)

var All = map[DatabaseType]Capability{
    // ... existing databases ...
    
    NewDatabase: {
        Name:              "New Database",
        ID:                NewDatabase,
        SupportsCDC:       true,  // Enable CDC support
        CDCMechanisms:     []string{"mechanism_name"},
        DefaultPort:       5432,
        // ... other metadata ...
    },
}
```

## Database-Specific Considerations

### SQL Databases

**Placeholder Styles:**
- PostgreSQL: `$1`, `$2`, `$3`, ...
- MySQL/MariaDB: `?`, `?`, `?`, ...
- SQL Server: `@p1`, `@p2`, `@p3`, ...
- Oracle: `:1`, `:2`, `:3`, ...

**Identifier Quoting:**
- PostgreSQL: `"identifier"`
- MySQL: `` `identifier` ``
- SQL Server: `[identifier]`

**CDC Mechanisms:**
- PostgreSQL: Logical replication (pgoutput, wal2json)
- MySQL: Binary log (binlog)
- SQL Server: Change Data Capture (CDC), Change Tracking
- Oracle: LogMiner, Streams

### NoSQL Databases

**Event Format:**
```go
// MongoDB Change Stream
{
    "operation": "insert",
    "collection": "users",  // map to TableName
    "database": "myapp",    // map to SchemaName
    "document": {...},      // map to Data
}

// Cassandra CDC
{
    "operation": "update",
    "keyspace": "myapp",    // map to SchemaName
    "table": "users",       // map to TableName
    "data": {...},
}
```

**Considerations:**
- Document databases: Full document in `Data`, may not have `OldData`
- Key-value stores: Simple key-value in `Data`
- Graph databases: Node/edge representation in `Data`

## Example: MongoDB CDC Operations

```go
package mongodb

func (r *ReplicationOps) ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*adapter.CDCEvent, error) {
    event := &adapter.CDCEvent{
        Timestamp: time.Now(),
        Metadata:  make(map[string]interface{}),
    }
    
    // MongoDB Change Stream event structure
    if opType, ok := rawEvent["operationType"].(string); ok {
        switch opType {
        case "insert":
            event.Operation = adapter.CDCInsert
        case "update", "replace":
            event.Operation = adapter.CDCUpdate
        case "delete":
            event.Operation = adapter.CDCDelete
        }
    }
    
    // Extract namespace (database.collection)
    if ns, ok := rawEvent["ns"].(map[string]interface{}); ok {
        if db, ok := ns["db"].(string); ok {
            event.SchemaName = db
        }
        if coll, ok := ns["coll"].(string); ok {
            event.TableName = coll
        }
    }
    
    // Extract document data
    if fullDocument, ok := rawEvent["fullDocument"].(map[string]interface{}); ok {
        event.Data = fullDocument
    }
    
    // Extract update description for old data
    if updateDesc, ok := rawEvent["updateDescription"].(map[string]interface{}); ok {
        event.Metadata["updateDescription"] = updateDesc
    }
    
    // Add resume token (MongoDB's equivalent of LSN)
    if resumeToken, ok := rawEvent["_id"]; ok {
        event.LSN = fmt.Sprintf("%v", resumeToken)
    }
    
    return event, nil
}

func (r *ReplicationOps) ApplyCDCEvent(ctx context.Context, event *adapter.CDCEvent) error {
    collection := r.conn.db.Collection(event.TableName)
    
    switch event.Operation {
    case adapter.CDCInsert:
        _, err := collection.InsertOne(ctx, event.Data)
        return err
        
    case adapter.CDCUpdate:
        // Use _id from data as filter
        filter := bson.M{"_id": event.Data["_id"]}
        update := bson.M{"$set": event.Data}
        _, err := collection.UpdateOne(ctx, filter, update)
        return err
        
    case adapter.CDCDelete:
        filter := bson.M{"_id": event.OldData["_id"]}
        _, err := collection.DeleteOne(ctx, filter)
        return err
    }
    
    return nil
}
```

## Testing Your Implementation

### Unit Tests

Create `{dbname}/cdc_ops_test.go`:

```go
func TestParseEvent(t *testing.T) {
    ops := &ReplicationOps{}
    
    rawEvent := map[string]interface{}{
        "operation":  "insert",
        "table_name": "users",
        "data": map[string]interface{}{
            "id":   1,
            "name": "Alice",
        },
    }
    
    event, err := ops.ParseEvent(context.Background(), rawEvent)
    assert.NoError(t, err)
    assert.Equal(t, adapter.CDCInsert, event.Operation)
    assert.Equal(t, "users", event.TableName)
    assert.Equal(t, 1, event.Data["id"])
}

func TestApplyCDCEvent(t *testing.T) {
    // Set up test database connection
    // Create test event
    // Call ApplyCDCEvent
    // Verify data was inserted/updated/deleted
}
```

### Integration Tests

Test with real database CDC:
1. Start CDC on source database
2. Make changes (INSERT, UPDATE, DELETE)
3. Verify events are parsed correctly
4. Verify events are applied to target
5. Check data consistency

## Common Pitfalls

### 1. Metadata Field Filtering
**Problem:** Metadata fields (timestamps, operation types) get inserted as data columns.

**Solution:** Implement `isMetadataField()` to filter them out:
```go
func (r *ReplicationOps) isMetadataField(fieldName string) bool {
    metadataFields := map[string]bool{
        "timestamp":   true,
        "operation":   true,
        "table_name":  true,
        // Add your database-specific fields
    }
    return metadataFields[fieldName]
}
```

### 2. NULL Handling
**Problem:** NULL values cause panics or incorrect WHERE clauses.

**Solution:** Check for nil explicitly:
```go
if val == nil {
    whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", column))
} else {
    whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", column))
    values = append(values, val)
}
```

### 3. Identifier Quoting
**Problem:** Table/column names with spaces or special characters fail.

**Solution:** Always quote identifiers using database-specific rules.

### 4. Transaction Boundaries
**Problem:** Related changes are applied out of order.

**Solution:** 
- Parse transaction IDs from events
- Group events by transaction
- Apply in transaction order

## Performance Optimization

### Batching
```go
func (r *ReplicationOps) ApplyCDCEventBatch(ctx context.Context, events []*adapter.CDCEvent) error {
    // Begin transaction
    tx, _ := r.conn.Begin()
    defer tx.Rollback()
    
    for _, event := range events {
        if err := r.applyCDCEvent(ctx, tx, event); err != nil {
            return err
        }
    }
    
    return tx.Commit()
}
```

### Connection Pooling
- Reuse connections for apply operations
- Use separate connection for CDC source
- Pool size based on throughput

### Prepared Statements
```go
// Cache prepared statements for frequently used tables
type ReplicationOps struct {
    conn       *Connection
    stmtCache  map[string]*PreparedStatement
}

func (r *ReplicationOps) getInsertStatement(table string, columns []string) (*PreparedStatement, error) {
    key := fmt.Sprintf("insert:%s:%v", table, columns)
    if stmt, ok := r.stmtCache[key]; ok {
        return stmt, nil
    }
    
    // Prepare and cache
    stmt, err := r.conn.Prepare(buildInsertQuery(table, columns))
    r.stmtCache[key] = stmt
    return stmt, err
}
```

## Checklist

- [ ] Created `cdc_ops.go` with three core methods
- [ ] Implemented `applyCDCInsert`, `applyCDCUpdate`, `applyCDCDelete`
- [ ] Added helper methods (`quoteIdentifier`, `isMetadataField`)
- [ ] Updated adapter to return ReplicationOps
- [ ] Added database to `dbcapabilities` (if new)
- [ ] Created unit tests
- [ ] Created integration tests
- [ ] Tested bidirectional replication
- [ ] Documented database-specific quirks
- [ ] Performance tested with realistic load

## Support

Once complete, your database will work with:
- ✅ Bidirectional replication with ANY other supported database
- ✅ Data transformations
- ✅ Future mesh-based replication
- ✅ All existing CDC management APIs

**No service layer changes required!**
