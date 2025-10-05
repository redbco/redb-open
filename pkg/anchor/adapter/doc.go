// Package adapter provides the unified interface for all database adapters.
//
// This package defines the contracts that database-specific implementations must follow,
// enabling a consistent way to interact with any database technology while respecting
// their unique characteristics.
//
// # Architecture
//
// The adapter package follows an interface-driven design with several key components:
//
//   - DatabaseAdapter: The main interface that all database adapters implement
//   - Connection: Represents an active database connection with operation interfaces
//   - InstanceConnection: Represents an instance-level (server) connection
//   - Operation Interfaces: SchemaOperator, DataOperator, ReplicationOperator, MetadataOperator
//   - Registry: Manages adapter registration and retrieval
//
// # Usage
//
// To use an adapter, first register it with the global registry:
//
//	import (
//	    "github.com/redbco/redb-open/pkg/anchor/adapter"
//	    "github.com/redbco/redb-open/services/anchor/internal/database/postgres"
//	)
//
//	func init() {
//	    adapter.Register(postgres.NewAdapter())
//	}
//
// Then connect to a database:
//
//	config := adapter.ConnectionConfig{
//	    DatabaseID:     "my-db",
//	    ConnectionType: "postgres",
//	    Host:           "localhost",
//	    Port:           5432,
//	    DatabaseName:   "myapp",
//	    Username:       "user",
//	    Password:       "pass",
//	}
//
//	conn, err := adapter.GlobalRegistry().Connect(ctx, config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer conn.Close()
//
// Perform operations through the connection:
//
//	// Schema discovery
//	schema, err := conn.SchemaOperations().DiscoverSchema(ctx)
//
//	// Data operations
//	data, err := conn.DataOperations().Fetch(ctx, "users", 100)
//
//	// Replication (if supported)
//	if repOps := conn.ReplicationOperations(); repOps != nil {
//	    if repOps.IsSupported() {
//	        status, err := repOps.GetStatus(ctx)
//	    }
//	}
//
// # Capability-Based Design
//
// The adapter system is designed around database capabilities. Not all databases
// support all operations. Use nil checks and the IsSupported() method to check
// for capability support:
//
//	conn, _ := adapter.GlobalRegistry().Connect(ctx, config)
//
//	// Check if schema operations are supported
//	if schemaOps := conn.SchemaOperations(); schemaOps != nil {
//	    schema, err := schemaOps.DiscoverSchema(ctx)
//	}
//
//	// Check if replication is supported
//	if repOps := conn.ReplicationOperations(); repOps != nil && repOps.IsSupported() {
//	    // Use replication operations
//	}
//
// # Error Handling
//
// The adapter package provides standardized error types:
//
//   - DatabaseError: Wraps database-specific errors with context
//   - UnsupportedOperationError: Indicates an unsupported operation
//   - ConnectionError: Indicates a connection failure
//   - ConfigurationError: Indicates invalid configuration
//
// Use the Is() and As() functions from the errors package to check error types:
//
//	if adapter.IsUnsupported(err) {
//	    // Handle unsupported operation
//	}
//
//	if adapter.IsConnectionError(err) {
//	    // Handle connection error
//	}
//
// # Implementing a New Adapter
//
// To implement a new database adapter:
//
// 1. Create a new package under services/anchor/internal/database/{dbname}
//
// 2. Implement the DatabaseAdapter interface:
//
//	type Adapter struct{}
//
//	func NewAdapter() adapter.DatabaseAdapter {
//	    return &Adapter{}
//	}
//
//	func (a *Adapter) Type() dbcapabilities.DatabaseType {
//	    return dbcapabilities.YourDB
//	}
//
//	func (a *Adapter) Capabilities() dbcapabilities.Capability {
//	    return dbcapabilities.MustGet(dbcapabilities.YourDB)
//	}
//
// 3. Implement the Connection interface and operation interfaces
//
// 4. Register the adapter:
//
//	func init() {
//	    adapter.Register(NewAdapter())
//	}
//
// See the documentation in docs/DATABASE_ADAPTER_IMPLEMENTATION_GUIDE.md for
// detailed instructions on implementing a new adapter.
//
// # Thread Safety
//
// All types in this package are designed to be thread-safe. The Registry uses
// mutex locks to protect concurrent access. Connection implementations should
// also be thread-safe.
package adapter
