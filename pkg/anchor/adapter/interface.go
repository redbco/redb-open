// Package adapter provides the unified interface for all database adapters.
// This package defines the contracts that database-specific implementations must follow.
package adapter

import (
	"context"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
)

// DatabaseAdapter represents a database technology adapter.
// Each database type (PostgreSQL, MySQL, MongoDB, etc.) must implement this interface.
type DatabaseAdapter interface {
	// Type returns the canonical database type identifier
	Type() dbcapabilities.DatabaseType

	// Capabilities returns the capability metadata for this database type
	Capabilities() dbcapabilities.Capability

	// Connect establishes a connection to a specific database
	Connect(ctx context.Context, config ConnectionConfig) (Connection, error)

	// ConnectInstance establishes a connection to a database instance (server-level)
	ConnectInstance(ctx context.Context, config InstanceConfig) (InstanceConnection, error)
}

// Connection represents an active connection to a specific database.
// This is the main interface for interacting with a database.
type Connection interface {
	// Identity and status
	ID() string
	Type() dbcapabilities.DatabaseType
	IsConnected() bool

	// Lifecycle management
	Ping(ctx context.Context) error
	Close() error

	// Operation interfaces
	// Returns nil if the operation category is not supported by this database
	SchemaOperations() SchemaOperator
	DataOperations() DataOperator
	ReplicationOperations() ReplicationOperator
	MetadataOperations() MetadataOperator

	// Raw returns the underlying database-specific connection object.
	// Use this only when you need to perform operations not covered by the standard interfaces.
	// Type assertion is required when using Raw().
	Raw() interface{}

	// Configuration
	Config() ConnectionConfig
	Adapter() DatabaseAdapter
}

// InstanceConnection represents an active connection to a database instance.
// This is used for instance-level operations like creating/dropping databases.
type InstanceConnection interface {
	// Identity and status
	ID() string
	Type() dbcapabilities.DatabaseType
	IsConnected() bool

	// Lifecycle management
	Ping(ctx context.Context) error
	Close() error

	// Instance-level database management
	ListDatabases(ctx context.Context) ([]string, error)
	CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error
	DropDatabase(ctx context.Context, name string, options map[string]interface{}) error

	// Metadata operations
	MetadataOperations() MetadataOperator

	// Raw returns the underlying database-specific connection object
	Raw() interface{}

	// Configuration
	Config() InstanceConfig
	Adapter() DatabaseAdapter
}

// SchemaOperator handles schema discovery and modification operations.
// Not all databases support all schema operations.
type SchemaOperator interface {
	// DiscoverSchema retrieves the complete schema of the database as a UnifiedModel
	DiscoverSchema(ctx context.Context) (*unifiedmodel.UnifiedModel, error)

	// CreateStructure creates database objects from a UnifiedModel
	CreateStructure(ctx context.Context, model *unifiedmodel.UnifiedModel) error

	// ListTables returns the names of all tables/collections in the database
	ListTables(ctx context.Context) ([]string, error)

	// GetTableSchema retrieves the schema for a specific table/collection
	GetTableSchema(ctx context.Context, tableName string) (*unifiedmodel.Table, error)
}

// DataOperator handles data CRUD operations.
// All databases should support basic data operations.
type DataOperator interface {
	// Read operations
	Fetch(ctx context.Context, table string, limit int) ([]map[string]interface{}, error)
	FetchWithColumns(ctx context.Context, table string, columns []string, limit int) ([]map[string]interface{}, error)

	// Write operations
	Insert(ctx context.Context, table string, data []map[string]interface{}) (int64, error)
	Update(ctx context.Context, table string, data []map[string]interface{}, whereColumns []string) (int64, error)
	Upsert(ctx context.Context, table string, data []map[string]interface{}, uniqueColumns []string) (int64, error)
	Delete(ctx context.Context, table string, conditions map[string]interface{}) (int64, error)

	// Streaming for large datasets
	Stream(ctx context.Context, params StreamParams) (StreamResult, error)

	// Query execution (for databases supporting query languages)
	ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]interface{}, error)
	ExecuteCountQuery(ctx context.Context, query string) (int64, error)

	// Utility operations
	GetRowCount(ctx context.Context, table string, whereClause string) (int64, bool, error)
	Wipe(ctx context.Context) error
}

// ReplicationOperator handles Change Data Capture (CDC) and replication operations.
// Only databases with CDC support will implement this fully.
type ReplicationOperator interface {
	// Capability checking
	IsSupported() bool
	GetSupportedMechanisms() []string

	// Setup and management
	CheckPrerequisites(ctx context.Context) error
	Connect(ctx context.Context, config ReplicationConfig) (ReplicationSource, error)

	// Status and monitoring
	GetStatus(ctx context.Context) (map[string]interface{}, error)
	GetLag(ctx context.Context) (map[string]interface{}, error)

	// Slot/publication management (where applicable)
	ListSlots(ctx context.Context) ([]map[string]interface{}, error)
	DropSlot(ctx context.Context, slotName string) error
	ListPublications(ctx context.Context) ([]map[string]interface{}, error)
	DropPublication(ctx context.Context, publicationName string) error

	// CDC Event handling - these methods enable database-agnostic CDC
	// ParseEvent converts a raw database-specific event to a standardized CDCEvent
	ParseEvent(ctx context.Context, rawEvent map[string]interface{}) (*CDCEvent, error)

	// ApplyCDCEvent applies a standardized CDC event to this database
	// This method handles INSERT, UPDATE, DELETE operations in a database-specific way
	ApplyCDCEvent(ctx context.Context, event *CDCEvent) error

	// TransformData applies transformation rules to event data
	// Returns the transformed data ready for application to target database
	// transformationServiceEndpoint is the gRPC endpoint for the transformation service (optional, for custom transformations)
	TransformData(ctx context.Context, data map[string]interface{}, rules []TransformationRule, transformationServiceEndpoint string) (map[string]interface{}, error)
}

// MetadataOperator handles metadata collection and introspection.
// All databases should support basic metadata operations.
type MetadataOperator interface {
	// Metadata collection
	CollectDatabaseMetadata(ctx context.Context) (map[string]interface{}, error)
	CollectInstanceMetadata(ctx context.Context) (map[string]interface{}, error)

	// Version and identification
	GetVersion(ctx context.Context) (string, error)
	GetUniqueIdentifier(ctx context.Context) (string, error)

	// Statistics
	GetDatabaseSize(ctx context.Context) (int64, error)
	GetTableCount(ctx context.Context) (int, error)

	// Command execution (for databases supporting admin commands)
	ExecuteCommand(ctx context.Context, command string) ([]byte, error)
}

// ReplicationSource represents an active replication connection.
type ReplicationSource interface {
	// Identity
	GetSourceID() string
	GetDatabaseID() string

	// Status
	GetStatus() map[string]interface{}
	GetMetadata() map[string]interface{}
	IsActive() bool

	// Lifecycle
	Start() error
	Stop() error
	Close() error

	// Position tracking for graceful shutdown and resume
	// GetPosition returns the current replication position (LSN, binlog position, etc.)
	// The format is database-specific: PostgreSQL uses LSN, MySQL uses binlog file:position
	GetPosition() (string, error)

	// SetPosition sets the starting replication position for resume operations
	// This should be called before Start() to resume from a specific position
	SetPosition(position string) error

	// SaveCheckpoint persists the current position for crash recovery
	// This is typically called periodically or after processing batches of events
	SaveCheckpoint(ctx context.Context, position string) error
}

// StreamParams configures streaming operations for large datasets.
type StreamParams struct {
	Table     string   // Table/collection name
	Columns   []string // Specific columns to fetch (empty = all)
	BatchSize int32    // Number of rows per batch
	Offset    int64    // Starting offset
	OrderBy   string   // Column to order by (optional)
}

// StreamResult contains the result of a streaming operation.
type StreamResult struct {
	Data       []map[string]interface{} // The batch of data
	HasMore    bool                     // Whether more data is available
	NextCursor string                   // Cursor for pagination
}
