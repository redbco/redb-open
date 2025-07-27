package common

import "time"

// UnifiedInstanceConfig represents a database instance configuration that can be used
// for both database storage and connection management
type UnifiedInstanceConfig struct {
	// Core identifiers
	InstanceID        string  `json:"instanceId,omitempty" db:"instance_id"`
	TenantID          string  `json:"tenantId,omitempty" db:"tenant_id"`
	WorkspaceID       string  `json:"workspaceId,omitempty" db:"workspace_id"`
	EnvironmentID     *string `json:"environmentId,omitempty" db:"environment_id"`
	ConnectedToNodeID string  `json:"connectedToNodeId,omitempty" db:"connected_to_node_id"`
	OwnerID           string  `json:"ownerId,omitempty" db:"owner_id"`

	// Instance information
	Name             string `json:"name,omitempty" db:"instance_name"`
	Description      string `json:"description,omitempty" db:"instance_description"`
	Type             string `json:"connectionType" db:"instance_type"`
	Vendor           string `json:"DatabaseVendor" db:"instance_vendor"`
	Version          string `json:"version,omitempty" db:"instance_version"`
	UniqueIdentifier string `json:"uniqueIdentifier,omitempty" db:"instance_unique_identifier"`

	// Connection details
	Host         string `json:"host" db:"instance_host"`
	Port         int    `json:"port" db:"instance_port"`
	Username     string `json:"username,omitempty" db:"instance_username"`
	Password     string `json:"password,omitempty" db:"instance_password"`
	DatabaseName string `json:"databaseName" db:"instance_system_db_name"`

	// Connection options
	Enabled               bool    `json:"enabled,omitempty" db:"instance_enabled"`
	SSL                   bool    `json:"ssl,omitempty" db:"instance_ssl"`
	SSLMode               string  `json:"sslMode,omitempty" db:"instance_ssl_mode"`
	SSLRejectUnauthorized *bool   `json:"sslRejectUnauthorized,omitempty"`
	SSLCert               *string `json:"sslCert,omitempty" db:"instance_ssl_cert"`
	SSLKey                *string `json:"sslKey,omitempty" db:"instance_ssl_key"`
	SSLRootCert           *string `json:"sslRootCert,omitempty" db:"instance_ssl_root_cert"`
	Role                  string  `json:"role,omitempty"`

	// Administrative fields (only for database storage)
	PolicyIDs     []string  `json:"policyIds,omitempty" db:"policy_ids"`
	StatusMessage string    `json:"statusMessage,omitempty" db:"instance_status_message"`
	Status        string    `json:"status,omitempty" db:"status"`
	Created       time.Time `json:"created,omitempty" db:"created"`
	Updated       time.Time `json:"updated,omitempty" db:"updated"`
}

// UnifiedDatabaseConfig represents a database configuration that can be used
// for both database storage and connection management
type UnifiedDatabaseConfig struct {
	// Core identifiers
	DatabaseID        string  `json:"databaseId,omitempty" db:"database_id"`
	TenantID          string  `json:"tenantId,omitempty" db:"tenant_id"`
	WorkspaceID       string  `json:"workspaceId,omitempty" db:"workspace_id"`
	EnvironmentID     *string `json:"environmentId,omitempty" db:"environment_id"`
	InstanceID        string  `json:"instanceId,omitempty" db:"instance_id"`
	ConnectedToNodeID string  `json:"connectedToNodeId,omitempty" db:"connected_to_node_id"`
	OwnerID           string  `json:"ownerId,omitempty" db:"owner_id"`

	// Database information
	Name        string `json:"name,omitempty" db:"database_name"`
	Description string `json:"description,omitempty" db:"database_description"`
	Type        string `json:"connectionType" db:"database_type"`
	Vendor      string `json:"DatabaseVendor" db:"database_vendor"`
	Version     string `json:"version,omitempty" db:"database_version"`

	// Connection details (inherited from instance)
	Host         string `json:"host" db:"instance_host"`
	Port         int    `json:"port" db:"instance_port"`
	Username     string `json:"username,omitempty" db:"database_username"`
	Password     string `json:"password,omitempty" db:"database_password"`
	DatabaseName string `json:"databaseName" db:"database_db_name"`

	// Connection options (inherited from instance)
	Enabled               bool    `json:"enabled,omitempty" db:"database_enabled"`
	SSL                   bool    `json:"ssl,omitempty" db:"instance_ssl"`
	SSLMode               string  `json:"sslMode,omitempty" db:"instance_ssl_mode"`
	SSLRejectUnauthorized *bool   `json:"sslRejectUnauthorized,omitempty"`
	SSLCert               *string `json:"sslCert,omitempty" db:"instance_ssl_cert"`
	SSLKey                *string `json:"sslKey,omitempty" db:"instance_ssl_key"`
	SSLRootCert           *string `json:"sslRootCert,omitempty" db:"instance_ssl_root_cert"`
	Role                  string  `json:"role,omitempty"`

	// Administrative fields (only for database storage)
	PolicyIDs     []string  `json:"policyIds,omitempty" db:"policy_ids"`
	StatusMessage string    `json:"statusMessage,omitempty" db:"database_status_message"`
	Status        string    `json:"status,omitempty" db:"status"`
	Created       time.Time `json:"created,omitempty" db:"created"`
	Updated       time.Time `json:"updated,omitempty" db:"updated"`
}

// ToConnectionConfig returns a version suitable for database connections
// (strips administrative fields and adjusts types)
func (c *UnifiedInstanceConfig) ToConnectionConfig() InstanceConfig {
	enabled := c.Enabled

	// Helper function to safely dereference string pointers
	stringFromPtr := func(ptr *string) string {
		if ptr != nil {
			return *ptr
		}
		return ""
	}

	return InstanceConfig{
		InstanceID:            c.InstanceID,
		WorkspaceID:           c.WorkspaceID,
		TenantID:              c.TenantID,
		EnvironmentID:         stringFromPtr(c.EnvironmentID),
		Name:                  c.Name,
		Description:           c.Description,
		DatabaseVendor:        c.Vendor,
		ConnectionType:        c.Type,
		Host:                  c.Host,
		Port:                  c.Port,
		Username:              c.Username,
		Password:              c.Password,
		DatabaseName:          c.DatabaseName,
		Enabled:               &enabled,
		SSL:                   c.SSL,
		SSLMode:               c.SSLMode,
		SSLRejectUnauthorized: c.SSLRejectUnauthorized,
		SSLCert:               stringFromPtr(c.SSLCert),
		SSLKey:                stringFromPtr(c.SSLKey),
		SSLRootCert:           stringFromPtr(c.SSLRootCert),
		Role:                  c.Role,
		ConnectedToNodeID:     c.ConnectedToNodeID,
		OwnerID:               c.OwnerID,
		UniqueIdentifier:      c.UniqueIdentifier,
		Version:               c.Version,
	}
}

// ToConnectionConfig returns a version suitable for database connections
// (strips administrative fields and adjusts types)
func (c *UnifiedDatabaseConfig) ToConnectionConfig() DatabaseConfig {
	enabled := c.Enabled

	// Helper function to safely dereference string pointers
	stringFromPtr := func(ptr *string) string {
		if ptr != nil {
			return *ptr
		}
		return ""
	}

	return DatabaseConfig{
		DatabaseID:            c.DatabaseID,
		WorkspaceID:           c.WorkspaceID,
		TenantID:              c.TenantID,
		EnvironmentID:         stringFromPtr(c.EnvironmentID),
		InstanceID:            c.InstanceID,
		Name:                  c.Name,
		Description:           c.Description,
		DatabaseVendor:        c.Vendor,
		ConnectionType:        c.Type,
		Host:                  c.Host,
		Port:                  c.Port,
		Username:              c.Username,
		Password:              c.Password,
		DatabaseName:          c.DatabaseName,
		Enabled:               &enabled,
		SSL:                   c.SSL,
		SSLMode:               c.SSLMode,
		SSLRejectUnauthorized: c.SSLRejectUnauthorized,
		SSLCert:               stringFromPtr(c.SSLCert),
		SSLKey:                stringFromPtr(c.SSLKey),
		SSLRootCert:           stringFromPtr(c.SSLRootCert),
		Role:                  c.Role,
		ConnectedToNodeID:     c.ConnectedToNodeID,
		OwnerID:               c.OwnerID,
	}
}

type StructureParams struct {
	UniqueIdentifier  string                 `json:"uniqueIdentifier"`
	DatabaseType      string                 `json:"databaseType"`
	DatabaseEdition   string                 `json:"databaseEdition"`
	Version           string                 `json:"version"`
	DatabaseSize      string                 `json:"databaseSize"`
	Tables            []TableInfo            `json:"tables"`
	EnumTypes         []EnumInfo             `json:"enumTypes"`
	CustomTypes       map[string]string      `json:"customTypes"`
	Schemas           []DatabaseSchemaInfo   `json:"schemas"`
	Views             []ViewInfo             `json:"views"`
	MaterializedViews []MaterializedViewInfo `json:"materializedViews"`
	Functions         []FunctionInfo         `json:"functions"`
	Triggers          []TriggerInfo          `json:"triggers"`
	Sequences         []SequenceInfo         `json:"sequences"`
	Extensions        []ExtensionInfo        `json:"extensions"`
	Events            []EventInfo            `json:"events"`
	Procedures        []ProcedureInfo        `json:"procedures"`
	Modules           []ModuleInfo           `json:"modules"`
	Types             []TypeInfo             `json:"types"`
	Packages          []PackageInfo          `json:"packages"`
	Constraints       []Constraint           `json:"constraints"`
	Indexes           []IndexInfo            `json:"indexes"`
}

type SchemaInfo struct {
	SchemaType        string                 `json:"schemaType"`
	Tables            []TableInfo            `json:"tables"`
	EnumTypes         []EnumInfo             `json:"enumTypes"`
	CustomTypes       map[string]string      `json:"customTypes"`
	Schemas           []DatabaseSchemaInfo   `json:"schemas"`
	Views             []ViewInfo             `json:"views"`
	MaterializedViews []MaterializedViewInfo `json:"materializedViews"`
	Functions         []FunctionInfo         `json:"functions"`
	Triggers          []TriggerInfo          `json:"triggers"`
	Sequences         []SequenceInfo         `json:"sequences"`
	Extensions        []ExtensionInfo        `json:"extensions"`
}

type DatabaseSchemaInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Database Structure Types
type ColumnInfo struct {
	Name                 string       `json:"name"`
	DataType             string       `json:"dataType"`
	IsNullable           bool         `json:"isNullable"`
	IsPrimaryKey         bool         `json:"isPrimaryKey"`
	IsArray              bool         `json:"isArray"`
	IsUnique             bool         `json:"isUnique"`
	IsAutoIncrement      bool         `json:"isAutoIncrement"`
	IsGenerated          bool         `json:"isGenerated"`
	GenerationExpression *string      `json:"generationExpression,omitempty"`
	ColumnDefault        *string      `json:"columnDefault,omitempty"`
	Constraints          []Constraint `json:"constraints,omitempty"`
	ArrayElementType     *string      `json:"arrayElementType,omitempty"`
	CustomTypeName       *string      `json:"customTypeName,omitempty"`
	VarcharLength        *int         `json:"varcharLength,omitempty"`
	NumericPrecision     *string      `json:"numericPrecision,omitempty"`
	NumericScale         *string      `json:"numericScale,omitempty"`
}

type Constraint struct {
	Type             string          `json:"type"`
	Name             string          `json:"name"`
	Table            string          `json:"table"`
	Column           string          `json:"column"`
	ForeignTable     string          `json:"foreignTable,omitempty"`
	ForeignColumn    string          `json:"foreignColumn,omitempty"`
	OnUpdate         string          `json:"onUpdate,omitempty"`
	OnDelete         string          `json:"onDelete,omitempty"`
	Definition       string          `json:"definition,omitempty"`
	ForeignKey       *ForeignKeyInfo `json:"foreignKey,omitempty"`
	ReferencedTable  string          `json:"referencedTable,omitempty"`
	ReferencedColumn string          `json:"referencedColumn,omitempty"`
	LabelOrType      string          `json:"labelOrType,omitempty"`
	PropertyKeys     []string        `json:"propertyKeys,omitempty"`
	IsRelationship   bool            `json:"isRelationship,omitempty"`
}

type ForeignKeyInfo struct {
	Table    string `json:"table"`
	Column   string `json:"column"`
	OnUpdate string `json:"onUpdate"`
	OnDelete string `json:"onDelete"`
}

type EnumInfo struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

type IndexInfo struct {
	Name     string       `json:"name"`
	Columns  []string     `json:"columns,omitempty"`
	IsUnique bool         `json:"isUnique,omitempty"`
	Mappings MappingInfo  `json:"mappings,omitempty"` // Elasticsearch
	Settings SettingsInfo `json:"settings,omitempty"` // Elasticsearch
}

type MappingInfo struct {
	Properties map[string]PropertyInfo `json:"properties"`
}

type PropertyInfo struct {
	Type   string `json:"type"`
	Format string `json:"format,omitempty"`
}

type SettingsInfo struct {
	NumberOfShards   string `json:"numberOfShards"`
	NumberOfReplicas string `json:"numberOfReplicas"`
}

type TableInfo struct {
	Schema            string       `json:"schema"`
	Name              string       `json:"name"`
	TableType         string       `json:"tableType"`
	Columns           []ColumnInfo `json:"columns"`
	PrimaryKey        []string     `json:"primaryKey"`
	Indexes           []IndexInfo  `json:"indexes"`
	Constraints       []Constraint `json:"constraints"`
	ParentTable       string       `json:"parentTable,omitempty"`       // Only used for partition child tables
	PartitionValue    string       `json:"partitionValue,omitempty"`    // Only used for partition child tables
	PartitionStrategy string       `json:"partitionStrategy,omitempty"` // Only used for partitioned tables
	PartitionKey      []string     `json:"partitionKey,omitempty"`      // Only used for partitioned tables
	Partitions        []string     `json:"partitions,omitempty"`        // Only used for partitioned tables
	ViewDefinition    string       `json:"viewDefinition,omitempty"`    // Only used for views and materialized views
	Tablespace        string       `json:"tablespace,omitempty"`        // Only used for tables
}

type TableInfoWithSize struct {
	Schema        string       `json:"schema"`
	Name          string       `json:"name"`
	Columns       []ColumnInfo `json:"columns"`
	PrimaryKey    []string     `json:"primaryKey"`
	Indexes       []IndexInfo  `json:"indexes"`
	Constraints   []Constraint `json:"constraints"`
	EstimatedSize int64        `json:"estimatedSize"`
	EstimatedRows int64        `json:"estimatedRows"`
}

type ViewInfo struct {
	Name       string `json:"name"`
	Schema     string `json:"schema"`
	Definition string `json:"definition"`
}

type MaterializedViewInfo struct {
	Name            string            `json:"name"`
	Schema          string            `json:"schema,omitempty"`
	Definition      string            `json:"definition,omitempty"`
	Keyspace        string            `json:"keyspace,omitempty"`
	BaseTable       string            `json:"baseTable,omitempty"`
	IncludeAll      bool              `json:"includeAll,omitempty"`
	Columns         []string          `json:"columns,omitempty"`
	PrimaryKey      []string          `json:"primaryKey,omitempty"`
	ClusteringOrder map[string]string `json:"clusteringOrder,omitempty"`
	WhereClause     string            `json:"whereClause,omitempty"`
}

type FunctionInfo struct {
	Name       string `json:"name"`
	Schema     string `json:"schema"`
	Arguments  string `json:"arguments"`
	ReturnType string `json:"returnType"`
	Body       string `json:"body"`
}

type TriggerInfo struct {
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Event     string `json:"event"`
	Statement string `json:"statement"`
	Timing    string `json:"timing"`
}

type ExtensionInfo struct {
	Name        string   `json:"name"`
	Schema      string   `json:"schema"`
	Version     string   `json:"version"`
	Tables      []string `json:"tables"`
	Description string   `json:"description,omitempty"`
}

type SequenceInfo struct {
	Name      string `json:"name"`
	Schema    string `json:"schema"`
	DataType  string `json:"dataType"`
	Start     int64  `json:"startValue"`
	Increment int64  `json:"increment"`
	MaxValue  int64  `json:"maxValue"`
	MinValue  int64  `json:"minValue"`
	CacheSize int64  `json:"cacheSize"`
	Cycle     bool   `json:"cycle"`
}

type EventInfo struct {
	Name            string `json:"name"`
	DefinerUser     string `json:"definerUser"`
	DefinerHost     string `json:"definerHost"`
	EventType       string `json:"eventType"`
	ExecuteAt       string `json:"executeAt,omitempty"`
	IntervalValue   string `json:"intervalValue,omitempty"`
	IntervalField   string `json:"intervalField,omitempty"`
	Status          string `json:"status"`
	Starts          string `json:"starts,omitempty"`
	Ends            string `json:"ends,omitempty"`
	OnCompletion    string `json:"onCompletion"`
	Created         string `json:"created"`
	LastAltered     string `json:"lastAltered"`
	LastExecuted    string `json:"lastExecuted,omitempty"`
	EventComment    string `json:"eventComment"`
	Originator      int    `json:"originator"`
	TimeZone        string `json:"timeZone"`
	EventBody       string `json:"eventBody"`
	EventDefinition string `json:"eventDefinition"`
}

type ProcedureInfo struct {
	Name              string `json:"name"`
	DefinerUser       string `json:"definerUser"`
	DefinerHost       string `json:"definerHost"`
	Created           string `json:"created"`
	Modified          string `json:"modified"`
	SecurityType      string `json:"securityType"`
	Comment           string `json:"comment"`
	DefinerSQL        string `json:"definerSQL"`
	ParameterStyle    string `json:"parameterStyle"`
	IsDeterministic   string `json:"isDeterministic"`
	SQLDataAccess     string `json:"sqlDataAccess"`
	RoutineBody       string `json:"routineBody"`
	RoutineDefinition string `json:"routineDefinition"`
	ParameterList     string `json:"parameterList"`
	Schema            string `json:"schema"`
	Arguments         string `json:"arguments"`
	Body              string `json:"body"`
}

type ModuleInfo struct {
	Name string `edgedb:"name"`
}

type TypeInfo struct {
	Module      string                 `json:"module"`
	Name        string                 `json:"name"`
	IsAbstract  bool                   `json:"isAbstract"`
	Properties  []EdgeDBPropertyInfo   `json:"properties"`
	Links       []LinkInfo             `json:"links"`
	Bases       []string               `json:"bases"`
	Constraints []EdgeDBConstraintInfo `json:"constraints"`
	Attributes  []TypeAttribute        `json:"attributes"`
	TypeCode    string                 `json:"typeCode"`
}

type EdgeDBPropertyInfo struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Required    bool                   `json:"required"`
	ReadOnly    bool                   `json:"readOnly"`
	Default     interface{}            `json:"default,omitempty"`
	Constraints []EdgeDBConstraintInfo `json:"constraints,omitempty"`
}

type LinkInfo struct {
	Name     string `json:"name"`
	Target   string `json:"target"`
	Required bool   `json:"required"`
	Multi    bool   `json:"multi"`
	ReadOnly bool   `json:"readOnly"`
	OnDelete string `json:"onDelete,omitempty"`
	OnUpdate string `json:"onUpdate,omitempty"`
}

type ScalarInfo struct {
	Module      string                 `json:"module"`
	Name        string                 `json:"name"`
	BaseType    string                 `json:"baseType"`
	Constraints []EdgeDBConstraintInfo `json:"constraints"`
}

type AliasInfo struct {
	Module string `json:"module"`
	Name   string `json:"name"`
	Type   string `json:"type"`
}

type EdgeDBConstraintInfo struct {
	Name string      `json:"name"`
	Args interface{} `json:"args,omitempty"`
}

type PackageInfo struct {
	Name          string `json:"name"`
	Schema        string `json:"schema"`
	Version       string `json:"version"`
	Description   string `json:"description"`
	Specification string `json:"specification"`
	Body          string `json:"body"`
}

type TypeAttribute struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// CollectionInfo represents a MongoDB collection
type CollectionInfo struct {
	Name       string                   `json:"name"`
	Options    map[string]interface{}   `json:"options,omitempty"`
	Indexes    []IndexInfo              `json:"indexes,omitempty"`
	SampleDocs []map[string]interface{} `json:"sampleDocs,omitempty"`
	Count      int64                    `json:"count"`
	Size       int64                    `json:"size"`
}

// DocumentInfo represents a MongoDB document
type DocumentInfo struct {
	ID         interface{}            `json:"_id"`
	Fields     map[string]interface{} `json:"fields"`
	Collection string                 `json:"collection"`
}

// CollectionValidation represents validation rules for a collection
type CollectionValidation struct {
	Validator        map[string]interface{} `json:"validator,omitempty"`
	ValidationLevel  string                 `json:"validationLevel,omitempty"`
	ValidationAction string                 `json:"validationAction,omitempty"`
}

// CollectionOptions represents options for a collection
type CollectionOptions struct {
	Capped       bool  `json:"capped,omitempty"`
	Size         int64 `json:"size,omitempty"`
	MaxDocuments int64 `json:"maxDocuments,omitempty"`
}

// FindOptions represents options for finding documents
type FindOptions struct {
	Limit      int                    `json:"limit,omitempty"`
	Skip       int                    `json:"skip,omitempty"`
	Sort       map[string]string      `json:"sort,omitempty"`
	Projection map[string]interface{} `json:"projection,omitempty"`
}

// KeyInfo represents information about a Redis key
type KeyInfo struct {
	Name        string      `json:"name"`
	Type        string      `json:"type"`
	TTL         int64       `json:"ttl"`
	Size        int64       `json:"size"`
	SampleValue interface{} `json:"sampleValue,omitempty"`
}

// StreamInfo represents information about a Redis stream
type StreamInfo struct {
	Name         string `json:"name"`
	Length       int64  `json:"length"`
	FirstEntryID string `json:"firstEntryID,omitempty"`
	LastEntryID  string `json:"lastEntryID,omitempty"`
	Groups       int    `json:"groups"`
}

// KeySpaceInfo represents information about a Redis keyspace
type KeySpaceInfo struct {
	ID      int   `json:"id"`
	Keys    int64 `json:"keys"`
	Expires int64 `json:"expires"`
	AvgTTL  int64 `json:"avgTTL"`
}

// ReplicationConfig represents a replication connection configuration
type ReplicationConfig struct {
	ReplicationID     string `json:"replicationId"`
	DatabaseID        string `json:"databaseId"`
	WorkspaceID       string `json:"workspaceId"`
	TenantID          string `json:"tenantId"`
	EnvironmentID     string `json:"environmentId,omitempty"`
	ReplicationName   string `json:"replicationName"`
	ConnectionType    string `json:"connectionType"`
	DatabaseVendor    string `json:"databaseVendor"`
	Host              string `json:"host"`
	Port              int    `json:"port"`
	Username          string `json:"username"`
	Password          string `json:"password"`
	DatabaseName      string `json:"databaseName"`
	SSL               bool   `json:"ssl"`
	SSLMode           string `json:"sslMode,omitempty"`
	SSLCert           string `json:"sslCert,omitempty"`
	SSLKey            string `json:"sslKey,omitempty"`
	SSLRootCert       string `json:"sslRootCert,omitempty"`
	Role              string `json:"role,omitempty"`
	Enabled           *bool  `json:"enabled,omitempty"`
	ConnectedToNodeID string `json:"connectedToNodeId"`
	OwnerID           string `json:"ownerId"`
	// Replication-specific configuration
	TableNames         []string                     `json:"tableNames,omitempty"`         // Tables to replicate (now supports multiple)
	SlotName           string                       `json:"slotName,omitempty"`           // Postgres replication slot
	PublicationName    string                       `json:"publicationName,omitempty"`    // Postgres publication
	StreamNames        []string                     `json:"streamNames,omitempty"`        // Snowflake streams
	CollectionNames    []string                     `json:"collectionNames,omitempty"`    // MongoDB collections
	IndexNames         []string                     `json:"indexNames,omitempty"`         // Elasticsearch indices
	KeyPatterns        []string                     `json:"keyPatterns,omitempty"`        // Redis key patterns
	EventHandler       func(map[string]interface{}) `json:"-"`                            // Event callback function
	ReplicationOptions map[string]interface{}       `json:"replicationOptions,omitempty"` // Database-specific options
}

// ReplicationClient represents a replication connection to a database
// Now supports multiple tables per logical database connection
type ReplicationClient struct {
	ReplicationID     string                       `json:"replicationId"`
	DatabaseID        string                       `json:"databaseId"`
	DatabaseType      string                       `json:"databaseType"`
	Config            ReplicationConfig            `json:"config"`
	Connection        interface{}                  `json:"-"`           // Database-specific connection
	ReplicationSource interface{}                  `json:"-"`           // Database-specific replication source details
	EventHandler      func(map[string]interface{}) `json:"-"`           // Event callback function
	IsConnected       int32                        `json:"isConnected"` // Use atomic operations
	LastActivity      time.Time                    `json:"lastActivity"`
	Status            string                       `json:"status"`
	StatusMessage     string                       `json:"statusMessage"`
	ErrorCount        int32                        `json:"errorCount"`
	CreatedAt         time.Time                    `json:"createdAt"`
	ConnectedAt       *time.Time                   `json:"connectedAt,omitempty"`
	// Multi-table support
	TableNames map[string]struct{} `json:"tableNames"` // Set of tables being replicated
	// Optionally, per-table event handlers or metadata can be added here
}

// AddTable adds a table to the replication client
func (rc *ReplicationClient) AddTable(table string) {
	if rc.TableNames == nil {
		rc.TableNames = make(map[string]struct{})
	}
	rc.TableNames[table] = struct{}{}
}

// RemoveTable removes a table from the replication client
func (rc *ReplicationClient) RemoveTable(table string) {
	if rc.TableNames != nil {
		delete(rc.TableNames, table)
	}
}

// HasTable checks if the replication client is replicating a given table
func (rc *ReplicationClient) HasTable(table string) bool {
	_, ok := rc.TableNames[table]
	return ok
}

// GetTables returns a slice of all tables being replicated
func (rc *ReplicationClient) GetTables() []string {
	tables := make([]string, 0, len(rc.TableNames))
	for t := range rc.TableNames {
		tables = append(tables, t)
	}
	return tables
}

// ReplicationMetadata represents metadata about a replication connection
type ReplicationMetadata struct {
	ReplicationID   string                 `json:"replicationId"`
	DatabaseID      string                 `json:"databaseId"`
	Status          string                 `json:"status"`
	EventsProcessed int64                  `json:"eventsProcessed"`
	LastEventTime   *time.Time             `json:"lastEventTime,omitempty"`
	Lag             map[string]interface{} `json:"lag,omitempty"`
	ErrorCount      int32                  `json:"errorCount"`
	TableNames      []string               `json:"tableNames,omitempty"`
	AdditionalInfo  map[string]interface{} `json:"additionalInfo,omitempty"`
}

// ReplicationEventHandler defines the interface for handling replication events
type ReplicationEventHandler interface {
	HandleEvent(event map[string]interface{}) error
	GetEventTypes() []string
	IsEventTypeSupported(eventType string) bool
}

// ReplicationSourceInterface defines the interface for database-specific replication sources
type ReplicationSourceInterface interface {
	// GetSourceID returns a unique identifier for this replication source
	GetSourceID() string

	// GetDatabaseID returns the database ID this source is replicating from
	GetDatabaseID() string

	// GetStatus returns the current status of the replication source
	GetStatus() map[string]interface{}

	// Start begins replication from this source
	Start() error

	// Stop halts replication from this source
	Stop() error

	// IsActive returns whether the replication source is currently active
	IsActive() bool

	// GetMetadata returns metadata about the replication source
	GetMetadata() map[string]interface{}

	// Close properly closes and cleans up the replication source
	Close() error
}

// DatabaseReplicationInterface defines the interface for database-specific replication implementations
type DatabaseReplicationInterface interface {
	// CreateReplicationSource creates a new replication source for the given configuration
	CreateReplicationSource(config ReplicationConfig) (ReplicationSourceInterface, error)

	// ReconnectReplicationSource reconnects to an existing replication source
	ReconnectReplicationSource(source ReplicationSourceInterface) error

	// GetReplicationStatus returns the status of all replication sources for this database
	GetReplicationStatus(databaseID string) (map[string]interface{}, error)

	// CheckReplicationPrerequisites checks if the database supports replication
	CheckReplicationPrerequisites(databaseID string) error

	// ListReplicationSources returns all replication sources for this database
	ListReplicationSources(databaseID string) ([]ReplicationSourceInterface, error)

	// CleanupReplicationSources cleans up orphaned or inactive replication sources
	CleanupReplicationSources(databaseID string) error
}
