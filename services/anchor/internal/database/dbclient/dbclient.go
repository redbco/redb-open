package dbclient

import (
	"fmt"
	"sync"
	"time"
)

// DatabaseClient represents a connected database client
type DatabaseClient struct {
	DB                interface{}
	DatabaseType      string
	DatabaseID        string // In v2, database_id IS the config_id
	WorkspaceID       string
	TenantID          string
	EnvironmentID     string
	InstanceID        string
	Name              string
	Config            DatabaseConfig
	LastSchema        interface{}
	IsConnected       int32
	AdapterConnection interface{} // Stores adapter.Connection when using adapter-based connections
}

type DatabaseClients struct {
	Clients []*DatabaseClient
	Mutex   sync.RWMutex
}

// InstanceClient represents a connected database instance client
type InstanceClient struct {
	DB                interface{}
	InstanceType      string
	InstanceID        string // In v2, instance_id IS the config_id
	Config            InstanceConfig
	LastSchema        interface{}
	IsConnected       int32
	AdapterConnection interface{} // Stores adapter.InstanceConnection when using adapter-based connections
}

type InstanceClients struct {
	Clients []*InstanceClient
	Mutex   sync.RWMutex
}

type DatabaseConfig struct {
	DatabaseID            string `json:"databaseId,omitempty"`            // Unique identifier for the database (same as config_id in v2)
	WorkspaceID           string `json:"workspaceId,omitempty"`           // Workspace ID for the database connection
	TenantID              string `json:"tenantId,omitempty"`              // Tenant ID for the database connection
	EnvironmentID         string `json:"environmentId,omitempty"`         // Environment ID for the database connection
	InstanceID            string `json:"instanceId,omitempty"`            // Associated instance ID
	Name                  string `json:"name,omitempty"`                  // Name for the database connection
	Description           string `json:"description,omitempty"`           // Description for the database connection
	DatabaseVendor        string `json:"DatabaseVendor"`                  // Database provider (e.g., "postgres", "mysql")
	ConnectionType        string `json:"connectionType"`                  // Connection type (e.g., "direct", "proxy")
	Host                  string `json:"host"`                            // Database host
	Port                  int    `json:"port"`                            // Database port
	Username              string `json:"username,omitempty"`              // Database username
	Password              string `json:"password,omitempty"`              // Database password
	DatabaseName          string `json:"databaseName"`                    // Database name
	Enabled               *bool  `json:"enabled,omitempty"`               // Optional field to ignore the connection if set to false
	SSL                   bool   `json:"ssl,omitempty"`                   // Whether to use SSL/TLS
	SSLMode               string `json:"sslMode,omitempty"`               // SSL mode (e.g., "verify-full", "require")
	SSLRejectUnauthorized *bool  `json:"sslRejectUnauthorized,omitempty"` // Whether to reject unauthorized SSL certificates
	SSLCert               string `json:"sslCert,omitempty"`               // Path to SSL certificate file
	SSLKey                string `json:"sslKey,omitempty"`                // Path to SSL key file
	SSLRootCert           string `json:"sslRootCert,omitempty"`           // Path to SSL root certificate file
	Role                  string `json:"role,omitempty"`                  // Database role
	ConnectedToNodeID     string `json:"connectedToNodeId,omitempty"`     // Node ID where database is connected
	OwnerID               string `json:"ownerId,omitempty"`               // Owner ID
}

type InstanceConfig struct {
	InstanceID            string `json:"instanceId,omitempty"`            // Unique identifier for the instance (same as config_id in v2)
	WorkspaceID           string `json:"workspaceId,omitempty"`           // Workspace ID for the instance connection
	TenantID              string `json:"tenantId,omitempty"`              // Tenant ID for the instance connection
	EnvironmentID         string `json:"environmentId,omitempty"`         // Environment ID for the instance connection
	Name                  string `json:"name,omitempty"`                  // Name for the instance connection
	Description           string `json:"description,omitempty"`           // Description for the instance connection
	DatabaseVendor        string `json:"DatabaseVendor"`                  // Database provider (e.g., "postgres", "mysql")
	ConnectionType        string `json:"connectionType"`                  // Connection type (e.g., "direct", "proxy")
	Host                  string `json:"host"`                            // Database host
	Port                  int    `json:"port"`                            // Database port
	Username              string `json:"username,omitempty"`              // Database username
	Password              string `json:"password,omitempty"`              // Database password
	DatabaseName          string `json:"databaseName"`                    // System database name for connection
	Enabled               *bool  `json:"enabled,omitempty"`               // Optional field to ignore the connection if set to false
	SSL                   bool   `json:"ssl,omitempty"`                   // Whether to use SSL/TLS
	SSLMode               string `json:"sslMode,omitempty"`               // SSL mode (e.g., "verify-full", "require")
	SSLRejectUnauthorized *bool  `json:"sslRejectUnauthorized,omitempty"` // Whether to reject unauthorized SSL certificates
	SSLCert               string `json:"sslCert,omitempty"`               // Path to SSL certificate file
	SSLKey                string `json:"sslKey,omitempty"`                // Path to SSL key file
	SSLRootCert           string `json:"sslRootCert,omitempty"`           // Path to SSL root certificate file
	Role                  string `json:"role,omitempty"`                  // Database role
	ConnectedToNodeID     string `json:"connectedToNodeId,omitempty"`     // Node ID where instance is connected
	OwnerID               string `json:"ownerId,omitempty"`               // Owner ID
	UniqueIdentifier      string `json:"uniqueIdentifier,omitempty"`      // Unique identifier for the instance
	Version               string `json:"version,omitempty"`               // Instance version
}

type ConnectionResult struct {
	ID      string         `json:"id"`
	Type    string         `json:"type"`
	Config  DatabaseConfig `json:"config"`
	Success bool           `json:"success"`
	Error   string         `json:"error,omitempty"`
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

func GenerateUniqueID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
