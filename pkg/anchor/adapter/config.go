package adapter

// ConnectionConfig contains the configuration for a database connection.
// This is a unified configuration that works across all database types.
type ConnectionConfig struct {
	// Core identifiers
	DatabaseID    string  `json:"databaseId"`
	TenantID      string  `json:"tenantId"`
	WorkspaceID   string  `json:"workspaceId"`
	EnvironmentID *string `json:"environmentId,omitempty"`
	InstanceID    string  `json:"instanceId,omitempty"`

	// Connection metadata
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// Database type and vendor
	ConnectionType string `json:"connectionType"` // e.g., "postgres", "mysql"
	DatabaseVendor string `json:"databaseVendor"` // Cloud provider or "custom"

	// Connection details
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	DatabaseName string `json:"databaseName"`

	// SSL/TLS configuration
	SSL                   bool    `json:"ssl,omitempty"`
	SSLMode               string  `json:"sslMode,omitempty"` // verify-full, require, etc.
	SSLRejectUnauthorized *bool   `json:"sslRejectUnauthorized,omitempty"`
	SSLCert               *string `json:"sslCert,omitempty"`
	SSLKey                *string `json:"sslKey,omitempty"`
	SSLRootCert           *string `json:"sslRootCert,omitempty"`

	// Additional options
	Role              string `json:"role,omitempty"`
	ConnectedToNodeID string `json:"connectedToNodeId,omitempty"`
	OwnerID           string `json:"ownerId,omitempty"`
	Enabled           *bool  `json:"enabled,omitempty"`

	// Cloud/Object Storage credentials (S3, GCS, Azure Blob)
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`
	Region          string `json:"region,omitempty"`
	PathStyle       bool   `json:"pathStyle,omitempty"`

	// BigQuery specific
	ProjectID       string `json:"projectId,omitempty"`
	CredentialsFile string `json:"credentialsFile,omitempty"`
	CredentialsJSON string `json:"credentialsJson,omitempty"`
	Location        string `json:"location,omitempty"`

	// InfluxDB specific
	Token        string `json:"token,omitempty"`
	Organization string `json:"organization,omitempty"`

	// Azure specific
	ConnectionString string `json:"connectionString,omitempty"`

	// Database-specific options (use sparingly)
	Options map[string]interface{} `json:"options,omitempty"`
}

// InstanceConfig contains the configuration for a database instance connection.
type InstanceConfig struct {
	// Core identifiers
	InstanceID    string  `json:"instanceId"`
	TenantID      string  `json:"tenantId"`
	WorkspaceID   string  `json:"workspaceId"`
	EnvironmentID *string `json:"environmentId,omitempty"`

	// Connection metadata
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// Database type and vendor
	ConnectionType string `json:"connectionType"`
	DatabaseVendor string `json:"databaseVendor"`

	// Instance information
	Version          string `json:"version,omitempty"`
	UniqueIdentifier string `json:"uniqueIdentifier,omitempty"`

	// Connection details
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	DatabaseName string `json:"databaseName"` // System database for connection

	// SSL/TLS configuration
	SSL                   bool    `json:"ssl,omitempty"`
	SSLMode               string  `json:"sslMode,omitempty"`
	SSLRejectUnauthorized *bool   `json:"sslRejectUnauthorized,omitempty"`
	SSLCert               *string `json:"sslCert,omitempty"`
	SSLKey                *string `json:"sslKey,omitempty"`
	SSLRootCert           *string `json:"sslRootCert,omitempty"`

	// Additional options
	Role              string `json:"role,omitempty"`
	ConnectedToNodeID string `json:"connectedToNodeId,omitempty"`
	OwnerID           string `json:"ownerId,omitempty"`
	Enabled           *bool  `json:"enabled,omitempty"`

	// Cloud/Object Storage credentials
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`
	Region          string `json:"region,omitempty"`
	PathStyle       bool   `json:"pathStyle,omitempty"`

	// BigQuery specific
	ProjectID       string `json:"projectId,omitempty"`
	CredentialsFile string `json:"credentialsFile,omitempty"`
	CredentialsJSON string `json:"credentialsJson,omitempty"`
	Location        string `json:"location,omitempty"`

	// InfluxDB specific
	Token        string `json:"token,omitempty"`
	Organization string `json:"organization,omitempty"`

	// Azure specific
	ConnectionString string `json:"connectionString,omitempty"`

	// Database-specific options
	Options map[string]interface{} `json:"options,omitempty"`
}

// ReplicationConfig contains the configuration for a replication connection.
type ReplicationConfig struct {
	// Core identifiers
	ReplicationID string  `json:"replicationId"`
	DatabaseID    string  `json:"databaseId"`
	TenantID      string  `json:"tenantId"`
	WorkspaceID   string  `json:"workspaceId"`
	EnvironmentID *string `json:"environmentId,omitempty"`

	// Replication metadata
	ReplicationName string `json:"replicationName"`

	// Database type
	ConnectionType string `json:"connectionType"`
	DatabaseVendor string `json:"databaseVendor"`

	// Connection details (may reuse database connection)
	Host         string `json:"host"`
	Port         int    `json:"port"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	DatabaseName string `json:"databaseName"`

	// SSL/TLS configuration
	SSL         bool   `json:"ssl,omitempty"`
	SSLMode     string `json:"sslMode,omitempty"`
	SSLCert     string `json:"sslCert,omitempty"`
	SSLKey      string `json:"sslKey,omitempty"`
	SSLRootCert string `json:"sslRootCert,omitempty"`

	// Replication scope
	TableNames      []string `json:"tableNames,omitempty"`      // Tables to replicate
	CollectionNames []string `json:"collectionNames,omitempty"` // Collections to replicate
	IndexNames      []string `json:"indexNames,omitempty"`      // Indexes to replicate
	KeyPatterns     []string `json:"keyPatterns,omitempty"`     // Key patterns to replicate

	// Database-specific replication options
	SlotName        string   `json:"slotName,omitempty"`        // PostgreSQL replication slot
	PublicationName string   `json:"publicationName,omitempty"` // PostgreSQL publication
	StreamNames     []string `json:"streamNames,omitempty"`     // Snowflake streams

	// Resume/checkpoint support
	StartPosition string `json:"startPosition,omitempty"` // Starting position for resume (LSN, binlog position, etc.)

	// Event handling
	EventHandler func(map[string]interface{}) `json:"-"` // Event callback function

	// Additional options
	Role              string                 `json:"role,omitempty"`
	ConnectedToNodeID string                 `json:"connectedToNodeId,omitempty"`
	OwnerID           string                 `json:"ownerId,omitempty"`
	Enabled           *bool                  `json:"enabled,omitempty"`
	Options           map[string]interface{} `json:"options,omitempty"`
}

// GetStringPtr returns a pointer to a string value, or nil if the string is empty.
// Helper function for optional string fields.
func GetStringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// GetString returns the string value from a pointer, or empty string if nil.
// Helper function for optional string fields.
func GetString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// GetBoolPtr returns a pointer to a bool value.
// Helper function for optional bool fields.
func GetBoolPtr(b bool) *bool {
	return &b
}

// GetBool returns the bool value from a pointer, or false if nil.
// Helper function for optional bool fields.
func GetBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}
