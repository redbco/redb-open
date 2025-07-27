package common

import (
	"sync"
)

// DatabaseClient represents a connected database client
type DatabaseClient struct {
	DB           interface{}
	DatabaseType string
	DatabaseID   string // In v2, database_id IS the config_id
	Config       DatabaseConfig
	LastSchema   interface{}
	IsConnected  int32
}

type DatabaseClients struct {
	Clients []*DatabaseClient
	Mutex   sync.RWMutex
}

// InstanceClient represents a connected database instance client
type InstanceClient struct {
	DB           interface{}
	InstanceType string
	InstanceID   string // In v2, instance_id IS the config_id
	Config       InstanceConfig
	LastSchema   interface{}
	IsConnected  int32
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
