package engine

// Database represents a database
type Database struct {
	TenantID              string   `json:"tenant_id"`
	WorkspaceID           string   `json:"workspace_id"`
	EnvironmentID         string   `json:"environment_id"`
	ConnectedToNodeID     string   `json:"connected_to_node_id"`
	InstanceID            string   `json:"instance_id"`
	InstanceName          string   `json:"instance_name"`
	DatabaseID            string   `json:"database_id"`
	DatabaseName          string   `json:"database_name"`
	DatabaseDescription   string   `json:"database_description,omitempty"`
	DatabaseType          string   `json:"database_type"`
	DatabaseVendor        string   `json:"database_vendor"`
	DatabaseVersion       string   `json:"database_version"`
	DatabaseUsername      string   `json:"database_username"`
	DatabasePassword      string   `json:"database_password"`
	DatabaseDBName        string   `json:"database_db_name"`
	DatabaseEnabled       bool     `json:"database_enabled"`
	PolicyIDs             []string `json:"policy_ids"`
	OwnerID               string   `json:"owner_id"`
	DatabaseStatusMessage string   `json:"database_status_message"`
	Status                Status   `json:"status"`
	Created               string   `json:"created"`
	Updated               string   `json:"updated"`
	DatabaseSchema        string   `json:"database_schema"`
	DatabaseTables        string   `json:"database_tables"`
	InstanceHost          string   `json:"instance_host"`
	InstancePort          int32    `json:"instance_port"`
	InstanceSSLMode       string   `json:"instance_ssl_mode"`
	InstanceSSLCert       string   `json:"instance_ssl_cert"`
	InstanceSSLKey        string   `json:"instance_ssl_key"`
	InstanceSSLRootCert   string   `json:"instance_ssl_root_cert"`
	InstanceSSL           bool     `json:"instance_ssl"`
	InstanceStatusMessage string   `json:"instance_status_message"`
	InstanceStatus        string   `json:"instance_status"`
}

type ListDatabasesResponse struct {
	Databases []Database `json:"databases"`
}

type ShowDatabaseResponse struct {
	Database Database `json:"database"`
}

type ConnectDatabaseRequest struct {
	DatabaseName        string  `json:"database_name" validate:"required"`
	DatabaseDescription string  `json:"database_description" validate:"required"`
	DatabaseType        string  `json:"database_type" validate:"required"`
	DatabaseVendor      string  `json:"database_vendor" validate:"required"`
	Host                string  `json:"host" validate:"required"`
	Port                int32   `json:"port" validate:"required"`
	Username            string  `json:"username"`
	Password            string  `json:"password"`
	DBName              string  `json:"db_name" validate:"required"`
	NodeID              *string `json:"node_id,omitempty"`
	Enabled             *bool   `json:"enabled,omitempty"`
	SSL                 *bool   `json:"ssl,omitempty"`
	SSLMode             string  `json:"ssl_mode,omitempty"`
	SSLCert             string  `json:"ssl_cert,omitempty"`
	SSLKey              string  `json:"ssl_key,omitempty"`
	SSLRootCert         string  `json:"ssl_root_cert,omitempty"`
	EnvironmentID       string  `json:"environment_id,omitempty"`
}

type ConnectDatabaseResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Database Database `json:"database"`
	Status   Status   `json:"status"`
}

// ConnectDatabaseWithInstanceRequest represents the request for connecting a database to an existing instance
type ConnectDatabaseWithInstanceRequest struct {
	InstanceName        string  `json:"instance_name" validate:"required"`
	DatabaseName        string  `json:"database_name" validate:"required"`
	DatabaseDescription string  `json:"database_description" validate:"required"`
	DBName              string  `json:"db_name" validate:"required"`
	Username            *string `json:"username,omitempty"`
	Password            *string `json:"password,omitempty"`
	NodeID              *string `json:"node_id,omitempty"`
	Enabled             *bool   `json:"enabled,omitempty"`
	EnvironmentID       string  `json:"environment_id,omitempty"`
}

// ConnectDatabaseWithInstanceResponse represents the response for connecting a database to an existing instance
type ConnectDatabaseWithInstanceResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Database Database `json:"database"`
	Status   Status   `json:"status"`
}

type ModifyDatabaseRequest struct {
	DatabaseNameNew     string `json:"database_name_new,omitempty"`
	DatabaseDescription string `json:"database_description,omitempty"`
	DatabaseType        string `json:"database_type,omitempty"`
	DatabaseVendor      string `json:"database_vendor,omitempty"`
	Host                string `json:"host,omitempty"`
	Port                *int32 `json:"port,omitempty"`
	Username            string `json:"username,omitempty"`
	Password            string `json:"password,omitempty"`
	DBName              string `json:"db_name,omitempty"`
	Enabled             *bool  `json:"enabled,omitempty"`
	SSL                 *bool  `json:"ssl,omitempty"`
	SSLMode             string `json:"ssl_mode,omitempty"`
	SSLCert             string `json:"ssl_cert,omitempty"`
	SSLKey              string `json:"ssl_key,omitempty"`
	SSLRootCert         string `json:"ssl_root_cert,omitempty"`
	EnvironmentID       string `json:"environment_id,omitempty"`
	NodeID              string `json:"node_id,omitempty"`
}

type ModifyDatabaseResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Database Database `json:"database"`
	Status   Status   `json:"status"`
}

type DisconnectDatabaseRequest struct {
	DeleteDatabaseObject *bool `json:"delete_database_object,omitempty"`
	DeleteRepo           *bool `json:"delete_repo,omitempty"`
}

type DisconnectDatabaseResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type GetLatestStoredDatabaseSchemaResponse struct {
	Message string      `json:"message"`
	Success bool        `json:"success"`
	Status  Status      `json:"status"`
	Schema  interface{} `json:"schema"`
}

type WipeDatabaseResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type AddDatabaseRequest struct {
	DatabaseName        string `json:"database_name" validate:"required"`
	DatabaseDescription string `json:"database_description" validate:"required"`
	DBName              string `json:"db_name" validate:"required"`
	NodeID              string `json:"node_id" validate:"required"`
	Enabled             *bool  `json:"enabled,omitempty"`
}

type AddDatabaseResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Database Database `json:"database"`
	Status   Status   `json:"status"`
}

// DropDatabaseResponse represents the response for dropping a database
type DropDatabaseResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// Data transformation models
type TransformDataRequest struct {
	MappingName string                 `json:"mapping_name"`
	Mode        string                 `json:"mode"` // "append", "replace", "update"
	Options     map[string]interface{} `json:"options,omitempty"`
}

type TransformDataResponse struct {
	Message            string `json:"message"`
	Success            bool   `json:"success"`
	Status             Status `json:"status"`
	SourceDatabaseName string `json:"source_database_name"`
	SourceTableName    string `json:"source_table_name"`
	TargetDatabaseName string `json:"target_database_name"`
	TargetTableName    string `json:"target_table_name"`
	RowsTransformed    int64  `json:"rows_transformed"`
	RowsAffected       int64  `json:"rows_affected"`
}

// ReconnectDatabaseRequest represents a request to reconnect a database
type ReconnectDatabaseRequest struct {
	// No fields needed as it's just a path-based operation
}

// ReconnectDatabaseResponse represents a response from reconnecting a database
type ReconnectDatabaseResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Database Database `json:"database"`
	Status   Status   `json:"status"`
}
