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

	// Resource registry data (structured)
	ResourceContainers []DatabaseResourceContainer `json:"resource_containers,omitempty"`
}

// DatabaseResourceItem represents an item in a database resource container
type DatabaseResourceItem struct {
	ItemName                 string                   `json:"item_name"`
	ItemDisplayName          string                   `json:"item_display_name,omitempty"`
	DataType                 string                   `json:"data_type"`
	UnifiedDataType          string                   `json:"unified_data_type,omitempty"`
	IsNullable               bool                     `json:"is_nullable"`
	IsPrimaryKey             bool                     `json:"is_primary_key"`
	IsUnique                 bool                     `json:"is_unique"`
	IsIndexed                bool                     `json:"is_indexed"`
	IsRequired               bool                     `json:"is_required"`
	IsArray                  bool                     `json:"is_array"`
	DefaultValue             string                   `json:"default_value,omitempty"`
	Constraints              []map[string]interface{} `json:"constraints,omitempty"`
	IsPrivileged             bool                     `json:"is_privileged"`
	PrivilegedClassification string                   `json:"privileged_classification,omitempty"`
	DetectionConfidence      float64                  `json:"detection_confidence,omitempty"`
	DetectionMethod          string                   `json:"detection_method,omitempty"`
	OrdinalPosition          int32                    `json:"ordinal_position"`
	MaxLength                int32                    `json:"max_length,omitempty"`
	Precision                int32                    `json:"precision,omitempty"`
	Scale                    int32                    `json:"scale,omitempty"`
	ItemComment              string                   `json:"item_comment,omitempty"`
}

// DatabaseResourceContainer represents a database resource container (table, collection, etc.)
type DatabaseResourceContainer struct {
	ObjectType                        string                     `json:"object_type"`
	ObjectName                        string                     `json:"object_name"`
	ContainerClassification           string                     `json:"container_classification,omitempty"`
	ContainerClassificationConfidence float64                    `json:"container_classification_confidence,omitempty"`
	ContainerClassificationSource     string                     `json:"container_classification_source"`
	ContainerMetadata                 map[string]interface{}     `json:"container_metadata,omitempty"`
	EnrichedMetadata                  map[string]interface{}     `json:"enriched_metadata,omitempty"`
	DatabaseType                      string                     `json:"database_type,omitempty"`
	Vendor                            string                     `json:"vendor,omitempty"`
	ItemCount                         int32                      `json:"item_count"`
	Status                            string                     `json:"status"`
	Items                             []DatabaseResourceItem     `json:"items"`
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
	DeleteBranch         *bool `json:"delete_branch,omitempty"`
	DisconnectInstance   *bool `json:"disconnect_instance,omitempty"`
}

type DisconnectDatabaseResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type DatabaseDisconnectMetadata struct {
	DatabaseName                string `json:"database_name"`
	InstanceName                string `json:"instance_name"`
	IsLastDatabaseInInstance    bool   `json:"is_last_database_in_instance"`
	TotalDatabasesInInstance    int32  `json:"total_databases_in_instance"`
	HasAttachedBranch           bool   `json:"has_attached_branch"`
	AttachedRepoName            string `json:"attached_repo_name,omitempty"`
	AttachedBranchName          string `json:"attached_branch_name,omitempty"`
	IsOnlyBranchInRepo          bool   `json:"is_only_branch_in_repo"`
	TotalBranchesInRepo         int32  `json:"total_branches_in_repo"`
	HasOtherDatabasesOnBranch   bool   `json:"has_other_databases_on_branch"`
	CanDeleteBranchOnly         bool   `json:"can_delete_branch_only"`
	CanDeleteEntireRepo         bool   `json:"can_delete_entire_repo"`
	ShouldDeleteRepo            bool   `json:"should_delete_repo"`
	ShouldDeleteBranch          bool   `json:"should_delete_branch"`
}

type GetDatabaseDisconnectMetadataResponse struct {
	Message  string                      `json:"message"`
	Success  bool                        `json:"success"`
	Status   Status                      `json:"status"`
	Metadata DatabaseDisconnectMetadata  `json:"metadata"`
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

// ConnectDatabaseStringRequest represents a request to connect a database using a connection string
type ConnectDatabaseStringRequest struct {
	ConnectionString    string `json:"connection_string" validate:"required"`
	DatabaseName        string `json:"database_name" validate:"required"`
	DatabaseDescription string `json:"database_description,omitempty"`
	NodeID              string `json:"node_id,omitempty"`
	EnvironmentID       string `json:"environment_id,omitempty"`
	Enabled             *bool  `json:"enabled,omitempty"`
}

// ConnectDatabaseStringResponse represents the response for connecting a database via connection string
type ConnectDatabaseStringResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Database Database `json:"database"`
	Status   Status   `json:"status"`
}
