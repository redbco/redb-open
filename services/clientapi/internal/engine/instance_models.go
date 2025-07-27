package engine

// Instance represents an instance
type Instance struct {
	TenantID                 string   `json:"tenant_id"`
	WorkspaceID              string   `json:"workspace_id"`
	EnvironmentID            string   `json:"environment_id"`
	InstanceID               string   `json:"instance_id"`
	InstanceName             string   `json:"instance_name"`
	InstanceDescription      string   `json:"instance_description,omitempty"`
	InstanceType             string   `json:"instance_type"`
	InstanceVendor           string   `json:"instance_vendor"`
	InstanceVersion          string   `json:"instance_version"`
	InstanceUniqueIdentifier string   `json:"instance_unique_identifier"`
	ConnectedToNodeID        string   `json:"connected_to_node_id"`
	InstanceHost             string   `json:"instance_host"`
	InstancePort             int32    `json:"instance_port"`
	InstanceUsername         string   `json:"instance_username"`
	InstancePassword         string   `json:"instance_password"`
	InstanceSystemDBName     string   `json:"instance_system_db_name"`
	InstanceEnabled          bool     `json:"instance_enabled"`
	InstanceSSL              bool     `json:"instance_ssl"`
	InstanceSSLMode          string   `json:"instance_ssl_mode"`
	InstanceSSLCert          string   `json:"instance_ssl_cert"`
	InstanceSSLKey           string   `json:"instance_ssl_key"`
	InstanceSSLRootCert      string   `json:"instance_ssl_root_cert"`
	PolicyIDs                []string `json:"policy_ids"`
	OwnerID                  string   `json:"owner_id"`
	InstanceStatusMessage    string   `json:"instance_status_message"`
	Status                   Status   `json:"status"`
	Created                  string   `json:"created"`
	Updated                  string   `json:"updated"`
}

type ListInstancesResponse struct {
	Instances []Instance `json:"instances"`
}

type ShowInstanceResponse struct {
	Instance Instance `json:"instance"`
}

type ConnectInstanceRequest struct {
	InstanceName        string  `json:"instance_name" validate:"required"`
	InstanceDescription string  `json:"instance_description" validate:"required"`
	InstanceType        string  `json:"instance_type" validate:"required"`
	InstanceVendor      string  `json:"instance_vendor,omitempty"`
	Host                string  `json:"host" validate:"required"`
	Port                int32   `json:"port" validate:"required"`
	Username            string  `json:"username,omitempty"`
	Password            string  `json:"password,omitempty"`
	NodeID              *string `json:"node_id,omitempty"`
	Enabled             *bool   `json:"enabled,omitempty"`
	SSL                 *bool   `json:"ssl,omitempty"`
	SSLMode             string  `json:"ssl_mode,omitempty"`
	SSLCert             string  `json:"ssl_cert,omitempty"`
	SSLKey              string  `json:"ssl_key,omitempty"`
	SSLRootCert         string  `json:"ssl_root_cert,omitempty"`
	EnvironmentID       string  `json:"environment_id,omitempty"`
}

type ConnectInstanceResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Instance Instance `json:"instance"`
	Status   Status   `json:"status"`
}

type ReconnectInstanceResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Instance Instance `json:"instance"`
	Status   Status   `json:"status"`
}

type ModifyInstanceRequest struct {
	InstanceNameNew     string `json:"instance_name_new,omitempty"`
	InstanceDescription string `json:"instance_description,omitempty"`
	InstanceType        string `json:"instance_type,omitempty"`
	InstanceVendor      string `json:"instance_vendor,omitempty"`
	Host                string `json:"host,omitempty"`
	Port                *int32 `json:"port,omitempty"`
	Username            string `json:"username,omitempty"`
	Password            string `json:"password,omitempty"`
	Enabled             *bool  `json:"enabled,omitempty"`
	SSL                 *bool  `json:"ssl,omitempty"`
	SSLMode             string `json:"ssl_mode,omitempty"`
	SSLCert             string `json:"ssl_cert,omitempty"`
	SSLKey              string `json:"ssl_key,omitempty"`
	SSLRootCert         string `json:"ssl_root_cert,omitempty"`
	EnvironmentID       string `json:"environment_id,omitempty"`
	NodeID              string `json:"node_id,omitempty"`
}

type ModifyInstanceResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Instance Instance `json:"instance"`
	Status   Status   `json:"status"`
}

type DisconnectInstanceRequest struct {
	DeleteInstance *bool `json:"delete_instance,omitempty"`
}

type DisconnectInstanceResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type CreateDatabaseRequest struct {
	DatabaseName        string  `json:"database_name" validate:"required"`
	DatabaseDescription string  `json:"database_description" validate:"required"`
	DBName              string  `json:"db_name" validate:"required"`
	NodeID              *string `json:"node_id,omitempty"`
	Enabled             *bool   `json:"enabled,omitempty"`
	CreateWithUser      *bool   `json:"create_with_user,omitempty"`
	DatabaseUsername    *string `json:"database_username,omitempty"`
	DatabasePassword    *string `json:"database_password,omitempty"`
}

type CreateDatabaseResponse struct {
	Message  string   `json:"message"`
	Success  bool     `json:"success"`
	Database Database `json:"database"`
	Status   Status   `json:"status"`
}
