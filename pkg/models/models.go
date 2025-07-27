package models

import (
	"time"
)

// LocalIdentity represents the localidentity table
type LocalIdentity struct {
	IdentityID string `json:"identity_id" db:"identity_id"`
}

// Mesh represents the mesh table for distributed system coordination
type Mesh struct {
	MeshID          string    `json:"mesh_id" db:"mesh_id"`
	MeshName        string    `json:"mesh_name" db:"mesh_name"`
	MeshDescription string    `json:"mesh_description" db:"mesh_description"`
	PublicKey       string    `json:"public_key" db:"public_key"`
	PrivateKey      string    `json:"private_key" db:"private_key"`
	AllowJoin       bool      `json:"allow_join" db:"allow_join"`
	JoinKey         *string   `json:"join_key" db:"join_key"`
	Status          string    `json:"status" db:"status"`
	Created         time.Time `json:"created" db:"created"`
	Updated         time.Time `json:"updated" db:"updated"`
}

// Region represents the regions table
type Region struct {
	RegionID          string    `json:"region_id" db:"region_id"`
	RegionName        string    `json:"region_name" db:"region_name"`
	RegionDescription string    `json:"region_description" db:"region_description"`
	RegionType        string    `json:"region_type" db:"region_type"`
	RegionLocation    string    `json:"region_location" db:"region_location"`
	RegionLatitude    *float64  `json:"region_latitude" db:"region_latitude"`
	RegionLongitude   *float64  `json:"region_longitude" db:"region_longitude"`
	GlobalRegion      bool      `json:"global_region" db:"global_region"`
	Status            string    `json:"status" db:"status"`
	Created           time.Time `json:"created" db:"created"`
	Updated           time.Time `json:"updated" db:"updated"`
}

// Node represents the nodes table
type Node struct {
	NodeID          string    `json:"node_id" db:"node_id"`
	NodeName        string    `json:"node_name" db:"node_name"`
	NodeDescription string    `json:"node_description" db:"node_description"`
	NodePlatform    string    `json:"node_platform" db:"node_platform"`
	NodeVersion     string    `json:"node_version" db:"node_version"`
	RegionID        *string   `json:"region_id" db:"region_id"`
	PublicKey       *string   `json:"public_key" db:"public_key"`
	PrivateKey      *string   `json:"private_key" db:"private_key"`
	IPAddress       string    `json:"ip_address" db:"ip_address"`
	Port            int       `json:"port" db:"port"`
	Status          string    `json:"status" db:"status"`
	Created         time.Time `json:"created" db:"created"`
	Updated         time.Time `json:"updated" db:"updated"`
}

// Route represents the routes table
type Route struct {
	RouteID            string    `json:"route_id" db:"route_id"`
	SourceNodeID       string    `json:"source_node_id" db:"source_node_id"`
	TargetNodeID       string    `json:"target_node_id" db:"target_node_id"`
	RouteBidirectional bool      `json:"route_bidirectional" db:"route_bidirectional"`
	RouteLatency       float64   `json:"route_latency" db:"route_latency"`
	RouteBandwidth     float64   `json:"route_bandwidth" db:"route_bandwidth"`
	RouteCost          int       `json:"route_cost" db:"route_cost"`
	Status             string    `json:"status" db:"status"`
	Created            time.Time `json:"created" db:"created"`
	Updated            time.Time `json:"updated" db:"updated"`
}

// Tenant represents the tenants table
type Tenant struct {
	TenantID          string    `json:"tenant_id" db:"tenant_id"`
	TenantName        string    `json:"tenant_name" db:"tenant_name"`
	TenantDescription string    `json:"tenant_description" db:"tenant_description"`
	Status            string    `json:"status" db:"status"`
	Created           time.Time `json:"created" db:"created"`
	Updated           time.Time `json:"updated" db:"updated"`
}

// User represents the users table
type User struct {
	UserID           string    `json:"user_id" db:"user_id"`
	TenantID         string    `json:"tenant_id" db:"tenant_id"`
	UserEmail        string    `json:"user_email" db:"user_email"`
	UserName         string    `json:"user_name" db:"user_name"`
	UserPasswordHash string    `json:"user_password_hash" db:"user_password_hash"`
	UserEnabled      bool      `json:"user_enabled" db:"user_enabled"`
	PasswordChanged  time.Time `json:"password_changed" db:"password_changed"`
	Created          time.Time `json:"created" db:"created"`
	Updated          time.Time `json:"updated" db:"updated"`
}

// APIToken represents the apitokens table
type APIToken struct {
	APITokenID             string    `json:"apitoken_id" db:"apitoken_id"`
	TenantID               string    `json:"tenant_id" db:"tenant_id"`
	UserID                 string    `json:"user_id" db:"user_id"`
	APITokenName           string    `json:"apitoken_name" db:"apitoken_name"`
	APITokenDescription    string    `json:"apitoken_description" db:"apitoken_description"`
	APITokenKey            string    `json:"apitoken_key" db:"apitoken_key"`
	APITokenEnabled        bool      `json:"apitoken_enabled" db:"apitoken_enabled"`
	APITokenAutoExpires    bool      `json:"apitoken_auto_expires" db:"apitoken_auto_expires"`
	APITokenExpiryTimeDays int       `json:"apitoken_expiry_time_days" db:"apitoken_expiry_time_days"`
	APITokenKeyCycled      time.Time `json:"apitoken_key_cycled" db:"apitoken_key_cycled"`
	OwnerID                string    `json:"owner_id" db:"owner_id"`
	Created                time.Time `json:"created" db:"created"`
	Updated                time.Time `json:"updated" db:"updated"`
}

// Group represents the groups table
type Group struct {
	GroupID          string    `json:"group_id" db:"group_id"`
	TenantID         string    `json:"tenant_id" db:"tenant_id"`
	GroupName        string    `json:"group_name" db:"group_name"`
	GroupDescription string    `json:"group_description" db:"group_description"`
	ParentGroupID    *string   `json:"parent_group_id" db:"parent_group_id"`
	OwnerID          string    `json:"owner_id" db:"owner_id"`
	Created          time.Time `json:"created" db:"created"`
	Updated          time.Time `json:"updated" db:"updated"`
}

// Role represents the roles table
type Role struct {
	RoleID          string    `json:"role_id" db:"role_id"`
	TenantID        string    `json:"tenant_id" db:"tenant_id"`
	RoleName        string    `json:"role_name" db:"role_name"`
	RoleDescription string    `json:"role_description" db:"role_description"`
	OwnerID         string    `json:"owner_id" db:"owner_id"`
	Created         time.Time `json:"created" db:"created"`
	Updated         time.Time `json:"updated" db:"updated"`
}

// Permission represents the permissions table
type Permission struct {
	PermissionID string                 `json:"permission_id" db:"permission_id"`
	TenantID     string                 `json:"tenant_id" db:"tenant_id"`
	Action       string                 `json:"action" db:"action"`
	Resource     string                 `json:"resource" db:"resource"`
	Scope        string                 `json:"scope" db:"scope"`
	Constraints  map[string]interface{} `json:"constraints" db:"constraints"`
	Conditions   map[string]interface{} `json:"conditions" db:"conditions"`
	OwnerID      string                 `json:"owner_id" db:"owner_id"`
	Created      time.Time              `json:"created" db:"created"`
	Updated      time.Time              `json:"updated" db:"updated"`
}

// UserGroup represents the user_groups table
type UserGroup struct {
	TenantID  string    `json:"tenant_id" db:"tenant_id"`
	UserID    string    `json:"user_id" db:"user_id"`
	GroupID   string    `json:"group_id" db:"group_id"`
	GrantedBy string    `json:"granted_by" db:"granted_by"`
	GrantedAt time.Time `json:"granted_at" db:"granted_at"`
	OwnerID   string    `json:"owner_id" db:"owner_id"`
	Created   time.Time `json:"created" db:"created"`
	Updated   time.Time `json:"updated" db:"updated"`
}

// UserRole represents the user_roles table
type UserRole struct {
	TenantID  string     `json:"tenant_id" db:"tenant_id"`
	UserID    string     `json:"user_id" db:"user_id"`
	RoleID    string     `json:"role_id" db:"role_id"`
	GrantedBy string     `json:"granted_by" db:"granted_by"`
	GrantedAt time.Time  `json:"granted_at" db:"granted_at"`
	ExpiresAt *time.Time `json:"expires_at" db:"expires_at"`
	OwnerID   string     `json:"owner_id" db:"owner_id"`
	Created   time.Time  `json:"created" db:"created"`
	Updated   time.Time  `json:"updated" db:"updated"`
}

// GroupRole represents the group_roles table
type GroupRole struct {
	TenantID  string     `json:"tenant_id" db:"tenant_id"`
	GroupID   string     `json:"group_id" db:"group_id"`
	RoleID    string     `json:"role_id" db:"role_id"`
	GrantedBy string     `json:"granted_by" db:"granted_by"`
	GrantedAt time.Time  `json:"granted_at" db:"granted_at"`
	ExpiresAt *time.Time `json:"expires_at" db:"expires_at"`
	OwnerID   string     `json:"owner_id" db:"owner_id"`
	Created   time.Time  `json:"created" db:"created"`
	Updated   time.Time  `json:"updated" db:"updated"`
}

// RolePermission represents the role_permissions table
type RolePermission struct {
	TenantID     string    `json:"tenant_id" db:"tenant_id"`
	RoleID       string    `json:"role_id" db:"role_id"`
	PermissionID string    `json:"permission_id" db:"permission_id"`
	OwnerID      string    `json:"owner_id" db:"owner_id"`
	Created      time.Time `json:"created" db:"created"`
	Updated      time.Time `json:"updated" db:"updated"`
}

// RoleTemplate represents the role_templates table
type RoleTemplate struct {
	TemplateID          string                 `json:"template_id" db:"template_id"`
	TemplateName        string                 `json:"template_name" db:"template_name"`
	TemplateDescription string                 `json:"template_description" db:"template_description"`
	TemplateCategory    string                 `json:"template_category" db:"template_category"`
	TemplateMetadata    map[string]interface{} `json:"template_metadata" db:"template_metadata"`
	Created             time.Time              `json:"created" db:"created"`
	Updated             time.Time              `json:"updated" db:"updated"`
}

// TemplatePermission represents the template_permissions table
type TemplatePermission struct {
	TemplateID   string    `json:"template_id" db:"template_id"`
	PermissionID string    `json:"permission_id" db:"permission_id"`
	Created      time.Time `json:"created" db:"created"`
}

// Satellite represents the satellites table
type Satellite struct {
	SatelliteID          string    `json:"satellite_id" db:"satellite_id"`
	TenantID             string    `json:"tenant_id" db:"tenant_id"`
	SatelliteName        string    `json:"satellite_name" db:"satellite_name"`
	SatelliteDescription string    `json:"satellite_description" db:"satellite_description"`
	SatellitePlatform    string    `json:"satellite_platform" db:"satellite_platform"`
	SatelliteVersion     string    `json:"satellite_version" db:"satellite_version"`
	SatelliteRegionID    *string   `json:"satellite_region_id" db:"satellite_region_id"`
	PublicKey            *string   `json:"public_key" db:"public_key"`
	PrivateKey           *string   `json:"private_key" db:"private_key"`
	SatelliteIPAddress   string    `json:"satellite_ip_address" db:"satellite_ip_address"`
	ConnectedToNodeID    string    `json:"connected_to_node_id" db:"connected_to_node_id"`
	OwnerID              string    `json:"owner_id" db:"owner_id"`
	Status               string    `json:"status" db:"status"`
	Created              time.Time `json:"created" db:"created"`
	Updated              time.Time `json:"updated" db:"updated"`
}

// Anchor represents the anchors table
type Anchor struct {
	AnchorID          string    `json:"anchor_id" db:"anchor_id"`
	TenantID          string    `json:"tenant_id" db:"tenant_id"`
	AnchorName        string    `json:"anchor_name" db:"anchor_name"`
	AnchorDescription string    `json:"anchor_description" db:"anchor_description"`
	AnchorPlatform    string    `json:"anchor_platform" db:"anchor_platform"`
	AnchorVersion     string    `json:"anchor_version" db:"anchor_version"`
	AnchorRegionID    *string   `json:"anchor_region_id" db:"anchor_region_id"`
	PublicKey         *string   `json:"public_key" db:"public_key"`
	PrivateKey        *string   `json:"private_key" db:"private_key"`
	AnchorIPAddress   string    `json:"anchor_ip_address" db:"anchor_ip_address"`
	ConnectedToNodeID *string   `json:"connected_to_node_id" db:"connected_to_node_id"`
	OwnerID           string    `json:"owner_id" db:"owner_id"`
	Status            string    `json:"status" db:"status"`
	Created           time.Time `json:"created" db:"created"`
	Updated           time.Time `json:"updated" db:"updated"`
}

// Policy represents the policies table
type Policy struct {
	PolicyID          string                 `json:"policy_id" db:"policy_id"`
	TenantID          string                 `json:"tenant_id" db:"tenant_id"`
	PolicyName        string                 `json:"policy_name" db:"policy_name"`
	PolicyDescription string                 `json:"policy_description" db:"policy_description"`
	PolicyObject      map[string]interface{} `json:"policy_object" db:"policy_object"`
	OwnerID           string                 `json:"owner_id" db:"owner_id"`
	Created           time.Time              `json:"created" db:"created"`
	Updated           time.Time              `json:"updated" db:"updated"`
}

// Workspace represents the workspaces table
type Workspace struct {
	WorkspaceID          string    `json:"workspace_id" db:"workspace_id"`
	TenantID             string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceName        string    `json:"workspace_name" db:"workspace_name"`
	WorkspaceDescription string    `json:"workspace_description" db:"workspace_description"`
	PolicyIDs            []string  `json:"policy_ids" db:"policy_ids"`
	OwnerID              string    `json:"owner_id" db:"owner_id"`
	Status               string    `json:"status" db:"status"`
	Created              time.Time `json:"created" db:"created"`
	Updated              time.Time `json:"updated" db:"updated"`
}

// Environment represents the environments table
type Environment struct {
	EnvironmentID           string    `json:"environment_id" db:"environment_id"`
	TenantID                string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceID             string    `json:"workspace_id" db:"workspace_id"`
	EnvironmentName         string    `json:"environment_name" db:"environment_name"`
	EnvironmentDescription  string    `json:"environment_description" db:"environment_description"`
	EnvironmentIsProduction bool      `json:"environment_is_production" db:"environment_is_production"`
	EnvironmentCriticality  int       `json:"environment_criticality" db:"environment_criticality"`
	EnvironmentPriority     int       `json:"environment_priority" db:"environment_priority"`
	PolicyIDs               []string  `json:"policy_ids" db:"policy_ids"`
	OwnerID                 string    `json:"owner_id" db:"owner_id"`
	Status                  string    `json:"status" db:"status"`
	Created                 time.Time `json:"created" db:"created"`
	Updated                 time.Time `json:"updated" db:"updated"`
}

// Instance represents the instances table
type Instance struct {
	InstanceID               string    `json:"instance_id" db:"instance_id"`
	TenantID                 string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceID              string    `json:"workspace_id" db:"workspace_id"`
	EnvironmentID            *string   `json:"environment_id" db:"environment_id"`
	ConnectedToNodeID        string    `json:"connected_to_node_id" db:"connected_to_node_id"`
	InstanceName             string    `json:"instance_name" db:"instance_name"`
	InstanceDescription      string    `json:"instance_description" db:"instance_description"`
	InstanceType             string    `json:"instance_type" db:"instance_type"`
	InstanceVendor           string    `json:"instance_vendor" db:"instance_vendor"`
	InstanceVersion          string    `json:"instance_version" db:"instance_version"`
	InstanceUniqueIdentifier string    `json:"instance_unique_identifier" db:"instance_unique_identifier"`
	InstanceHost             string    `json:"instance_host" db:"instance_host"`
	InstancePort             int       `json:"instance_port" db:"instance_port"`
	InstanceUsername         string    `json:"instance_username" db:"instance_username"`
	InstancePassword         string    `json:"instance_password" db:"instance_password"`
	InstanceSystemDBName     string    `json:"instance_system_db_name" db:"instance_system_db_name"`
	InstanceEnabled          bool      `json:"instance_enabled" db:"instance_enabled"`
	InstanceSSL              bool      `json:"instance_ssl" db:"instance_ssl"`
	InstanceSSLMode          string    `json:"instance_ssl_mode" db:"instance_ssl_mode"`
	InstanceSSLCert          *string   `json:"instance_ssl_cert" db:"instance_ssl_cert"`
	InstanceSSLKey           *string   `json:"instance_ssl_key" db:"instance_ssl_key"`
	InstanceSSLRootCert      *string   `json:"instance_ssl_root_cert" db:"instance_ssl_root_cert"`
	PolicyIDs                []string  `json:"policy_ids" db:"policy_ids"`
	OwnerID                  string    `json:"owner_id" db:"owner_id"`
	InstanceStatusMessage    string    `json:"instance_status_message" db:"instance_status_message"`
	Status                   string    `json:"status" db:"status"`
	Created                  time.Time `json:"created" db:"created"`
	Updated                  time.Time `json:"updated" db:"updated"`
}

// Database represents the databases table
type Database struct {
	DatabaseID            string    `json:"database_id" db:"database_id"`
	TenantID              string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceID           string    `json:"workspace_id" db:"workspace_id"`
	InstanceID            string    `json:"instance_id" db:"instance_id"`
	DatabaseName          string    `json:"database_name" db:"database_name"`
	DatabaseDescription   string    `json:"database_description" db:"database_description"`
	DatabaseType          string    `json:"database_type" db:"database_type"`
	DatabaseVendor        string    `json:"database_vendor" db:"database_vendor"`
	DatabaseVersion       string    `json:"database_version" db:"database_version"`
	DatabaseUsername      string    `json:"database_username" db:"database_username"`
	DatabasePassword      string    `json:"database_password" db:"database_password"`
	DatabaseDBName        string    `json:"database_db_name" db:"database_db_name"`
	DatabaseEnabled       bool      `json:"database_enabled" db:"database_enabled"`
	PolicyIDs             []string  `json:"policy_ids" db:"policy_ids"`
	OwnerID               string    `json:"owner_id" db:"owner_id"`
	DatabaseStatusMessage string    `json:"database_status_message" db:"database_status_message"`
	Status                string    `json:"status" db:"status"`
	Created               time.Time `json:"created" db:"created"`
	Updated               time.Time `json:"updated" db:"updated"`
}

// Repo represents the repos table
type Repo struct {
	RepoID          string    `json:"repo_id" db:"repo_id"`
	TenantID        string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceID     string    `json:"workspace_id" db:"workspace_id"`
	RepoName        string    `json:"repo_name" db:"repo_name"`
	RepoDescription string    `json:"repo_description" db:"repo_description"`
	PolicyIDs       []string  `json:"policy_ids" db:"policy_ids"`
	OwnerID         string    `json:"owner_id" db:"owner_id"`
	Status          string    `json:"status" db:"status"`
	Created         time.Time `json:"created" db:"created"`
	Updated         time.Time `json:"updated" db:"updated"`
}

// Branch represents the branches table
type Branch struct {
	BranchID            string    `json:"branch_id" db:"branch_id"`
	TenantID            string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceID         string    `json:"workspace_id" db:"workspace_id"`
	RepoID              string    `json:"repo_id" db:"repo_id"`
	BranchName          string    `json:"branch_name" db:"branch_name"`
	ParentBranchID      *string   `json:"parent_branch_id" db:"parent_branch_id"`
	ConnectedToDatabase bool      `json:"connected_to_database" db:"connected_to_database"`
	ConnectedDatabaseID *string   `json:"connected_database_id" db:"connected_database_id"`
	PolicyIDs           []string  `json:"policy_ids" db:"policy_ids"`
	Status              string    `json:"status" db:"status"`
	Created             time.Time `json:"created" db:"created"`
	Updated             time.Time `json:"updated" db:"updated"`
}

// Commit represents the commits table
type Commit struct {
	CommitID        int                    `json:"commit_id" db:"commit_id"`
	TenantID        string                 `json:"tenant_id" db:"tenant_id"`
	WorkspaceID     string                 `json:"workspace_id" db:"workspace_id"`
	RepoID          string                 `json:"repo_id" db:"repo_id"`
	BranchID        string                 `json:"branch_id" db:"branch_id"`
	CommitCode      string                 `json:"commit_code" db:"commit_code"`
	CommitIsHead    bool                   `json:"commit_is_head" db:"commit_is_head"`
	CommitMessage   string                 `json:"commit_message" db:"commit_message"`
	SchemaType      string                 `json:"schema_type" db:"schema_type"`
	SchemaStructure map[string]interface{} `json:"schema_structure" db:"schema_structure"`
	PolicyIDs       []string               `json:"policy_ids" db:"policy_ids"`
	Created         time.Time              `json:"created" db:"created"`
	Updated         time.Time              `json:"updated" db:"updated"`
}

// Mapping represents the mappings table
type Mapping struct {
	MappingID               string                 `json:"mapping_id" db:"mapping_id"`
	TenantID                string                 `json:"tenant_id" db:"tenant_id"`
	WorkspaceID             string                 `json:"workspace_id" db:"workspace_id"`
	MappingName             string                 `json:"mapping_name" db:"mapping_name"`
	MappingDescription      string                 `json:"mapping_description" db:"mapping_description"`
	MappingSourceType       string                 `json:"mapping_source_type" db:"mapping_source_type"`
	MappingTargetType       string                 `json:"mapping_target_type" db:"mapping_target_type"`
	MappingSourceIdentifier string                 `json:"mapping_source_identifier" db:"mapping_source_identifier"`
	MappingTargetIdentifier string                 `json:"mapping_target_identifier" db:"mapping_target_identifier"`
	MappingObject           map[string]interface{} `json:"mapping_object" db:"mapping_object"`
	PolicyIDs               []string               `json:"policy_ids" db:"policy_ids"`
	OwnerID                 string                 `json:"owner_id" db:"owner_id"`
	Created                 time.Time              `json:"created" db:"created"`
	Updated                 time.Time              `json:"updated" db:"updated"`
}

// Relationship represents the relationships table
type Relationship struct {
	RelationshipID               string    `json:"relationship_id" db:"relationship_id"`
	TenantID                     string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceID                  string    `json:"workspace_id" db:"workspace_id"`
	RelationshipName             string    `json:"relationship_name" db:"relationship_name"`
	RelationshipDescription      string    `json:"relationship_description" db:"relationship_description"`
	RelationshipType             string    `json:"relationship_type" db:"relationship_type"`
	RelationshipSourceType       string    `json:"relationship_source_type" db:"relationship_source_type"`
	RelationshipTargetType       string    `json:"relationship_target_type" db:"relationship_target_type"`
	RelationshipSourceIdentifier string    `json:"relationship_source_identifier" db:"relationship_source_identifier"`
	RelationshipTargetIdentifier string    `json:"relationship_target_identifier" db:"relationship_target_identifier"`
	MappingID                    string    `json:"mapping_id" db:"mapping_id"`
	PolicyIDs                    []string  `json:"policy_ids" db:"policy_ids"`
	OwnerID                      string    `json:"owner_id" db:"owner_id"`
	StatusMessage                string    `json:"status_message" db:"status_message"`
	Status                       string    `json:"status" db:"status"`
	Created                      time.Time `json:"created" db:"created"`
	Updated                      time.Time `json:"updated" db:"updated"`
}

// Transformation represents the transformations table
type Transformation struct {
	TransformationID          string    `json:"transformation_id" db:"transformation_id"`
	TenantID                  string    `json:"tenant_id" db:"tenant_id"`
	TransformationName        string    `json:"transformation_name" db:"transformation_name"`
	TransformationDescription string    `json:"transformation_description" db:"transformation_description"`
	TransformationType        string    `json:"transformation_type" db:"transformation_type"`
	TransformationVersion     string    `json:"transformation_version" db:"transformation_version"`
	TransformationFunction    string    `json:"transformation_function" db:"transformation_function"`
	TransformationEnabled     bool      `json:"transformation_enabled" db:"transformation_enabled"`
	OwnerID                   *string   `json:"owner_id" db:"owner_id"`
	Created                   time.Time `json:"created" db:"created"`
	Updated                   time.Time `json:"updated" db:"updated"`
}

// MCPServer represents the mcpservers table
type MCPServer struct {
	MCPServerID          string    `json:"mcpserver_id" db:"mcpserver_id"`
	TenantID             string    `json:"tenant_id" db:"tenant_id"`
	WorkspaceID          string    `json:"workspace_id" db:"workspace_id"`
	MCPServerName        string    `json:"mcpserver_name" db:"mcpserver_name"`
	MCPServerDescription string    `json:"mcpserver_description" db:"mcpserver_description"`
	MCPServerHostIDs     []string  `json:"mcpserver_host_ids" db:"mcpserver_host_ids"`
	MCPServerPort        int       `json:"mcpserver_port" db:"mcpserver_port"`
	MCPServerEnabled     bool      `json:"mcpserver_enabled" db:"mcpserver_enabled"`
	PolicyIDs            []string  `json:"policy_ids" db:"policy_ids"`
	OwnerID              string    `json:"owner_id" db:"owner_id"`
	StatusMessage        string    `json:"status_message" db:"status_message"`
	Status               string    `json:"status" db:"status"`
	Created              time.Time `json:"created" db:"created"`
	Updated              time.Time `json:"updated" db:"updated"`
}

// MCPResource represents the mcpresources table
type MCPResource struct {
	MCPResourceID          string                 `json:"mcpresource_id" db:"mcpresource_id"`
	TenantID               string                 `json:"tenant_id" db:"tenant_id"`
	WorkspaceID            string                 `json:"workspace_id" db:"workspace_id"`
	MCPResourceName        string                 `json:"mcpresource_name" db:"mcpresource_name"`
	MCPResourceDescription string                 `json:"mcpresource_description" db:"mcpresource_description"`
	MCPResourceConfig      map[string]interface{} `json:"mcpresource_config" db:"mcpresource_config"`
	MappingID              string                 `json:"mapping_id" db:"mapping_id"`
	PolicyIDs              []string               `json:"policy_ids" db:"policy_ids"`
	OwnerID                string                 `json:"owner_id" db:"owner_id"`
	Created                time.Time              `json:"created" db:"created"`
	Updated                time.Time              `json:"updated" db:"updated"`
}

// MCPTool represents the mcptools table
type MCPTool struct {
	MCPToolID          string                 `json:"mcptool_id" db:"mcptool_id"`
	TenantID           string                 `json:"tenant_id" db:"tenant_id"`
	WorkspaceID        string                 `json:"workspace_id" db:"workspace_id"`
	MCPToolName        string                 `json:"mcptool_name" db:"mcptool_name"`
	MCPToolDescription string                 `json:"mcptool_description" db:"mcptool_description"`
	MCPToolConfig      map[string]interface{} `json:"mcptool_config" db:"mcptool_config"`
	MappingID          string                 `json:"mapping_id" db:"mapping_id"`
	PolicyIDs          []string               `json:"policy_ids" db:"policy_ids"`
	OwnerID            string                 `json:"owner_id" db:"owner_id"`
	Created            time.Time              `json:"created" db:"created"`
	Updated            time.Time              `json:"updated" db:"updated"`
}

// MCPPrompt represents the mcpprompts table
type MCPPrompt struct {
	MCPPromptID          string                 `json:"mcpprompt_id" db:"mcpprompt_id"`
	TenantID             string                 `json:"tenant_id" db:"tenant_id"`
	WorkspaceID          string                 `json:"workspace_id" db:"workspace_id"`
	MCPPromptName        string                 `json:"mcpprompt_name" db:"mcpprompt_name"`
	MCPPromptDescription string                 `json:"mcpprompt_description" db:"mcpprompt_description"`
	MCPPromptConfig      map[string]interface{} `json:"mcpprompt_config" db:"mcpprompt_config"`
	MappingID            string                 `json:"mapping_id" db:"mapping_id"`
	PolicyIDs            []string               `json:"policy_ids" db:"policy_ids"`
	OwnerID              string                 `json:"owner_id" db:"owner_id"`
	Created              time.Time              `json:"created" db:"created"`
	Updated              time.Time              `json:"updated" db:"updated"`
}

// MCPServerResource represents the mcp_server_resources table
type MCPServerResource struct {
	MCPServerID   string    `json:"mcpserver_id" db:"mcpserver_id"`
	MCPResourceID string    `json:"mcpresource_id" db:"mcpresource_id"`
	Created       time.Time `json:"created" db:"created"`
}

// MCPServerTool represents the mcp_server_tools table
type MCPServerTool struct {
	MCPServerID string    `json:"mcpserver_id" db:"mcpserver_id"`
	MCPToolID   string    `json:"mcptool_id" db:"mcptool_id"`
	Created     time.Time `json:"created" db:"created"`
}

// MCPServerPrompt represents the mcp_server_prompts table
type MCPServerPrompt struct {
	MCPServerID string    `json:"mcpserver_id" db:"mcpserver_id"`
	MCPPromptID string    `json:"mcpprompt_id" db:"mcpprompt_id"`
	Created     time.Time `json:"created" db:"created"`
}

// AuditLog represents the audit_log table
type AuditLog struct {
	AuditID       string                 `json:"audit_id" db:"audit_id"`
	TenantID      string                 `json:"tenant_id" db:"tenant_id"`
	UserID        *string                `json:"user_id" db:"user_id"`
	Action        string                 `json:"action" db:"action"`
	ResourceType  string                 `json:"resource_type" db:"resource_type"`
	ResourceID    *string                `json:"resource_id" db:"resource_id"`
	ResourceName  *string                `json:"resource_name" db:"resource_name"`
	TargetUserID  *string                `json:"target_user_id" db:"target_user_id"`
	ChangeDetails map[string]interface{} `json:"change_details" db:"change_details"`
	IPAddress     *string                `json:"ip_address" db:"ip_address"`
	UserAgent     *string                `json:"user_agent" db:"user_agent"`
	Status        string                 `json:"status" db:"status"`
	Created       time.Time              `json:"created" db:"created"`
}
