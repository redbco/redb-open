package engine

// ResourceItem represents a resource item with all its details
type ResourceItem struct {
	ItemID                   string   `json:"item_id"`
	ContainerID              string   `json:"container_id"`
	TenantID                 string   `json:"tenant_id,omitempty"`
	WorkspaceID              string   `json:"workspace_id,omitempty"`
	ResourceURI              string   `json:"resource_uri"`
	Protocol                 string   `json:"protocol,omitempty"`
	Scope                    string   `json:"scope,omitempty"`
	ItemType                 string   `json:"item_type"`
	ItemName                 string   `json:"item_name"`
	ItemDisplayName          string   `json:"item_display_name,omitempty"`
	ItemPath                 []string `json:"item_path,omitempty"`
	DataType                 string   `json:"data_type"`
	UnifiedDataType          *string  `json:"unified_data_type,omitempty"`
	IsNullable               bool     `json:"is_nullable"`
	IsPrimaryKey             bool     `json:"is_primary_key"`
	IsUnique                 bool     `json:"is_unique"`
	IsIndexed                bool     `json:"is_indexed"`
	IsRequired               bool     `json:"is_required"`
	IsArray                  bool     `json:"is_array"`
	ArrayDimensions          int      `json:"array_dimensions,omitempty"`
	DefaultValue             *string  `json:"default_value,omitempty"`
	MaxLength                *int     `json:"max_length,omitempty"`
	Precision                *int     `json:"precision,omitempty"`
	Scale                    *int     `json:"scale,omitempty"`
	Description              *string  `json:"description,omitempty"`
	IsPrivileged             bool     `json:"is_privileged"`
	PrivilegedClassification *string  `json:"privileged_classification,omitempty"`
	DetectionConfidence      *float64 `json:"detection_confidence,omitempty"`
	DetectionMethod          *string  `json:"detection_method,omitempty"`
	Created                  string   `json:"created,omitempty"`
	Updated                  string   `json:"updated,omitempty"`
}

// RelationshipInfo represents relationship name and status
type RelationshipInfo struct {
	RelationshipName string `json:"relationship_name"`
	Status           Status `json:"status"`
}

// Mapping represents a mapping
type Mapping struct {
	TenantID           string             `json:"tenant_id"`
	WorkspaceID        string             `json:"workspace_id"`
	MappingID          string             `json:"mapping_id"`
	MappingName        string             `json:"mapping_name"`
	MappingDescription string             `json:"mapping_description,omitempty"`
	MappingType        string             `json:"mapping_type,omitempty"`
	PolicyID           string             `json:"policy_id,omitempty"`
	OwnerID            string             `json:"owner_id"`
	MappingRuleCount   int32              `json:"mapping_rule_count"`
	Validated          bool               `json:"validated"`
	ValidatedAt        string             `json:"validated_at,omitempty"`
	ValidationErrors   []string           `json:"validation_errors,omitempty"`
	ValidationWarnings []string           `json:"validation_warnings,omitempty"`
	MappingSourceType  string             `json:"mapping_source_type,omitempty"`
	MappingTargetType  string             `json:"mapping_target_type,omitempty"`
	MappingSource      string             `json:"mapping_source,omitempty"`
	MappingTarget      string             `json:"mapping_target,omitempty"`
	SourceDatabaseID   string             `json:"source_database_id,omitempty"`
	SourceDatabaseName string             `json:"source_database_name,omitempty"`
	SourceTableName    string             `json:"source_table_name,omitempty"`
	TargetDatabaseID   string             `json:"target_database_id,omitempty"`
	TargetDatabaseName string             `json:"target_database_name,omitempty"`
	TargetTableName    string             `json:"target_table_name,omitempty"`
	RelationshipNames  []string           `json:"relationship_names,omitempty"`
	RelationshipInfos  []RelationshipInfo `json:"relationship_infos,omitempty"`
	MCPResourceNames   []string           `json:"mcp_resource_names,omitempty"`
	MCPToolNames       []string           `json:"mcp_tool_names,omitempty"`
}

type MappingWithRules struct {
	TenantID             string                 `json:"tenant_id"`
	WorkspaceID          string                 `json:"workspace_id"`
	MappingID            string                 `json:"mapping_id"`
	MappingName          string                 `json:"mapping_name"`
	MappingDescription   string                 `json:"mapping_description,omitempty"`
	MappingType          string                 `json:"mapping_type,omitempty"`
	PolicyID             string                 `json:"policy_id,omitempty"`
	OwnerID              string                 `json:"owner_id"`
	MappingRules         []MappingRuleInMapping `json:"mapping_rules"`
	Validated            bool                   `json:"validated"`
	ValidatedAt          string                 `json:"validated_at,omitempty"`
	ValidationErrors     []string               `json:"validation_errors,omitempty"`
	ValidationWarnings   []string               `json:"validation_warnings,omitempty"`
	MappingSourceType    string                 `json:"mapping_source_type,omitempty"`
	MappingTargetType    string                 `json:"mapping_target_type,omitempty"`
	MappingSource        string                 `json:"mapping_source,omitempty"`
	MappingTarget        string                 `json:"mapping_target,omitempty"`
	SourceDatabaseID     string                 `json:"source_database_id,omitempty"`
	SourceDatabaseName   string                 `json:"source_database_name,omitempty"`
	SourceTableName      string                 `json:"source_table_name,omitempty"`
	TargetDatabaseID     string                 `json:"target_database_id,omitempty"`
	TargetDatabaseName   string                 `json:"target_database_name,omitempty"`
	TargetTableName      string                 `json:"target_table_name,omitempty"`
	RelationshipNames    []string               `json:"relationship_names,omitempty"`
	RelationshipInfos    []RelationshipInfo     `json:"relationship_infos,omitempty"`
	MCPResourceNames     []string               `json:"mcp_resource_names,omitempty"`
	MCPToolNames         []string               `json:"mcp_tool_names,omitempty"`
	SourceContainerItems []ResourceItem         `json:"source_container_items,omitempty"`
	TargetContainerItems []ResourceItem         `json:"target_container_items,omitempty"`
}

type ListMappingsResponse struct {
	Mappings []Mapping `json:"mappings"`
}

type ShowMappingResponse struct {
	Mapping MappingWithRules `json:"mapping"`
}

type AddMappingRequest struct {
	MappingName        string `json:"mapping_name" validate:"required"`
	MappingDescription string `json:"mapping_description" validate:"required"`
	Scope              string `json:"scope" validate:"required,oneof=database table"`
	Source             string `json:"source" validate:"required"`
	Target             string `json:"target" validate:"required"`
	PolicyID           string `json:"policy_id,omitempty"`
	GenerateRules      *bool  `json:"generate_rules,omitempty"` // Defaults to true if not provided
}

type AddMappingResponse struct {
	Message string  `json:"message"`
	Success bool    `json:"success"`
	Mapping Mapping `json:"mapping"`
	Status  Status  `json:"status"`
}

type AddDatabaseMappingRequest struct {
	MappingName               string `json:"mapping_name" validate:"required"`
	MappingDescription        string `json:"mapping_description" validate:"required"`
	MappingSourceDatabaseName string `json:"mapping_source_database_name" validate:"required"`
	MappingTargetDatabaseName string `json:"mapping_target_database_name" validate:"required"`
	PolicyID                  string `json:"policy_id,omitempty"`
}

type AddDatabaseMappingResponse struct {
	Message string  `json:"message"`
	Success bool    `json:"success"`
	Mapping Mapping `json:"mapping"`
	Status  Status  `json:"status"`
}

type AddTableMappingRequest struct {
	MappingName               string `json:"mapping_name" validate:"required"`
	MappingDescription        string `json:"mapping_description" validate:"required"`
	MappingSourceDatabaseName string `json:"mapping_source_database_name" validate:"required"`
	MappingSourceTableName    string `json:"mapping_source_table_name" validate:"required"`
	MappingTargetDatabaseName string `json:"mapping_target_database_name" validate:"required"`
	MappingTargetTableName    string `json:"mapping_target_table_name" validate:"required"`
	PolicyID                  string `json:"policy_id,omitempty"`
}

type AddTableMappingWithDeployRequest struct {
	MappingName        string `json:"mapping_name"`
	MappingDescription string `json:"mapping_description"`
	SourceDatabaseName string `json:"source_database_name"`
	SourceTableName    string `json:"source_table_name"`
	TargetDatabaseName string `json:"target_database_name"`
	TargetTableName    string `json:"target_table_name"`
	PolicyID           string `json:"policy_id,omitempty"`
}

type DeploymentInfo struct {
	TableDeployed bool     `json:"table_deployed"`
	TypesDeployed []string `json:"types_deployed"`
}

type AddTableMappingWithDeployResponse struct {
	Message        string         `json:"message"`
	Success        bool           `json:"success"`
	Status         Status         `json:"status"`
	Mapping        Mapping        `json:"mapping"`
	DeploymentInfo DeploymentInfo `json:"deployment_info"`
}

type AddTableMappingResponse struct {
	Message string  `json:"message"`
	Success bool    `json:"success"`
	Mapping Mapping `json:"mapping"`
	Status  Status  `json:"status"`
}

type ModifyMappingRequest struct {
	MappingNameNew     string `json:"mapping_name_new,omitempty"`
	MappingDescription string `json:"mapping_description,omitempty"`
	PolicyID           string `json:"policy_id,omitempty"`
}

type ModifyMappingResponse struct {
	Message string  `json:"message"`
	Success bool    `json:"success"`
	Mapping Mapping `json:"mapping"`
	Status  Status  `json:"status"`
}

type DeleteMappingResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// MappingRule represents a mapping rule
type MappingRule struct {
	TenantID                         string      `json:"tenant_id"`
	WorkspaceID                      string      `json:"workspace_id"`
	MappingRuleID                    string      `json:"mapping_rule_id"`
	MappingRuleName                  string      `json:"mapping_rule_name"`
	MappingRuleDescription           string      `json:"mapping_rule_description,omitempty"`
	MappingRuleMetadata              interface{} `json:"mapping_rule_metadata,omitempty"`
	MappingRuleSource                string      `json:"mapping_rule_source"`
	MappingRuleTarget                string      `json:"mapping_rule_target"`
	MappingRuleTransformationID      string      `json:"mapping_rule_transformation_id"`
	MappingRuleTransformationName    string      `json:"mapping_rule_transformation_name"`
	MappingRuleTransformationOptions string      `json:"mapping_rule_transformation_options,omitempty"`
	OwnerID                          string      `json:"owner_id"`
	MappingCount                     int32       `json:"mapping_count"`
	Mappings                         []Mapping   `json:"mappings"`
}

// MappingRule represents a mapping rule
type MappingRuleInMapping struct {
	MappingRuleID                    string         `json:"mapping_rule_id"`
	MappingRuleName                  string         `json:"mapping_rule_name"`
	MappingRuleDescription           string         `json:"mapping_rule_description,omitempty"`
	MappingRuleMetadata              interface{}    `json:"mapping_rule_metadata,omitempty"`
	MappingRuleSource                string         `json:"mapping_rule_source"`
	MappingRuleTarget                string         `json:"mapping_rule_target"`
	MappingRuleTransformationID      string         `json:"mapping_rule_transformation_id"`
	MappingRuleTransformationName    string         `json:"mapping_rule_transformation_name"`
	MappingRuleTransformationOptions string         `json:"mapping_rule_transformation_options,omitempty"`
	SourceItems                      []ResourceItem `json:"source_items,omitempty"`
	TargetItems                      []ResourceItem `json:"target_items,omitempty"`
}

type ListMappingRulesResponse struct {
	MappingRules []MappingRule `json:"mapping_rules"`
}

type ShowMappingRuleResponse struct {
	MappingRule MappingRule `json:"mapping_rule"`
}

type AddMappingRuleRequest struct {
	MappingRuleName                  string `json:"mapping_rule_name" validate:"required"`
	MappingRuleDescription           string `json:"mapping_rule_description" validate:"required"`
	MappingRuleSource                string `json:"mapping_rule_source" validate:"required"`
	MappingRuleTarget                string `json:"mapping_rule_target" validate:"required"`
	MappingRuleTransformationName    string `json:"mapping_rule_transformation_name" validate:"required"`
	MappingRuleTransformationOptions string `json:"mapping_rule_transformation_options,omitempty"`
}

type AddMappingRuleResponse struct {
	Message     string      `json:"message"`
	Success     bool        `json:"success"`
	MappingRule MappingRule `json:"mapping_rule"`
	Status      Status      `json:"status"`
}

type ModifyMappingRuleRequest struct {
	MappingRuleNameNew               string `json:"mapping_rule_name_new,omitempty"`
	MappingRuleDescription           string `json:"mapping_rule_description,omitempty"`
	MappingRuleSource                string `json:"mapping_rule_source,omitempty"`
	MappingRuleTarget                string `json:"mapping_rule_target,omitempty"`
	MappingRuleTransformationName    string `json:"mapping_rule_transformation_name,omitempty"`
	MappingRuleTransformationOptions string `json:"mapping_rule_transformation_options,omitempty"`
}

type ModifyMappingRuleResponse struct {
	Message     string      `json:"message"`
	Success     bool        `json:"success"`
	MappingRule MappingRule `json:"mapping_rule"`
	Status      Status      `json:"status"`
}

type DeleteMappingRuleResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type AttachMappingRuleRequest struct {
	MappingRuleName  string `json:"mapping_rule_name" validate:"required"`
	MappingRuleOrder *int64 `json:"mapping_rule_order,omitempty"`
}

type AttachMappingRuleResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type DetachMappingRuleRequest struct {
	MappingRuleName string `json:"mapping_rule_name" validate:"required"`
}

type DetachMappingRuleResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// New models for mapping rule operations within mappings

type AddRuleToMappingRequest struct {
	RuleName       string `json:"rule_name" validate:"required"`
	Source         string `json:"source" validate:"required"`
	Target         string `json:"target" validate:"required"`
	Transformation string `json:"transformation" validate:"required"`
	Order          *int32 `json:"order,omitempty"`
}

type AddRuleToMappingResponse struct {
	Message string      `json:"message"`
	Success bool        `json:"success"`
	Rule    MappingRule `json:"rule"`
	Status  Status      `json:"status"`
}

type ModifyRuleInMappingRequest struct {
	Source         *string `json:"source,omitempty"`
	Target         *string `json:"target,omitempty"`
	Transformation *string `json:"transformation,omitempty"`
	Order          *int32  `json:"order,omitempty"`
}

type ModifyRuleInMappingResponse struct {
	Message string      `json:"message"`
	Success bool        `json:"success"`
	Rule    MappingRule `json:"rule"`
	Status  Status      `json:"status"`
}

type RemoveRuleFromMappingResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

type ListRulesInMappingResponse struct {
	Rules []MappingRuleInMapping `json:"rules"`
}

// ValidateMappingResponse represents the response for validating a mapping
type ValidateMappingResponse struct {
	IsValid     bool     `json:"is_valid"`
	Errors      []string `json:"errors"`
	Warnings    []string `json:"warnings"`
	ValidatedAt string   `json:"validated_at"`
}
