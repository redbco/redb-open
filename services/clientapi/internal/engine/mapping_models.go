package engine

// Mapping represents a mapping
type Mapping struct {
	TenantID           string `json:"tenant_id"`
	WorkspaceID        string `json:"workspace_id"`
	MappingID          string `json:"mapping_id"`
	MappingName        string `json:"mapping_name"`
	MappingDescription string `json:"mapping_description,omitempty"`
	MappingType        string `json:"mapping_type,omitempty"`
	PolicyID           string `json:"policy_id,omitempty"`
	OwnerID            string `json:"owner_id"`
	MappingRuleCount   int32  `json:"mapping_rule_count"`
}

type MappingWithRules struct {
	TenantID           string                 `json:"tenant_id"`
	WorkspaceID        string                 `json:"workspace_id"`
	MappingID          string                 `json:"mapping_id"`
	MappingName        string                 `json:"mapping_name"`
	MappingDescription string                 `json:"mapping_description,omitempty"`
	MappingType        string                 `json:"mapping_type,omitempty"`
	PolicyID           string                 `json:"policy_id,omitempty"`
	OwnerID            string                 `json:"owner_id"`
	MappingRules       []MappingRuleInMapping `json:"mapping_rules"`
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
	MappingRuleID                    string      `json:"mapping_rule_id"`
	MappingRuleName                  string      `json:"mapping_rule_name"`
	MappingRuleDescription           string      `json:"mapping_rule_description,omitempty"`
	MappingRuleMetadata              interface{} `json:"mapping_rule_metadata,omitempty"`
	MappingRuleSource                string      `json:"mapping_rule_source"`
	MappingRuleTarget                string      `json:"mapping_rule_target"`
	MappingRuleTransformationID      string      `json:"mapping_rule_transformation_id"`
	MappingRuleTransformationName    string      `json:"mapping_rule_transformation_name"`
	MappingRuleTransformationOptions string      `json:"mapping_rule_transformation_options,omitempty"`
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
