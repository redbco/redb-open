package models

import (
	"time"
)

// MappingRule represents the mapping_rules table
type MappingRule struct {
	MappingRuleID           string                 `json:"mapping_rule_id" db:"mapping_rule_id"`
	TenantID                string                 `json:"tenant_id" db:"tenant_id"`
	WorkspaceID             string                 `json:"workspace_id" db:"workspace_id"`
	MappingRuleName         string                 `json:"mapping_rule_name" db:"mapping_rule_name"`
	MappingRuleDescription  string                 `json:"mapping_rule_description" db:"mapping_rule_description"`
	MappingRuleMetadata     map[string]interface{} `json:"mapping_rule_metadata" db:"mapping_rule_metadata"`
	MappingRuleWorkflowType string                 `json:"mapping_rule_workflow_type" db:"mapping_rule_workflow_type"`
	MappingRuleCardinality  string                 `json:"mapping_rule_cardinality" db:"mapping_rule_cardinality"`
	OwnerID                 string                 `json:"owner_id" db:"owner_id"`
	Created                 time.Time              `json:"created" db:"created"`
	Updated                 time.Time              `json:"updated" db:"updated"`
}

// MappingRuleSourceItem represents the mapping_rule_source_items junction table
type MappingRuleSourceItem struct {
	MappingRuleID  string    `json:"mapping_rule_id" db:"mapping_rule_id"`
	ResourceItemID string    `json:"resource_item_id" db:"resource_item_id"`
	ItemOrder      int       `json:"item_order" db:"item_order"`
	Created        time.Time `json:"created" db:"created"`
}

// MappingRuleTargetItem represents the mapping_rule_target_items junction table
type MappingRuleTargetItem struct {
	MappingRuleID  string    `json:"mapping_rule_id" db:"mapping_rule_id"`
	ResourceItemID string    `json:"resource_item_id" db:"resource_item_id"`
	ItemOrder      int       `json:"item_order" db:"item_order"`
	Created        time.Time `json:"created" db:"created"`
}

// MappingFilter represents the mapping_filters table
type MappingFilter struct {
	FilterID         string                 `json:"filter_id" db:"filter_id"`
	MappingID        string                 `json:"mapping_id" db:"mapping_id"`
	FilterType       string                 `json:"filter_type" db:"filter_type"`
	FilterExpression map[string]interface{} `json:"filter_expression" db:"filter_expression"`
	FilterOrder      int                    `json:"filter_order" db:"filter_order"`
	FilterOperator   string                 `json:"filter_operator" db:"filter_operator"`
	Created          time.Time              `json:"created" db:"created"`
	Updated          time.Time              `json:"updated" db:"updated"`
}

// ResourceContainer represents the resource_containers table
type ResourceContainer struct {
	ContainerID       string                 `json:"container_id" db:"container_id"`
	TenantID          string                 `json:"tenant_id" db:"tenant_id"`
	WorkspaceID       string                 `json:"workspace_id" db:"workspace_id"`
	ResourceURI       string                 `json:"resource_uri" db:"resource_uri"`
	Protocol          string                 `json:"protocol" db:"protocol"`
	Scope             string                 `json:"scope" db:"scope"`
	ObjectType        string                 `json:"object_type" db:"object_type"`
	ObjectName        string                 `json:"object_name" db:"object_name"`
	DatabaseID        *string                `json:"database_id" db:"database_id"`
	InstanceID        *string                `json:"instance_id" db:"instance_id"`
	IntegrationID     *string                `json:"integration_id" db:"integration_id"`
	MCPServerID       *string                `json:"mcpserver_id" db:"mcpserver_id"`
	ConnectedToNodeID int64                  `json:"connected_to_node_id" db:"connected_to_node_id"`
	OwnerID           string                 `json:"owner_id" db:"owner_id"`
	Status            string                 `json:"status" db:"status"`
	StatusMessage     string                 `json:"status_message" db:"status_message"`
	LastSeen          time.Time              `json:"last_seen" db:"last_seen"`
	Online            bool                   `json:"online" db:"online"`
	ContainerMetadata map[string]interface{} `json:"container_metadata" db:"container_metadata"`
	EnrichedMetadata  map[string]interface{} `json:"enriched_metadata" db:"enriched_metadata"`
	DatabaseType      *string                `json:"database_type" db:"database_type"`
	Vendor            *string                `json:"vendor" db:"vendor"`
	ItemCount         int                    `json:"item_count" db:"item_count"`
	SizeBytes         int64                  `json:"size_bytes" db:"size_bytes"`
	// Container classification fields (table-level semantic categorization)
	// Examples: 'entity_storage', 'time_series', 'event_log', 'lookup_table', 'audit_log'
	// Only the highest-ranking classification is stored
	ContainerClassification           *string   `json:"container_classification,omitempty" db:"container_classification"`
	ContainerClassificationConfidence *float64  `json:"container_classification_confidence,omitempty" db:"container_classification_confidence"` // 0.00-1.00
	ContainerClassificationSource     string    `json:"container_classification_source" db:"container_classification_source"`                   // 'auto' or 'manual'
	Created                           time.Time `json:"created" db:"created"`
	Updated                           time.Time `json:"updated" db:"updated"`
}

// ResourceItem represents the resource_items table
type ResourceItem struct {
	ItemID                   string                   `json:"item_id" db:"item_id"`
	ContainerID              string                   `json:"container_id" db:"container_id"`
	TenantID                 string                   `json:"tenant_id" db:"tenant_id"`
	WorkspaceID              string                   `json:"workspace_id" db:"workspace_id"`
	ResourceURI              string                   `json:"resource_uri" db:"resource_uri"`
	Protocol                 string                   `json:"protocol" db:"protocol"`
	Scope                    string                   `json:"scope" db:"scope"`
	ItemType                 string                   `json:"item_type" db:"item_type"`
	ItemName                 string                   `json:"item_name" db:"item_name"`
	ItemDisplayName          string                   `json:"item_display_name" db:"item_display_name"`
	ItemPath                 []string                 `json:"item_path" db:"item_path"`
	DataType                 string                   `json:"data_type" db:"data_type"`
	UnifiedDataType          *string                  `json:"unified_data_type" db:"unified_data_type"`
	IsNullable               bool                     `json:"is_nullable" db:"is_nullable"`
	IsPrimaryKey             bool                     `json:"is_primary_key" db:"is_primary_key"`
	IsUnique                 bool                     `json:"is_unique" db:"is_unique"`
	IsIndexed                bool                     `json:"is_indexed" db:"is_indexed"`
	IsRequired               bool                     `json:"is_required" db:"is_required"`
	IsArray                  bool                     `json:"is_array" db:"is_array"`
	ArrayDimensions          int                      `json:"array_dimensions" db:"array_dimensions"`
	DefaultValue             *string                  `json:"default_value" db:"default_value"`
	Constraints              []map[string]interface{} `json:"constraints" db:"constraints"`
	IsCustomType             bool                     `json:"is_custom_type" db:"is_custom_type"`
	CustomTypeName           *string                  `json:"custom_type_name" db:"custom_type_name"`
	CustomTypeDefinition     map[string]interface{}   `json:"custom_type_definition" db:"custom_type_definition"`
	HasSchema                bool                     `json:"has_schema" db:"has_schema"`
	SchemaFormat             *string                  `json:"schema_format" db:"schema_format"`
	SchemaDefinition         map[string]interface{}   `json:"schema_definition" db:"schema_definition"`
	SchemaVersion            *string                  `json:"schema_version" db:"schema_version"`
	SchemaEvolutionVersion   int                      `json:"schema_evolution_version" db:"schema_evolution_version"`
	SchemaValidationMode     string                   `json:"schema_validation_mode" db:"schema_validation_mode"`
	SchemaMismatchAction     string                   `json:"schema_mismatch_action" db:"schema_mismatch_action"`
	AllowNewFields           bool                     `json:"allow_new_fields" db:"allow_new_fields"`
	AllowFieldTypeWidening   bool                     `json:"allow_field_type_widening" db:"allow_field_type_widening"`
	AllowFieldRemoval        bool                     `json:"allow_field_removal" db:"allow_field_removal"`
	SchemaEvolutionLog       []map[string]interface{} `json:"schema_evolution_log" db:"schema_evolution_log"`
	NestedItems              []map[string]interface{} `json:"nested_items" db:"nested_items"`
	MaxLength                *int                     `json:"max_length" db:"max_length"`
	Precision                *int                     `json:"precision" db:"precision"`
	Scale                    *int                     `json:"scale" db:"scale"`
	ConnectedToNodeID        int64                    `json:"connected_to_node_id" db:"connected_to_node_id"`
	Status                   string                   `json:"status" db:"status"`
	Online                   bool                     `json:"online" db:"online"`
	ItemMetadata             map[string]interface{}   `json:"item_metadata" db:"item_metadata"`
	EnrichedMetadata         map[string]interface{}   `json:"enriched_metadata" db:"enriched_metadata"`
	ItemComment              *string                  `json:"item_comment" db:"item_comment"`
	IsPrivileged             bool                     `json:"is_privileged" db:"is_privileged"`
	PrivilegedClassification *string                  `json:"privileged_classification" db:"privileged_classification"`
	DetectionConfidence      *float64                 `json:"detection_confidence" db:"detection_confidence"`
	DetectionMethod          *string                  `json:"detection_method" db:"detection_method"`
	OrdinalPosition          *int                     `json:"ordinal_position" db:"ordinal_position"`
	Created                  time.Time                `json:"created" db:"created"`
	Updated                  time.Time                `json:"updated" db:"updated"`
}

// ResourceContainerFilter represents a filter for querying resource containers
type ResourceContainerFilter struct {
	TenantID      *string
	WorkspaceID   *string
	DatabaseID    *string
	InstanceID    *string
	IntegrationID *string
	MCPServerID   *string
	Protocol      *string
	Scope         *string
	ObjectType    *string
	ObjectName    *string
	Status        *string
	Online        *bool
	NodeID        *int64
	DatabaseType  *string
	Limit         *int
	Offset        *int
}

// ResourceItemFilter represents a filter for querying resource items
type ResourceItemFilter struct {
	TenantID                 *string
	WorkspaceID              *string
	ContainerID              *string
	Protocol                 *string
	Scope                    *string
	ItemType                 *string
	ItemName                 *string
	DataType                 *string
	UnifiedDataType          *string
	IsPrimaryKey             *bool
	IsUnique                 *bool
	IsIndexed                *bool
	IsRequired               *bool
	IsArray                  *bool
	IsPrivileged             *bool
	PrivilegedClassification *string
	IsCustomType             *bool
	HasSchema                *bool
	SchemaFormat             *string
	Online                   *bool
	Status                   *string
	NodeID                   *int64
	Limit                    *int
	Offset                   *int
}
