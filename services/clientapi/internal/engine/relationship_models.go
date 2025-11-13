package engine

// Relationship represents a relationship
type Relationship struct {
	TenantID                     string `json:"tenant_id"`
	WorkspaceID                  string `json:"workspace_id"`
	RelationshipID               string `json:"relationship_id"`
	RelationshipName             string `json:"relationship_name"`
	RelationshipDescription      string `json:"relationship_description,omitempty"`
	RelationshipType             string `json:"relationship_type"`
	RelationshipSourceType       string `json:"relationship_source_type"`
	RelationshipTargetType       string `json:"relationship_target_type"`
	RelationshipSourceDatabaseID string `json:"relationship_source_database_id"`
	RelationshipSourceTableName  string `json:"relationship_source_table_name"`
	RelationshipTargetDatabaseID string `json:"relationship_target_database_id"`
	RelationshipTargetTableName  string `json:"relationship_target_table_name"`
	MappingID                    string `json:"mapping_id"`
	MappingName                  string `json:"mapping_name,omitempty"`
	PolicyID                     string `json:"policy_id"`
	StatusMessage                string `json:"status_message"`
	Status                       Status `json:"status"`
	OwnerID                      string `json:"owner_id"`
	RelationshipSourceDatabaseName string `json:"relationship_source_database_name,omitempty"`
	RelationshipTargetDatabaseName string `json:"relationship_target_database_name,omitempty"`
	RelationshipSourceDatabaseType string `json:"relationship_source_database_type,omitempty"`
	RelationshipTargetDatabaseType string `json:"relationship_target_database_type,omitempty"`
}

type ListRelationshipsResponse struct {
	Relationships []Relationship `json:"relationships"`
}

type ShowRelationshipResponse struct {
	Relationship Relationship `json:"relationship"`
}

type AddRelationshipRequest struct {
	RelationshipName             string `json:"relationship_name" validate:"required"`
	RelationshipDescription      string `json:"relationship_description" validate:"required"`
	RelationshipType             string `json:"relationship_type" validate:"required"`
	RelationshipSourceType       string `json:"relationship_source_type,omitempty"`
	RelationshipTargetType       string `json:"relationship_target_type,omitempty"`
	RelationshipSourceDatabaseID string `json:"relationship_source_database_id" validate:"required"`
	RelationshipSourceTableName  string `json:"relationship_source_table_name" validate:"required"`
	RelationshipTargetDatabaseID string `json:"relationship_target_database_id" validate:"required"`
	RelationshipTargetTableName  string `json:"relationship_target_table_name" validate:"required"`
	MappingID                    string `json:"mapping_id" validate:"required"`
	PolicyID                     string `json:"policy_id,omitempty"`
}

type AddRelationshipResponse struct {
	Message      string       `json:"message"`
	Success      bool         `json:"success"`
	Relationship Relationship `json:"relationship"`
	Status       Status       `json:"status"`
}

type ModifyRelationshipRequest struct {
	RelationshipNameNew          string `json:"relationship_name_new,omitempty"`
	RelationshipDescription      string `json:"relationship_description,omitempty"`
	RelationshipType             string `json:"relationship_type,omitempty"`
	RelationshipSourceType       string `json:"relationship_source_type,omitempty"`
	RelationshipTargetType       string `json:"relationship_target_type,omitempty"`
	RelationshipSourceDatabaseID string `json:"relationship_source_database_id,omitempty"`
	RelationshipSourceTableName  string `json:"relationship_source_table_name,omitempty"`
	RelationshipTargetDatabaseID string `json:"relationship_target_database_id,omitempty"`
	RelationshipTargetTableName  string `json:"relationship_target_table_name,omitempty"`
	MappingID                    string `json:"mapping_id,omitempty"`
	PolicyID                     string `json:"policy_id,omitempty"`
}

type ModifyRelationshipResponse struct {
	Message      string       `json:"message"`
	Success      bool         `json:"success"`
	Relationship Relationship `json:"relationship"`
	Status       Status       `json:"status"`
}

type DeleteRelationshipResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}
