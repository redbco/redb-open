package relationship

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles relationship-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new relationship service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Relationship represents a relationship in the system
type Relationship struct {
	ID               string
	TenantID         string
	WorkspaceID      string
	Name             string
	Description      string
	Type             string
	SourceType       string
	TargetType       string
	SourceDatabaseID string
	SourceTableName  string
	TargetDatabaseID string
	TargetTableName  string
	MappingID        string
	PolicyIDs        []string
	OwnerID          string
	StatusMessage    string
	Status           string
	Created          time.Time
	Updated          time.Time
}

// Create creates a new relationship
func (s *Service) Create(ctx context.Context, tenantID, workspaceID, name, description, relationshipType, sourceType, targetType, sourceDatabaseID, sourceTableName, targetDatabaseID, targetTableName, mappingID, ownerID string) (*Relationship, error) {
	s.logger.Infof("Creating relationship in database for tenant: %s, workspace: %s, name: %s", tenantID, workspaceID, name)

	// Check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if the workspace exists and belongs to the tenant
	var workspaceExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = $1 AND tenant_id = $2)", workspaceID, tenantID).Scan(&workspaceExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check workspace existence: %w", err)
	}
	if !workspaceExists {
		return nil, errors.New("workspace not found in tenant")
	}

	// Check if mapping exists
	var mappingExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mappings WHERE tenant_id = $1 AND workspace_id = $2 AND mapping_id = $3)", tenantID, workspaceID, mappingID).Scan(&mappingExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check mapping existence: %w", err)
	}
	if !mappingExists {
		return nil, errors.New("mapping not found")
	}

	// Check if relationship with the same name already exists in this workspace
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM relationships WHERE tenant_id = $1 AND workspace_id = $2 AND relationship_name = $3)", tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check relationship existence: %w", err)
	}
	if exists {
		return nil, errors.New("relationship with this name already exists in the workspace")
	}

	// Insert the relationship into the database
	query := `
		INSERT INTO relationships (tenant_id, workspace_id, relationship_name, relationship_description, 
		                          relationship_type, relationship_source_type, relationship_target_type,
		                          relationship_source_database_id, relationship_source_table_name,
		                          relationship_target_database_id, relationship_target_table_name,
		                          mapping_id, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING relationship_id, tenant_id, workspace_id, relationship_name, relationship_description, 
		          relationship_type, relationship_source_type, relationship_target_type,
		          relationship_source_database_id, relationship_source_table_name,
		          relationship_target_database_id, relationship_target_table_name, mapping_id,
		          COALESCE(policy_ids, '{}') as policy_ids, owner_id, status_message, status, created, updated
	`

	var relationship Relationship
	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name, description, relationshipType, sourceType, targetType, sourceDatabaseID, sourceTableName, targetDatabaseID, targetTableName, mappingID, ownerID).Scan(
		&relationship.ID,
		&relationship.TenantID,
		&relationship.WorkspaceID,
		&relationship.Name,
		&relationship.Description,
		&relationship.Type,
		&relationship.SourceType,
		&relationship.TargetType,
		&relationship.SourceDatabaseID,
		&relationship.SourceTableName,
		&relationship.TargetDatabaseID,
		&relationship.TargetTableName,
		&relationship.MappingID,
		&relationship.PolicyIDs,
		&relationship.OwnerID,
		&relationship.StatusMessage,
		&relationship.Status,
		&relationship.Created,
		&relationship.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create relationship: %v", err)
		return nil, err
	}

	return &relationship, nil
}

// Get retrieves a relationship by ID
func (s *Service) Get(ctx context.Context, tenantID, workspaceID, id string) (*Relationship, error) {
	s.logger.Infof("Retrieving relationship from database with ID: %s", id)
	query := `
		SELECT relationship_id, tenant_id, workspace_id, relationship_name, relationship_description, 
		       relationship_type, relationship_source_type, relationship_target_type,
		       relationship_source_database_id, relationship_source_table_name,
		       relationship_target_database_id, relationship_target_table_name, mapping_id,
		       COALESCE(policy_ids, '{}') as policy_ids, owner_id, status_message, status, created, updated
		FROM relationships
		WHERE tenant_id = $1 AND workspace_id = $2 AND relationship_id = $3
	`

	var relationship Relationship
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, id).Scan(
		&relationship.ID,
		&relationship.TenantID,
		&relationship.WorkspaceID,
		&relationship.Name,
		&relationship.Description,
		&relationship.Type,
		&relationship.SourceType,
		&relationship.TargetType,
		&relationship.SourceDatabaseID,
		&relationship.SourceTableName,
		&relationship.TargetDatabaseID,
		&relationship.TargetTableName,
		&relationship.MappingID,
		&relationship.PolicyIDs,
		&relationship.OwnerID,
		&relationship.StatusMessage,
		&relationship.Status,
		&relationship.Created,
		&relationship.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("relationship not found")
		}
		s.logger.Errorf("Failed to get relationship: %v", err)
		return nil, err
	}

	return &relationship, nil
}

// List retrieves all relationships for a workspace
func (s *Service) List(ctx context.Context, tenantID, workspaceID string) ([]*Relationship, error) {
	s.logger.Infof("Listing relationships for tenant: %s, workspace: %s", tenantID, workspaceID)
	query := `
		SELECT relationship_id, tenant_id, workspace_id, relationship_name, relationship_description, 
		       relationship_type, relationship_source_type, relationship_target_type,
		       relationship_source_database_id, relationship_source_table_name,
		       relationship_target_database_id, relationship_target_table_name, mapping_id,
		       COALESCE(policy_ids, '{}') as policy_ids, owner_id, status_message, status, created, updated
		FROM relationships
		WHERE tenant_id = $1 AND workspace_id = $2
		ORDER BY relationship_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID)
	if err != nil {
		s.logger.Errorf("Failed to list relationships: %v", err)
		return nil, err
	}
	defer rows.Close()

	var relationships []*Relationship
	for rows.Next() {
		var relationship Relationship
		err := rows.Scan(
			&relationship.ID,
			&relationship.TenantID,
			&relationship.WorkspaceID,
			&relationship.Name,
			&relationship.Description,
			&relationship.Type,
			&relationship.SourceType,
			&relationship.TargetType,
			&relationship.SourceDatabaseID,
			&relationship.SourceTableName,
			&relationship.TargetDatabaseID,
			&relationship.TargetTableName,
			&relationship.MappingID,
			&relationship.PolicyIDs,
			&relationship.OwnerID,
			&relationship.StatusMessage,
			&relationship.Status,
			&relationship.Created,
			&relationship.Updated,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan relationship: %v", err)
			return nil, err
		}
		relationships = append(relationships, &relationship)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning relationships: %v", err)
		return nil, err
	}

	return relationships, nil
}

// Update updates a relationship
func (s *Service) Update(ctx context.Context, tenantID, workspaceID, id string, updates map[string]interface{}) (*Relationship, error) {
	s.logger.Infof("Updating relationship with ID: %s", id)

	if len(updates) == 0 {
		return s.Get(ctx, tenantID, workspaceID, id)
	}

	// Check if relationship exists
	_, err := s.Get(ctx, tenantID, workspaceID, id)
	if err != nil {
		return nil, err
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, workspaceID, id}
	argIndex := 4

	for field, value := range updates {
		switch field {
		case "relationship_name", "relationship_description", "relationship_type",
			"relationship_source_type", "relationship_target_type",
			"relationship_source_database_id", "relationship_source_table_name",
			"relationship_target_database_id", "relationship_target_table_name",
			"mapping_id", "status_message", "status":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		default:
			s.logger.Warnf("Ignoring invalid update field: %s", field)
		}
	}

	if len(setParts) == 0 {
		return s.Get(ctx, tenantID, workspaceID, id)
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE relationships 
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND relationship_id = $3
		RETURNING relationship_id, tenant_id, workspace_id, relationship_name, relationship_description, 
		          relationship_type, relationship_source_type, relationship_target_type,
		          relationship_source_database_id, relationship_source_table_name,
		          relationship_target_database_id, relationship_target_table_name, mapping_id,
		          COALESCE(policy_ids, '{}') as policy_ids, owner_id, status_message, status, created, updated
	`, setClause)

	var relationship Relationship
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&relationship.ID,
		&relationship.TenantID,
		&relationship.WorkspaceID,
		&relationship.Name,
		&relationship.Description,
		&relationship.Type,
		&relationship.SourceType,
		&relationship.TargetType,
		&relationship.SourceDatabaseID,
		&relationship.SourceTableName,
		&relationship.TargetDatabaseID,
		&relationship.TargetTableName,
		&relationship.MappingID,
		&relationship.PolicyIDs,
		&relationship.OwnerID,
		&relationship.StatusMessage,
		&relationship.Status,
		&relationship.Created,
		&relationship.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("relationship not found")
		}
		s.logger.Errorf("Failed to update relationship: %v", err)
		return nil, err
	}

	return &relationship, nil
}

// Delete deletes a relationship
func (s *Service) Delete(ctx context.Context, tenantID, workspaceID, id string) error {
	s.logger.Infof("Deleting relationship with ID: %s", id)

	// Check if relationship exists
	_, err := s.Get(ctx, tenantID, workspaceID, id)
	if err != nil {
		return err
	}

	// Begin transaction for cascading deletes
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Delete related replication sources
	_, err = tx.Exec(ctx, "DELETE FROM replication_sources WHERE tenant_id = $1 AND workspace_id = $2 AND relationship_id = $3",
		tenantID, workspaceID, id)
	if err != nil {
		return fmt.Errorf("failed to delete replication sources: %w", err)
	}

	// Delete relationship
	result, err := tx.Exec(ctx, "DELETE FROM relationships WHERE tenant_id = $1 AND workspace_id = $2 AND relationship_id = $3",
		tenantID, workspaceID, id)
	if err != nil {
		return fmt.Errorf("failed to delete relationship: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("relationship not found")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetByName retrieves a relationship by name
func (s *Service) GetByName(ctx context.Context, tenantID, workspaceID, name string) (*Relationship, error) {
	s.logger.Infof("Retrieving relationship from database with name: %s", name)
	query := `
		SELECT relationship_id, tenant_id, workspace_id, relationship_name, relationship_description, 
		       relationship_type, relationship_source_type, relationship_target_type,
		       relationship_source_database_id, relationship_source_table_name,
		       relationship_target_database_id, relationship_target_table_name, mapping_id,
		       COALESCE(policy_ids, '{}') as policy_ids, owner_id, status_message, status, created, updated
		FROM relationships
		WHERE tenant_id = $1 AND workspace_id = $2 AND relationship_name = $3
	`

	var relationship Relationship
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name).Scan(
		&relationship.ID,
		&relationship.TenantID,
		&relationship.WorkspaceID,
		&relationship.Name,
		&relationship.Description,
		&relationship.Type,
		&relationship.SourceType,
		&relationship.TargetType,
		&relationship.SourceDatabaseID,
		&relationship.SourceTableName,
		&relationship.TargetDatabaseID,
		&relationship.TargetTableName,
		&relationship.MappingID,
		&relationship.PolicyIDs,
		&relationship.OwnerID,
		&relationship.StatusMessage,
		&relationship.Status,
		&relationship.Created,
		&relationship.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("relationship not found")
		}
		s.logger.Errorf("Failed to get relationship: %v", err)
		return nil, err
	}

	return &relationship, nil
}

// UpdateByName updates a relationship by name
func (s *Service) UpdateByName(ctx context.Context, tenantID, workspaceID, name string, updates map[string]interface{}) (*Relationship, error) {
	s.logger.Infof("Updating relationship with name: %s", name)

	if len(updates) == 0 {
		return s.GetByName(ctx, tenantID, workspaceID, name)
	}

	// Check if relationship exists
	_, err := s.GetByName(ctx, tenantID, workspaceID, name)
	if err != nil {
		return nil, err
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, workspaceID, name}
	argIndex := 4

	for field, value := range updates {
		switch field {
		case "relationship_name", "relationship_description", "relationship_type",
			"relationship_source_type", "relationship_target_type",
			"relationship_source_database_id", "relationship_source_table_name",
			"relationship_target_database_id", "relationship_target_table_name",
			"mapping_id", "status_message", "status":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		default:
			s.logger.Warnf("Ignoring invalid update field: %s", field)
		}
	}

	if len(setParts) == 0 {
		return s.GetByName(ctx, tenantID, workspaceID, name)
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE relationships 
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND relationship_name = $3
		RETURNING relationship_id, tenant_id, workspace_id, relationship_name, relationship_description, 
		          relationship_type, relationship_source_type, relationship_target_type,
		          relationship_source_database_id, relationship_source_table_name,
		          relationship_target_database_id, relationship_target_table_name, mapping_id,
		          COALESCE(policy_ids, '{}') as policy_ids, owner_id, status_message, status, created, updated
	`, setClause)

	var relationship Relationship
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&relationship.ID,
		&relationship.TenantID,
		&relationship.WorkspaceID,
		&relationship.Name,
		&relationship.Description,
		&relationship.Type,
		&relationship.SourceType,
		&relationship.TargetType,
		&relationship.SourceDatabaseID,
		&relationship.SourceTableName,
		&relationship.TargetDatabaseID,
		&relationship.TargetTableName,
		&relationship.MappingID,
		&relationship.PolicyIDs,
		&relationship.OwnerID,
		&relationship.StatusMessage,
		&relationship.Status,
		&relationship.Created,
		&relationship.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("relationship not found")
		}
		s.logger.Errorf("Failed to update relationship: %v", err)
		return nil, err
	}

	return &relationship, nil
}

// DeleteByName deletes a relationship by name
func (s *Service) DeleteByName(ctx context.Context, tenantID, workspaceID, name string) error {
	s.logger.Infof("Deleting relationship with name: %s", name)

	// Check if relationship exists and get its ID
	relationship, err := s.GetByName(ctx, tenantID, workspaceID, name)
	if err != nil {
		return err
	}

	// Use the existing Delete method with the relationship ID
	return s.Delete(ctx, tenantID, workspaceID, relationship.ID)
}
