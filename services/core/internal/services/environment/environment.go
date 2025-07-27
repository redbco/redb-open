package environment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles environment-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new environment service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Environment represents an environment in the system
type Environment struct {
	ID            string
	TenantID      string
	WorkspaceName string
	Name          string
	Description   string
	Production    bool
	Criticality   int32
	Priority      int32
	OwnerID       string
	Status        string
	Created       time.Time
	Updated       time.Time
}

// Create creates a new environment
func (s *Service) Create(ctx context.Context, tenantID, workspaceName, name, description string, production bool, criticality, priority int32, ownerID string) (*Environment, error) {
	s.logger.Infof("Creating environment in database for tenant: %s, workspace: %s, name: %s", tenantID, workspaceName, name)

	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if the workspace exists and belongs to the tenant and get the workspace ID
	var workspaceID string
	err = s.db.Pool().QueryRow(ctx, "SELECT workspace_id FROM workspaces WHERE workspace_name = $1 AND tenant_id = $2", workspaceName, tenantID).Scan(&workspaceID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logger.Errorf("Workspace not found: workspace_name=%s, tenant_id=%s", workspaceName, tenantID)
			return nil, fmt.Errorf("workspace '%s' not found in tenant '%s'", workspaceName, tenantID)
		}
		return nil, fmt.Errorf("failed to check workspace existence: %w", err)
	}
	if workspaceID == "" {
		s.logger.Errorf("Workspace ID is empty: workspace_name=%s, tenant_id=%s", workspaceName, tenantID)
		return nil, errors.New("workspace not found in tenant")
	}

	s.logger.Infof("Found workspace: workspace_id=%s, workspace_name=%s, tenant_id=%s", workspaceID, workspaceName, tenantID)

	// Insert the environment into the database
	query := `
		INSERT INTO environments (tenant_id, workspace_id, environment_name, environment_description, environment_is_production, environment_criticality, environment_priority, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING environment_id, tenant_id, workspace_id, environment_name, environment_description, environment_is_production, environment_criticality, environment_priority, owner_id, status, created, updated
	`

	var environment Environment
	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name, description, production, criticality, priority, ownerID).Scan(
		&environment.ID,
		&environment.TenantID,
		&environment.WorkspaceName,
		&environment.Name,
		&environment.Description,
		&environment.Production,
		&environment.Criticality,
		&environment.Priority,
		&environment.OwnerID,
		&environment.Status,
		&environment.Created,
		&environment.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create environment: %v", err)
		return nil, err
	}

	return &environment, nil
}

// Get retrieves an environment by ID
func (s *Service) Get(ctx context.Context, tenantID, workspaceName, environmentName string) (*Environment, error) {
	s.logger.Infof("Retrieving environment from database with tenant: %s, workspace: %s, name: %s", tenantID, workspaceName, environmentName)
	query := `
		SELECT environment_id, tenant_id, workspace_id, environment_name, environment_description, environment_is_production, environment_criticality, environment_priority, owner_id, status, created, updated
		FROM environments
		WHERE tenant_id = $1 AND workspace_id = (SELECT workspace_id FROM workspaces WHERE workspace_name = $2) AND environment_name = $3
	`

	var environment Environment
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceName, environmentName).Scan(
		&environment.ID,
		&environment.TenantID,
		&environment.WorkspaceName,
		&environment.Name,
		&environment.Description,
		&environment.Production,
		&environment.Criticality,
		&environment.Priority,
		&environment.OwnerID,
		&environment.Status,
		&environment.Created,
		&environment.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("environment not found")
		}
		s.logger.Errorf("Failed to get environment: %v", err)
		return nil, err
	}

	return &environment, nil
}

// List retrieves all environments for a tenant and workspace
func (s *Service) List(ctx context.Context, tenantID, workspaceName string) ([]*Environment, error) {
	s.logger.Infof("Listing environments from database for tenant: %s, workspace: %s", tenantID, workspaceName)
	query := `
		SELECT environment_id, tenant_id, workspace_id, environment_name, environment_description, environment_is_production, environment_criticality, environment_priority, owner_id, status, created, updated
		FROM environments
		WHERE tenant_id = $1 AND workspace_id = (SELECT workspace_id FROM workspaces WHERE workspace_name = $2)
		ORDER BY environment_id
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceName)
	if err != nil {
		s.logger.Errorf("Failed to list environments: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var environments []*Environment
	for rows.Next() {
		var environment Environment
		err := rows.Scan(
			&environment.ID,
			&environment.TenantID,
			&environment.WorkspaceName,
			&environment.Name,
			&environment.Description,
			&environment.Production,
			&environment.Criticality,
			&environment.Priority,
			&environment.OwnerID,
			&environment.Status,
			&environment.Created,
			&environment.Updated,
		)
		if err != nil {
			return nil, err
		}
		environments = append(environments, &environment)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return environments, nil
}

// Update updates specific fields of an environment
func (s *Service) Update(ctx context.Context, tenantID, workspaceName, environmentName string, updates map[string]interface{}) (*Environment, error) {
	s.logger.Infof("Updating environment in database with ID: %s, updates: %v", environmentName, updates)

	// If no updates, just return the current environment
	if len(updates) == 0 {
		return s.Get(ctx, tenantID, workspaceName, environmentName)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE environments SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause with the environment ID
	query += fmt.Sprintf(" WHERE tenant_id = $%d AND workspace_id = (SELECT workspace_id FROM workspaces WHERE workspace_name = $%d) AND environment_name = $%d RETURNING environment_id, tenant_id, workspace_id, environment_name, environment_description, environment_is_production, environment_criticality, environment_priority, owner_id, status, created, updated", argIndex, argIndex+1, argIndex+2)
	args = append(args, tenantID, workspaceName, environmentName)

	// Execute the update query
	var environment Environment
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&environment.ID,
		&environment.TenantID,
		&environment.WorkspaceName,
		&environment.Name,
		&environment.Description,
		&environment.Production,
		&environment.Criticality,
		&environment.Priority,
		&environment.OwnerID,
		&environment.Status,
		&environment.Created,
		&environment.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("environment not found")
		}
		s.logger.Errorf("Failed to update environment: %v", err)
		return nil, err
	}

	return &environment, nil
}

// Delete deletes an environment
func (s *Service) Delete(ctx context.Context, tenantID, workspaceName, environmentName string) error {
	s.logger.Infof("Deleting environment from database with ID: %s", environmentName)
	query := `
		DELETE FROM environments
		WHERE tenant_id = $1 AND workspace_id = (SELECT workspace_id FROM workspaces WHERE workspace_name = $2) AND environment_name = $3
	`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, workspaceName, environmentName)
	if err != nil {
		s.logger.Errorf("Failed to delete environment: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("environment not found")
	}

	return nil
}

// Exists checks if an environment with the given name already exists for a tenant and workspace
func (s *Service) Exists(ctx context.Context, tenantID, workspaceID, name string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM environments WHERE tenant_id = $1 AND workspace_id = $2 AND environment_name = $3)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// GetInstanceCount returns the number of instances in the environment
func (s *Service) GetInstanceCount(ctx context.Context, tenantID, workspaceID, environmentID string) (int32, error) {
	query := "SELECT COUNT(*) FROM instances WHERE tenant_id = $1 AND workspace_id = $2 AND environment_id = $3"
	var count int32
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, environmentID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// GetDatabaseCount returns the number of databases in the environment
func (s *Service) GetDatabaseCount(ctx context.Context, tenantID, workspaceID, environmentID string) (int32, error) {
	query := "SELECT COUNT(*) FROM databases WHERE tenant_id = $1 AND workspace_id = $2 AND environment_id = $3"
	var count int32
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, environmentID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
