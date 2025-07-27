package workspace

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles workspace-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new workspace service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Workspace represents a workspace in the system
type Workspace struct {
	ID          string
	TenantID    string
	Name        string
	Description string
	OwnerID     string
	Created     time.Time
	Updated     time.Time
}

// GetWorkspaceID returns the workspace ID for a given tenant and name
func (s *Service) GetWorkspaceID(ctx context.Context, tenantID, name string) (string, error) {
	query := `
		SELECT workspace_id
		FROM workspaces
		WHERE tenant_id = $1 AND workspace_name = $2
	`
	var workspaceID string
	err := s.db.Pool().QueryRow(ctx, query, tenantID, name).Scan(&workspaceID)
	if err != nil {
		return "", err
	}
	return workspaceID, nil
}

// Create creates a new workspace
func (s *Service) Create(ctx context.Context, tenantID, name, description, ownerID string) (*Workspace, error) {
	s.logger.Infof("Creating workspace in database for tenant: %s, name: %s", tenantID, name)
	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Insert the workspace into the database
	query := `
		INSERT INTO workspaces (tenant_id, workspace_name, workspace_description, owner_id)
		VALUES ($1, $2, $3, $4)
		RETURNING workspace_id, tenant_id, workspace_name, workspace_description, owner_id, created, updated
	`

	var workspace Workspace
	err = s.db.Pool().QueryRow(ctx, query, tenantID, name, description, ownerID).Scan(
		&workspace.ID,
		&workspace.TenantID,
		&workspace.Name,
		&workspace.Description,
		&workspace.OwnerID,
		&workspace.Created,
		&workspace.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create workspace: %v", err)
		return nil, err
	}

	return &workspace, nil
}

// Get retrieves a workspace by ID
func (s *Service) Get(ctx context.Context, tenantID, name string) (*Workspace, error) {
	s.logger.Infof("Retrieving workspace from database with ID: %s", name)
	query := `
		SELECT workspace_id, tenant_id, workspace_name, workspace_description, owner_id, created, updated
		FROM workspaces
		WHERE tenant_id = $1 AND workspace_name = $2
	`

	var workspace Workspace
	err := s.db.Pool().QueryRow(ctx, query, tenantID, name).Scan(
		&workspace.ID,
		&workspace.TenantID,
		&workspace.Name,
		&workspace.Description,
		&workspace.OwnerID,
		&workspace.Created,
		&workspace.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("workspace not found")
		}
		s.logger.Errorf("Failed to get workspace: %v", err)
		return nil, err
	}

	return &workspace, nil
}

// List retrieves all workspaces for a tenant
func (s *Service) List(ctx context.Context, tenantID string) ([]*Workspace, error) {
	s.logger.Infof("Listing workspaces from database for tenant: %s", tenantID)
	query := `
		SELECT workspace_id, tenant_id, workspace_name, workspace_description, owner_id, created, updated
		FROM workspaces
		WHERE tenant_id = $1
		ORDER BY workspace_id
	`

	// Add detailed logging
	//s.logger.Infof("Executing query: %s with args: %v", query, tenantID)

	rows, err := s.db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		s.logger.Errorf("Failed to list workspaces: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var workspaces []*Workspace
	for rows.Next() {
		var workspace Workspace
		err := rows.Scan(
			&workspace.ID,
			&workspace.TenantID,
			&workspace.Name,
			&workspace.Description,
			&workspace.OwnerID,
			&workspace.Created,
			&workspace.Updated,
		)
		if err != nil {
			return nil, err
		}
		workspaces = append(workspaces, &workspace)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return workspaces, nil
}

// Update updates specific fields of a workspace
func (s *Service) Update(ctx context.Context, tenantID, name string, updates map[string]interface{}) (*Workspace, error) {
	s.logger.Infof("Updating workspace in database with ID: %s, updates: %v", name, updates)
	// If no updates, just return the current workspace
	if len(updates) == 0 {
		return s.Get(ctx, tenantID, name)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE workspaces SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause with the workspace ID
	query += fmt.Sprintf(" WHERE tenant_id = $%d AND workspace_name = $%d RETURNING workspace_id, tenant_id, workspace_name, workspace_description, owner_id, created, updated", argIndex, argIndex+1)
	args = append(args, tenantID, name)

	// Execute the update query
	var workspace Workspace
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&workspace.ID,
		&workspace.TenantID,
		&workspace.Name,
		&workspace.Description,
		&workspace.OwnerID,
		&workspace.Created,
		&workspace.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("workspace not found")
		}
		s.logger.Errorf("Failed to update workspace: %v", err)
		return nil, err
	}

	return &workspace, nil
}

// Delete deletes a workspace
func (s *Service) Delete(ctx context.Context, tenantID, name string) error {
	s.logger.Infof("Deleting workspace from database with ID: %s", name)
	query := `
		DELETE FROM workspaces
		WHERE tenant_id = $1 AND workspace_name = $2
	`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID, name)
	if err != nil {
		s.logger.Errorf("Failed to delete workspace: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("workspace not found")
	}

	return nil
}

// Exists checks if a workspace with the given name already exists for a tenant
func (s *Service) Exists(ctx context.Context, tenantID, name string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM workspaces WHERE tenant_id = $1 AND workspace_name = $2)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, tenantID, name).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}
