package tenant

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles tenant-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new tenant service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Tenant represents a tenant in the system
type Tenant struct {
	ID          string
	Name        string
	Description string
	URL         string
	Status      string
	Created     time.Time
	Updated     time.Time
}

// Create creates a new tenant
func (s *Service) Create(ctx context.Context, name, description, url string) (*Tenant, error) {
	s.logger.Infof("Creating tenant in database with name: %s", name)

	// Check if tenant with this name already exists
	var nameExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_name = $1)", name).Scan(&nameExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant name existence: %w", err)
	}
	if nameExists {
		return nil, errors.New("tenant with this name already exists")
	}

	// Check if tenant with this URL already exists
	var urlExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_url = $1)", url).Scan(&urlExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant URL existence: %w", err)
	}
	if urlExists {
		return nil, errors.New("tenant with this URL already exists")
	}

	// Insert the tenant into the database
	query := `
		INSERT INTO tenants (tenant_name, tenant_description, tenant_url, status)
		VALUES ($1, $2, $3, $4)
		RETURNING tenant_id, tenant_name, tenant_description, tenant_url, status, created, updated
	`

	var tenant Tenant
	err = s.db.Pool().QueryRow(ctx, query, name, description, url, "STATUS_HEALTHY").Scan(
		&tenant.ID,
		&tenant.Name,
		&tenant.Description,
		&tenant.URL,
		&tenant.Status,
		&tenant.Created,
		&tenant.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create tenant: %v", err)
		return nil, err
	}

	return &tenant, nil
}

// Get retrieves a tenant by ID
func (s *Service) Get(ctx context.Context, tenantID string) (*Tenant, error) {
	s.logger.Infof("Retrieving tenant from database with ID: %s", tenantID)
	query := `
		SELECT tenant_id, tenant_name, tenant_description, tenant_url, status, created, updated
		FROM tenants
		WHERE tenant_id = $1
	`

	var tenant Tenant
	err := s.db.Pool().QueryRow(ctx, query, tenantID).Scan(
		&tenant.ID,
		&tenant.Name,
		&tenant.Description,
		&tenant.URL,
		&tenant.Status,
		&tenant.Created,
		&tenant.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("tenant not found")
		}
		s.logger.Errorf("Failed to get tenant: %v", err)
		return nil, err
	}

	return &tenant, nil
}

// GetByName retrieves a tenant by name
func (s *Service) GetByName(ctx context.Context, name string) (*Tenant, error) {
	s.logger.Infof("Retrieving tenant from database with name: %s", name)
	query := `
		SELECT tenant_id, tenant_name, tenant_description, tenant_url, status, created, updated
		FROM tenants
		WHERE tenant_name = $1
	`

	var tenant Tenant
	err := s.db.Pool().QueryRow(ctx, query, name).Scan(
		&tenant.ID,
		&tenant.Name,
		&tenant.Description,
		&tenant.URL,
		&tenant.Status,
		&tenant.Created,
		&tenant.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("tenant not found")
		}
		s.logger.Errorf("Failed to get tenant by name: %v", err)
		return nil, err
	}

	return &tenant, nil
}

// List retrieves all tenants
func (s *Service) List(ctx context.Context) ([]*Tenant, error) {
	s.logger.Info("Listing tenants from database")
	query := `
		SELECT tenant_id, tenant_name, tenant_description, tenant_url, status, created, updated
		FROM tenants
		ORDER BY tenant_id
	`

	// Add detailed logging
	//s.logger.Infof("Executing query: %s", query)

	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		s.logger.Errorf("Failed to list tenants: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var tenants []*Tenant
	for rows.Next() {
		var tenant Tenant
		err := rows.Scan(
			&tenant.ID,
			&tenant.Name,
			&tenant.Description,
			&tenant.URL,
			&tenant.Status,
			&tenant.Created,
			&tenant.Updated,
		)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, &tenant)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tenants, nil
}

// Update updates specific fields of a tenant
func (s *Service) Update(ctx context.Context, tenantID string, updates map[string]interface{}) (*Tenant, error) {
	s.logger.Infof("Updating tenant in database with ID: %s, updates: %v", tenantID, updates)
	// If no updates, just return the current tenant
	if len(updates) == 0 {
		return s.Get(ctx, tenantID)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE tenants SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause with the tenant ID
	query += fmt.Sprintf(" WHERE tenant_id = $%d RETURNING tenant_id, tenant_name, tenant_description, tenant_url, status, created, updated", argIndex)
	args = append(args, tenantID)

	// Execute the update query
	var tenant Tenant
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&tenant.ID,
		&tenant.Name,
		&tenant.Description,
		&tenant.URL,
		&tenant.Status,
		&tenant.Created,
		&tenant.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("tenant not found")
		}
		s.logger.Errorf("Failed to update tenant: %v", err)
		return nil, err
	}

	return &tenant, nil
}

// Delete deletes a tenant
func (s *Service) Delete(ctx context.Context, tenantID string) error {
	s.logger.Infof("Deleting tenant from database with ID: %s", tenantID)
	query := `
		DELETE FROM tenants
		WHERE tenant_id = $1
	`

	commandTag, err := s.db.Pool().Exec(ctx, query, tenantID)
	if err != nil {
		s.logger.Errorf("Failed to delete tenant: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("tenant not found")
	}

	return nil
}

// Exists checks if a tenant with the given ID already exists
func (s *Service) Exists(ctx context.Context, tenantID string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, tenantID).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// NameExists checks if a tenant with the given name already exists
func (s *Service) NameExists(ctx context.Context, name string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_name = $1)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, name).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// URLExists checks if a tenant with the given URL already exists
func (s *Service) URLExists(ctx context.Context, url string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_url = $1)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, url).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}
