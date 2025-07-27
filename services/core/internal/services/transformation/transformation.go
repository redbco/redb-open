package transformation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles transformation-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new transformation service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Transformation represents a transformation in the system
type Transformation struct {
	ID          string
	TenantID    string
	Name        string
	Description string
	Type        string
	Version     string
	Function    string
	Enabled     bool
	OwnerID     string
	Created     time.Time
	Updated     time.Time
}

// Create creates a new transformation
func (s *Service) Create(ctx context.Context, tenantID, name, description, transformationType, version, function, ownerID string) (*Transformation, error) {
	s.logger.Infof("Creating transformation in database for tenant: %s, name: %s", tenantID, name)

	// Check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if transformation with the same name already exists in this tenant
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM transformations WHERE tenant_id = $1 AND transformation_name = $2)", tenantID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check transformation existence: %w", err)
	}
	if exists {
		return nil, errors.New("transformation with this name already exists in the tenant")
	}

	// Insert the transformation into the database
	query := `
		INSERT INTO transformations (tenant_id, transformation_name, transformation_description, 
		                           transformation_type, transformation_version, transformation_function, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING transformation_id, tenant_id, transformation_name, transformation_description, 
		          transformation_type, transformation_version, transformation_function, 
		          transformation_enabled, owner_id, created, updated
	`

	var transformation Transformation
	err = s.db.Pool().QueryRow(ctx, query, tenantID, name, description, transformationType, version, function, ownerID).Scan(
		&transformation.ID,
		&transformation.TenantID,
		&transformation.Name,
		&transformation.Description,
		&transformation.Type,
		&transformation.Version,
		&transformation.Function,
		&transformation.Enabled,
		&transformation.OwnerID,
		&transformation.Created,
		&transformation.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create transformation: %v", err)
		return nil, err
	}

	return &transformation, nil
}

// Get retrieves a transformation by ID
func (s *Service) Get(ctx context.Context, tenantID, id string) (*Transformation, error) {
	s.logger.Infof("Retrieving transformation from database with ID: %s", id)
	query := `
		SELECT transformation_id, tenant_id, transformation_name, transformation_description, 
		       transformation_type, transformation_version, transformation_function, 
		       transformation_enabled, owner_id, created, updated
		FROM transformations
		WHERE tenant_id = $1 AND transformation_id = $2
	`

	var transformation Transformation
	err := s.db.Pool().QueryRow(ctx, query, tenantID, id).Scan(
		&transformation.ID,
		&transformation.TenantID,
		&transformation.Name,
		&transformation.Description,
		&transformation.Type,
		&transformation.Version,
		&transformation.Function,
		&transformation.Enabled,
		&transformation.OwnerID,
		&transformation.Created,
		&transformation.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("transformation not found")
		}
		s.logger.Errorf("Failed to get transformation: %v", err)
		return nil, err
	}

	return &transformation, nil
}

// List retrieves all transformations for a tenant
func (s *Service) List(ctx context.Context, tenantID string) ([]*Transformation, error) {
	s.logger.Infof("Listing transformations for tenant: %s", tenantID)
	query := `
		SELECT transformation_id, tenant_id, transformation_name, transformation_description, 
		       transformation_type, transformation_version, transformation_function, 
		       transformation_enabled, owner_id, created, updated
		FROM transformations
		WHERE tenant_id = $1
		ORDER BY transformation_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		s.logger.Errorf("Failed to list transformations: %v", err)
		return nil, err
	}
	defer rows.Close()

	var transformations []*Transformation
	for rows.Next() {
		var transformation Transformation
		err := rows.Scan(
			&transformation.ID,
			&transformation.TenantID,
			&transformation.Name,
			&transformation.Description,
			&transformation.Type,
			&transformation.Version,
			&transformation.Function,
			&transformation.Enabled,
			&transformation.OwnerID,
			&transformation.Created,
			&transformation.Updated,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan transformation: %v", err)
			return nil, err
		}
		transformations = append(transformations, &transformation)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning transformations: %v", err)
		return nil, err
	}

	return transformations, nil
}

// Update updates a transformation
func (s *Service) Update(ctx context.Context, tenantID, id string, updates map[string]interface{}) (*Transformation, error) {
	s.logger.Infof("Updating transformation with ID: %s", id)

	if len(updates) == 0 {
		return s.Get(ctx, tenantID, id)
	}

	// Check if transformation exists
	_, err := s.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, id}
	argIndex := 3

	for field, value := range updates {
		switch field {
		case "transformation_name", "transformation_description", "transformation_type",
			"transformation_version", "transformation_function", "transformation_enabled":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		default:
			s.logger.Warnf("Ignoring invalid update field: %s", field)
		}
	}

	if len(setParts) == 0 {
		return s.Get(ctx, tenantID, id)
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE transformations 
		SET %s
		WHERE tenant_id = $1 AND transformation_id = $2
		RETURNING transformation_id, tenant_id, transformation_name, transformation_description, 
		          transformation_type, transformation_version, transformation_function, 
		          transformation_enabled, owner_id, created, updated
	`, setClause)

	var transformation Transformation
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&transformation.ID,
		&transformation.TenantID,
		&transformation.Name,
		&transformation.Description,
		&transformation.Type,
		&transformation.Version,
		&transformation.Function,
		&transformation.Enabled,
		&transformation.OwnerID,
		&transformation.Created,
		&transformation.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("transformation not found")
		}
		s.logger.Errorf("Failed to update transformation: %v", err)
		return nil, err
	}

	return &transformation, nil
}

// Delete deletes a transformation
func (s *Service) Delete(ctx context.Context, tenantID, id string) error {
	s.logger.Infof("Deleting transformation with ID: %s", id)

	// Check if transformation exists
	_, err := s.Get(ctx, tenantID, id)
	if err != nil {
		return err
	}

	// Delete transformation
	result, err := s.db.Pool().Exec(ctx, "DELETE FROM transformations WHERE tenant_id = $1 AND transformation_id = $2",
		tenantID, id)
	if err != nil {
		return fmt.Errorf("failed to delete transformation: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("transformation not found")
	}

	return nil
}
