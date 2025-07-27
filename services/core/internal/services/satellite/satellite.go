package satellite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles satellite-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new satellite service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Satellite represents a satellite in the system
type Satellite struct {
	ID              string
	TenantID        string
	Name            string
	Description     string
	Platform        string
	Version         string
	RegionID        string
	IPAddress       string
	ConnectedNodeID string
	OwnerID         string
	Status          string
	Created         time.Time
	Updated         time.Time
}

// Create creates a new satellite
func (s *Service) Create(ctx context.Context, tenantID, name, description, platform, version, regionID, ipAddress, nodeID, publicKey, privateKey, ownerID string) (*Satellite, error) {
	s.logger.Infof("Creating satellite in database for tenant: %s, name: %s", tenantID, name)

	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if the node exists
	var nodeExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)", nodeID).Scan(&nodeExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check node existence: %w", err)
	}
	if !nodeExists {
		return nil, errors.New("node not found")
	}

	// Check if satellite with the same name already exists for this tenant
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM satellites WHERE tenant_id = $1 AND satellite_name = $2)", tenantID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check satellite existence: %w", err)
	}
	if exists {
		return nil, errors.New("satellite with this name already exists for this tenant")
	}

	// Insert the satellite into the database
	query := `
		INSERT INTO satellites (tenant_id, satellite_name, satellite_description, satellite_platform, satellite_version, satellite_region_id, satellite_ip_address, connected_to_node_id, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING satellite_id, tenant_id, satellite_name, satellite_description, satellite_platform, satellite_version, satellite_region_id, satellite_ip_address, connected_to_node_id, owner_id, status, created, updated
	`

	var satellite Satellite
	err = s.db.Pool().QueryRow(ctx, query, tenantID, name, description, platform, version, regionID, ipAddress, nodeID, ownerID).Scan(
		&satellite.ID,
		&satellite.TenantID,
		&satellite.Name,
		&satellite.Description,
		&satellite.Platform,
		&satellite.Version,
		&satellite.RegionID,
		&satellite.IPAddress,
		&satellite.ConnectedNodeID,
		&satellite.OwnerID,
		&satellite.Status,
		&satellite.Created,
		&satellite.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create satellite: %v", err)
		return nil, err
	}

	return &satellite, nil
}

// Get retrieves a satellite by ID
func (s *Service) Get(ctx context.Context, id string) (*Satellite, error) {
	s.logger.Infof("Retrieving satellite from database with ID: %s", id)
	query := `
		SELECT satellite_id, tenant_id, satellite_name, satellite_description, satellite_platform, satellite_version, satellite_region_id, satellite_ip_address, connected_to_node_id, owner_id, status, created, updated
		FROM satellites
		WHERE satellite_id = $1
	`

	var satellite Satellite
	err := s.db.Pool().QueryRow(ctx, query, id).Scan(
		&satellite.ID,
		&satellite.TenantID,
		&satellite.Name,
		&satellite.Description,
		&satellite.Platform,
		&satellite.Version,
		&satellite.RegionID,
		&satellite.IPAddress,
		&satellite.ConnectedNodeID,
		&satellite.OwnerID,
		&satellite.Status,
		&satellite.Created,
		&satellite.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("satellite not found")
		}
		s.logger.Errorf("Failed to get satellite: %v", err)
		return nil, err
	}

	return &satellite, nil
}

// List retrieves all satellites for a tenant
func (s *Service) List(ctx context.Context, tenantID string) ([]*Satellite, error) {
	s.logger.Infof("Listing satellites from database for tenant: %s", tenantID)
	query := `
		SELECT satellite_id, tenant_id, satellite_name, satellite_description, satellite_platform, satellite_version, satellite_region_id, satellite_ip_address, connected_to_node_id, owner_id, status, created, updated
		FROM satellites
		WHERE tenant_id = $1
		ORDER BY satellite_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		s.logger.Errorf("Failed to list satellites: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var satellites []*Satellite
	for rows.Next() {
		var satellite Satellite
		err := rows.Scan(
			&satellite.ID,
			&satellite.TenantID,
			&satellite.Name,
			&satellite.Description,
			&satellite.Platform,
			&satellite.Version,
			&satellite.RegionID,
			&satellite.IPAddress,
			&satellite.ConnectedNodeID,
			&satellite.OwnerID,
			&satellite.Status,
			&satellite.Created,
			&satellite.Updated,
		)
		if err != nil {
			return nil, err
		}
		satellites = append(satellites, &satellite)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return satellites, nil
}

// Update updates specific fields of a satellite
func (s *Service) Update(ctx context.Context, id string, updates map[string]interface{}) (*Satellite, error) {
	s.logger.Infof("Updating satellite in database with ID: %s, updates: %v", id, updates)

	// If no updates, just return the current satellite
	if len(updates) == 0 {
		return s.Get(ctx, id)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE satellites SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause
	query += fmt.Sprintf(" WHERE satellite_id = $%d RETURNING satellite_id, tenant_id, satellite_name, satellite_description, satellite_platform, satellite_version, satellite_region_id, satellite_ip_address, connected_to_node_id, owner_id, status, created, updated", argIndex)
	args = append(args, id)

	var satellite Satellite
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&satellite.ID,
		&satellite.TenantID,
		&satellite.Name,
		&satellite.Description,
		&satellite.Platform,
		&satellite.Version,
		&satellite.RegionID,
		&satellite.IPAddress,
		&satellite.ConnectedNodeID,
		&satellite.OwnerID,
		&satellite.Status,
		&satellite.Created,
		&satellite.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("satellite not found")
		}
		s.logger.Errorf("Failed to update satellite: %v", err)
		return nil, err
	}

	return &satellite, nil
}

// Delete deletes a satellite
func (s *Service) Delete(ctx context.Context, id string) error {
	s.logger.Infof("Deleting satellite from database with ID: %s", id)
	query := `DELETE FROM satellites WHERE satellite_id = $1`

	commandTag, err := s.db.Pool().Exec(ctx, query, id)
	if err != nil {
		s.logger.Errorf("Failed to delete satellite: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("satellite not found")
	}

	return nil
}
