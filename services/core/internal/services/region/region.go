package region

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles region-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new region service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Region represents a region in the system
type Region struct {
	ID            string
	Name          string
	Description   string
	Location      string
	Latitude      *float64
	Longitude     *float64
	RegionType    string
	NodeCount     int32
	InstanceCount int32
	DatabaseCount int32
	Status        string
	GlobalRegion  bool
	Created       time.Time
	Updated       time.Time
}

// Create creates a new tenant-specific region
func (s *Service) Create(ctx context.Context, tenantID, name, regionType, description, location string, latitude, longitude *float64) (*Region, error) {
	s.logger.Infof("Creating region in database for tenant: %s, name: %s", tenantID, name)

	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if region with the same name already exists
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM regions WHERE region_name = $1)", name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check region existence: %w", err)
	}
	if exists {
		return nil, errors.New("region with this name already exists")
	}

	// Insert the region into the database
	query := `
		INSERT INTO regions (region_name, region_description, region_location, region_latitude, region_longitude, region_type, global_region)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING region_id, region_name, region_description, region_location, region_latitude, region_longitude, region_type, global_region, status, created, updated
	`

	var region Region
	err = s.db.Pool().QueryRow(ctx, query, name, description, location, latitude, longitude, regionType, false).Scan(
		&region.ID,
		&region.Name,
		&region.Description,
		&region.Location,
		&region.Latitude,
		&region.Longitude,
		&region.RegionType,
		&region.GlobalRegion,
		&region.Status,
		&region.Created,
		&region.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create region: %v", err)
		return nil, err
	}

	// Get counts
	region.NodeCount, _ = s.getNodeCount(ctx, region.ID)
	region.InstanceCount, _ = s.getInstanceCount(ctx, region.ID)
	region.DatabaseCount, _ = s.getDatabaseCount(ctx, region.ID)

	return &region, nil
}

// CreateGlobal creates a new global region
func (s *Service) CreateGlobal(ctx context.Context, name, regionType, description, location string, latitude, longitude *float64) (*Region, error) {
	s.logger.Infof("Creating global region in database with name: %s", name)

	// Check if region with the same name already exists
	var exists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM regions WHERE region_name = $1)", name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check region existence: %w", err)
	}
	if exists {
		return nil, errors.New("region with this name already exists")
	}

	// Insert the global region into the database
	query := `
		INSERT INTO regions (region_name, region_description, region_location, region_latitude, region_longitude, region_type, global_region)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING region_id, region_name, region_description, region_location, region_latitude, region_longitude, region_type, global_region, status, created, updated
	`

	var region Region
	err = s.db.Pool().QueryRow(ctx, query, name, description, location, latitude, longitude, regionType, true).Scan(
		&region.ID,
		&region.Name,
		&region.Description,
		&region.Location,
		&region.Latitude,
		&region.Longitude,
		&region.RegionType,
		&region.GlobalRegion,
		&region.Status,
		&region.Created,
		&region.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create global region: %v", err)
		return nil, err
	}

	// Get counts
	region.NodeCount, _ = s.getNodeCount(ctx, region.ID)
	region.InstanceCount, _ = s.getInstanceCount(ctx, region.ID)
	region.DatabaseCount, _ = s.getDatabaseCount(ctx, region.ID)

	return &region, nil
}

// Get retrieves a region by ID
func (s *Service) Get(ctx context.Context, name string) (*Region, error) {
	s.logger.Infof("Retrieving region from database with ID: %s", name)
	query := `
		SELECT region_id, region_name, region_description, region_location, region_latitude, region_longitude, region_type, global_region, status, created, updated
		FROM regions
		WHERE region_name = $1
	`

	var region Region
	err := s.db.Pool().QueryRow(ctx, query, name).Scan(
		&region.ID,
		&region.Name,
		&region.Description,
		&region.Location,
		&region.Latitude,
		&region.Longitude,
		&region.RegionType,
		&region.GlobalRegion,
		&region.Status,
		&region.Created,
		&region.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("region not found")
		}
		s.logger.Errorf("Failed to get region: %v", err)
		return nil, err
	}

	// Get counts
	region.NodeCount, _ = s.getNodeCount(ctx, region.ID)
	region.InstanceCount, _ = s.getInstanceCount(ctx, region.ID)
	region.DatabaseCount, _ = s.getDatabaseCount(ctx, region.ID)

	return &region, nil
}

// List retrieves all regions (global regions and tenant-specific regions)
func (s *Service) List(ctx context.Context, tenantID string) ([]*Region, error) {
	s.logger.Infof("Listing regions from database for tenant: %s", tenantID)
	query := `
		SELECT region_id, region_name, region_description, region_location, region_latitude, region_longitude, region_type, global_region, status, created, updated
		FROM regions
		ORDER BY global_region DESC, region_name
	`

	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		s.logger.Errorf("Failed to list regions: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var regions []*Region
	for rows.Next() {
		var region Region
		err := rows.Scan(
			&region.ID,
			&region.Name,
			&region.Description,
			&region.Location,
			&region.Latitude,
			&region.Longitude,
			&region.RegionType,
			&region.GlobalRegion,
			&region.Status,
			&region.Created,
			&region.Updated,
		)
		if err != nil {
			return nil, err
		}

		// Get counts for each region
		region.NodeCount, _ = s.getNodeCount(ctx, region.ID)
		region.InstanceCount, _ = s.getInstanceCount(ctx, region.ID)
		region.DatabaseCount, _ = s.getDatabaseCount(ctx, region.ID)

		regions = append(regions, &region)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return regions, nil
}

// Update updates specific fields of a region
func (s *Service) Update(ctx context.Context, name string, updates map[string]interface{}) (*Region, error) {
	s.logger.Infof("Updating region in database with ID: %s, updates: %v", name, updates)

	// If no updates, just return the current region
	if len(updates) == 0 {
		return s.Get(ctx, name)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE regions SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause with the region ID
	query += fmt.Sprintf(" WHERE region_name = $%d RETURNING region_id, region_name, region_description, region_location, region_latitude, region_longitude, region_type, global_region, status, created, updated", argIndex)
	args = append(args, name)

	// Execute the update query
	var region Region
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&region.ID,
		&region.Name,
		&region.Description,
		&region.Location,
		&region.Latitude,
		&region.Longitude,
		&region.RegionType,
		&region.GlobalRegion,
		&region.Status,
		&region.Created,
		&region.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("region not found")
		}
		s.logger.Errorf("Failed to update region: %v", err)
		return nil, err
	}

	// Get counts
	region.NodeCount, _ = s.getNodeCount(ctx, region.ID)
	region.InstanceCount, _ = s.getInstanceCount(ctx, region.ID)
	region.DatabaseCount, _ = s.getDatabaseCount(ctx, region.ID)

	return &region, nil
}

// Delete deletes a region
func (s *Service) Delete(ctx context.Context, name string) error {
	s.logger.Infof("Deleting region from database with ID: %s", name)
	query := `
		DELETE FROM regions
		WHERE region_name = $1
	`

	commandTag, err := s.db.Pool().Exec(ctx, query, name)
	if err != nil {
		s.logger.Errorf("Failed to delete region: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		s.logger.Errorf("Region not found: %v", name)
		return errors.New("region not found")
	}

	s.logger.Infof("Region deleted successfully: %s", name)
	return nil
}

// Exists checks if a region with the given name already exists
func (s *Service) Exists(ctx context.Context, name string) (bool, error) {
	query := `
		SELECT EXISTS(SELECT 1 FROM regions WHERE region_name = $1)
	`

	var exists bool
	err := s.db.Pool().QueryRow(ctx, query, name).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// Helper functions to get counts
func (s *Service) getNodeCount(ctx context.Context, regionID string) (int32, error) {
	query := "SELECT COUNT(*) FROM nodes WHERE region_id = $1"
	var count int32
	err := s.db.Pool().QueryRow(ctx, query, regionID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Service) getInstanceCount(ctx context.Context, regionID string) (int32, error) {
	query := "SELECT COUNT(*) FROM instances i JOIN nodes n ON i.connected_to_node_id = n.node_id WHERE n.region_id = $1"
	var count int32
	err := s.db.Pool().QueryRow(ctx, query, regionID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Service) getDatabaseCount(ctx context.Context, regionID string) (int32, error) {
	query := "SELECT COUNT(*) FROM databases d JOIN nodes n ON d.connected_to_node_id = n.node_id WHERE n.region_id = $1"
	var count int32
	err := s.db.Pool().QueryRow(ctx, query, regionID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
