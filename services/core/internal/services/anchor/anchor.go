package anchor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles anchor-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new anchor service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Anchor represents an anchor in the system
type Anchor struct {
	ID              string
	TenantID        string
	Name            string
	Description     string
	Platform        string
	Version         string
	RegionID        string
	IPAddress       string
	ConnectedNodeID *string // Can be null
	OwnerID         string
	Status          string
	Created         time.Time
	Updated         time.Time
}

// Create creates a new anchor
func (s *Service) Create(ctx context.Context, tenantID, name, description, platform, version, regionID, ipAddress, nodeID, publicKey, privateKey, ownerID string) (*Anchor, error) {
	s.logger.Infof("Creating anchor in database for tenant: %s, name: %s", tenantID, name)

	// First, check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if the node exists (if provided)
	if nodeID != "" {
		var nodeExists bool
		err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1)", nodeID).Scan(&nodeExists)
		if err != nil {
			return nil, fmt.Errorf("failed to check node existence: %w", err)
		}
		if !nodeExists {
			return nil, errors.New("node not found")
		}
	}

	// Check if anchor with the same name already exists for this tenant
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM anchors WHERE tenant_id = $1 AND anchor_name = $2)", tenantID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check anchor existence: %w", err)
	}
	if exists {
		return nil, errors.New("anchor with this name already exists for this tenant")
	}

	// Insert the anchor into the database
	query := `
		INSERT INTO anchors (tenant_id, anchor_name, anchor_description, anchor_platform, anchor_version, anchor_region_id, anchor_ip_address, connected_to_node_id, owner_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING anchor_id, tenant_id, anchor_name, anchor_description, anchor_platform, anchor_version, anchor_region_id, anchor_ip_address, connected_to_node_id, owner_id, status, created, updated
	`

	var anchor Anchor
	var connectedNodeID *string
	if nodeID != "" {
		connectedNodeID = &nodeID
	}

	err = s.db.Pool().QueryRow(ctx, query, tenantID, name, description, platform, version, regionID, ipAddress, connectedNodeID, ownerID).Scan(
		&anchor.ID,
		&anchor.TenantID,
		&anchor.Name,
		&anchor.Description,
		&anchor.Platform,
		&anchor.Version,
		&anchor.RegionID,
		&anchor.IPAddress,
		&anchor.ConnectedNodeID,
		&anchor.OwnerID,
		&anchor.Status,
		&anchor.Created,
		&anchor.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create anchor: %v", err)
		return nil, err
	}

	return &anchor, nil
}

// Get retrieves an anchor by ID
func (s *Service) Get(ctx context.Context, id string) (*Anchor, error) {
	s.logger.Infof("Retrieving anchor from database with ID: %s", id)
	query := `
		SELECT anchor_id, tenant_id, anchor_name, anchor_description, anchor_platform, anchor_version, anchor_region_id, anchor_ip_address, connected_to_node_id, owner_id, status, created, updated
		FROM anchors
		WHERE anchor_id = $1
	`

	var anchor Anchor
	err := s.db.Pool().QueryRow(ctx, query, id).Scan(
		&anchor.ID,
		&anchor.TenantID,
		&anchor.Name,
		&anchor.Description,
		&anchor.Platform,
		&anchor.Version,
		&anchor.RegionID,
		&anchor.IPAddress,
		&anchor.ConnectedNodeID,
		&anchor.OwnerID,
		&anchor.Status,
		&anchor.Created,
		&anchor.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("anchor not found")
		}
		s.logger.Errorf("Failed to get anchor: %v", err)
		return nil, err
	}

	return &anchor, nil
}

// List retrieves all anchors for a tenant
func (s *Service) List(ctx context.Context, tenantID string) ([]*Anchor, error) {
	s.logger.Infof("Listing anchors from database for tenant: %s", tenantID)
	query := `
		SELECT anchor_id, tenant_id, anchor_name, anchor_description, anchor_platform, anchor_version, anchor_region_id, anchor_ip_address, connected_to_node_id, owner_id, status, created, updated
		FROM anchors
		WHERE tenant_id = $1
		ORDER BY anchor_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID)
	if err != nil {
		s.logger.Errorf("Failed to list anchors: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var anchors []*Anchor
	for rows.Next() {
		var anchor Anchor
		err := rows.Scan(
			&anchor.ID,
			&anchor.TenantID,
			&anchor.Name,
			&anchor.Description,
			&anchor.Platform,
			&anchor.Version,
			&anchor.RegionID,
			&anchor.IPAddress,
			&anchor.ConnectedNodeID,
			&anchor.OwnerID,
			&anchor.Status,
			&anchor.Created,
			&anchor.Updated,
		)
		if err != nil {
			return nil, err
		}
		anchors = append(anchors, &anchor)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return anchors, nil
}

// Update updates specific fields of an anchor
func (s *Service) Update(ctx context.Context, id string, updates map[string]interface{}) (*Anchor, error) {
	s.logger.Infof("Updating anchor in database with ID: %s, updates: %v", id, updates)

	// If no updates, just return the current anchor
	if len(updates) == 0 {
		return s.Get(ctx, id)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE anchors SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause
	query += fmt.Sprintf(" WHERE anchor_id = $%d RETURNING anchor_id, tenant_id, anchor_name, anchor_description, anchor_platform, anchor_version, anchor_region_id, anchor_ip_address, connected_to_node_id, owner_id, status, created, updated", argIndex)
	args = append(args, id)

	var anchor Anchor
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&anchor.ID,
		&anchor.TenantID,
		&anchor.Name,
		&anchor.Description,
		&anchor.Platform,
		&anchor.Version,
		&anchor.RegionID,
		&anchor.IPAddress,
		&anchor.ConnectedNodeID,
		&anchor.OwnerID,
		&anchor.Status,
		&anchor.Created,
		&anchor.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("anchor not found")
		}
		s.logger.Errorf("Failed to update anchor: %v", err)
		return nil, err
	}

	return &anchor, nil
}

// Delete deletes an anchor
func (s *Service) Delete(ctx context.Context, id string) error {
	s.logger.Infof("Deleting anchor from database with ID: %s", id)
	query := `DELETE FROM anchors WHERE anchor_id = $1`

	commandTag, err := s.db.Pool().Exec(ctx, query, id)
	if err != nil {
		s.logger.Errorf("Failed to delete anchor: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("anchor not found")
	}

	return nil
}
