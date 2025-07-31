package mesh

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles mesh-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new mesh service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Mesh represents a mesh in the system
type Mesh struct {
	ID          string
	Name        string
	Description string
	PublicKey   string
	AllowJoin   bool
	NodeCount   int32
	Status      string
	Created     time.Time
	Updated     time.Time
}

// Node represents a node in the mesh
type Node struct {
	ID          string
	Name        string
	Description string
	Platform    string
	Version     string
	RegionID    string
	RegionName  string
	PublicKey   string
	PrivateKey  string
	IPAddress   string
	Port        int32
	Status      string
	Created     time.Time
	Updated     time.Time
}

// Create creates a new mesh
func (s *Service) Create(ctx context.Context, name, description string, allowJoin bool) (*Mesh, error) {
	s.logger.Infof("Creating mesh in database with name: %s", name)

	// Check if mesh with the same name already exists
	var exists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mesh WHERE mesh_name = $1)", name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check mesh existence: %w", err)
	}
	if exists {
		return nil, errors.New("mesh with this name already exists")
	}

	// Insert the mesh into the database
	query := `
		INSERT INTO mesh (mesh_name, mesh_description, allow_join)
		VALUES ($1, $2, $3)
		RETURNING mesh_id, mesh_name, mesh_description, allow_join, status, created, updated
	`

	var mesh Mesh
	err = s.db.Pool().QueryRow(ctx, query, name, description, allowJoin).Scan(
		&mesh.ID,
		&mesh.Name,
		&mesh.Description,
		&mesh.AllowJoin,
		&mesh.Status,
		&mesh.Created,
		&mesh.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create mesh: %v", err)
		return nil, err
	}

	// Get node count
	mesh.NodeCount, _ = s.getNodeCount(ctx, mesh.ID)

	return &mesh, nil
}

// Get retrieves a mesh by ID
func (s *Service) Get(ctx context.Context, id string) (*Mesh, error) {
	s.logger.Infof("Retrieving mesh from database with ID: %s", id)
	query := `
		SELECT mesh_id, mesh_name, mesh_description, allow_join, status, created, updated
		FROM mesh
		WHERE mesh_id = $1
	`

	var mesh Mesh
	err := s.db.Pool().QueryRow(ctx, query, id).Scan(
		&mesh.ID,
		&mesh.Name,
		&mesh.Description,
		&mesh.AllowJoin,
		&mesh.Status,
		&mesh.Created,
		&mesh.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("mesh not found")
		}
		s.logger.Errorf("Failed to get mesh: %v", err)
		return nil, err
	}

	// Get node count
	mesh.NodeCount, _ = s.getNodeCount(ctx, mesh.ID)

	return &mesh, nil
}

// Update updates specific fields of a mesh
func (s *Service) Update(ctx context.Context, id string, updates map[string]interface{}) (*Mesh, error) {
	s.logger.Infof("Updating mesh in database with ID: %s, updates: %v", id, updates)

	// If no updates, just return the current mesh
	if len(updates) == 0 {
		return s.Get(ctx, id)
	}

	// Build the update query dynamically based on provided fields
	query := "UPDATE mesh SET updated = CURRENT_TIMESTAMP"
	args := []interface{}{}
	argIndex := 1

	// Add each field that needs to be updated
	for field, value := range updates {
		query += fmt.Sprintf(", %s = $%d", field, argIndex)
		args = append(args, value)
		argIndex++
	}

	// Add the WHERE clause
	query += fmt.Sprintf(" WHERE mesh_id = $%d RETURNING mesh_id, mesh_name, mesh_description, allow_join, status, created, updated", argIndex)
	args = append(args, id)

	var mesh Mesh
	err := s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&mesh.ID,
		&mesh.Name,
		&mesh.Description,
		&mesh.AllowJoin,
		&mesh.Status,
		&mesh.Created,
		&mesh.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("mesh not found")
		}
		s.logger.Errorf("Failed to update mesh: %v", err)
		return nil, err
	}

	// Get node count
	mesh.NodeCount, _ = s.getNodeCount(ctx, mesh.ID)

	return &mesh, nil
}

// Delete deletes a mesh
func (s *Service) Delete(ctx context.Context, id string) error {
	s.logger.Infof("Deleting mesh from database with ID: %s", id)
	query := `DELETE FROM mesh WHERE mesh_id = $1`

	commandTag, err := s.db.Pool().Exec(ctx, query, id)
	if err != nil {
		s.logger.Errorf("Failed to delete mesh: %v", err)
		return err
	}

	if commandTag.RowsAffected() == 0 {
		return errors.New("mesh not found")
	}

	return nil
}

// GetNodes retrieves all nodes in a mesh
func (s *Service) GetNodes(ctx context.Context, meshID string) ([]*Node, error) {
	s.logger.Infof("Listing nodes from database for mesh: %s", meshID)
	query := `
		SELECT n.node_id, n.node_name, n.node_description, n.node_platform, n.node_version, 
		       COALESCE(n.region_id, ''), COALESCE(r.region_name, ''), n.ip_address, n.port, n.status, n.created, n.updated
		FROM nodes n
		LEFT JOIN regions r ON n.region_id = r.region_id
		ORDER BY n.node_name
	`

	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		s.logger.Errorf("Failed to list nodes: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var nodes []*Node
	for rows.Next() {
		var node Node
		err := rows.Scan(
			&node.ID,
			&node.Name,
			&node.Description,
			&node.Platform,
			&node.Version,
			&node.RegionID,
			&node.RegionName,
			&node.IPAddress,
			&node.Port,
			&node.Status,
			&node.Created,
			&node.Updated,
		)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &node)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return nodes, nil
}

// GetNode retrieves a specific node by ID
func (s *Service) GetNode(ctx context.Context, meshID, nodeID string) (*Node, error) {
	s.logger.Infof("Retrieving node from database with ID: %s in mesh: %s", nodeID, meshID)
	query := `
		SELECT n.node_id, n.node_name, n.node_description, n.node_platform, n.node_version, 
		       COALESCE(n.region_id, ''), COALESCE(r.region_name, ''), n.ip_address, n.port, n.status, n.created, n.updated
		FROM nodes n
		LEFT JOIN regions r ON n.region_id = r.region_id
		WHERE n.node_id = $1
	`

	var node Node
	err := s.db.Pool().QueryRow(ctx, query, nodeID).Scan(
		&node.ID,
		&node.Name,
		&node.Description,
		&node.Platform,
		&node.Version,
		&node.RegionID,
		&node.RegionName,
		&node.IPAddress,
		&node.Port,
		&node.Status,
		&node.Created,
		&node.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("node not found")
		}
		s.logger.Errorf("Failed to get node: %v", err)
		return nil, err
	}

	return &node, nil
}

// CreateNode creates a new node in the mesh
func (s *Service) CreateNode(ctx context.Context, meshID, nodeName, nodeDescription, platform, version, ipAddress string, port int32, regionID *string) (*Node, error) {
	s.logger.Infof("Creating node in database with name: %s for mesh: %s", nodeName, meshID)

	// Check if node with the same name already exists
	var exists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_name = $1)", nodeName).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check node existence: %w", err)
	}
	if exists {
		return nil, errors.New("node with this name already exists")
	}

	// Insert the node into the database
	query := `
		INSERT INTO nodes (node_name, node_description, node_platform, node_version, region_id, ip_address, port)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING node_id, node_name, node_description, node_platform, node_version, region_id, ip_address, port, status, created, updated
	`

	var node Node
	err = s.db.Pool().QueryRow(ctx, query, nodeName, nodeDescription, platform, version, regionID, ipAddress, port).Scan(
		&node.ID,
		&node.Name,
		&node.Description,
		&node.Platform,
		&node.Version,
		&node.RegionID,
		&node.IPAddress,
		&node.Port,
		&node.Status,
		&node.Created,
		&node.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create node: %v", err)
		return nil, err
	}

	// Get region name if region_id is provided
	if regionID != nil && *regionID != "" {
		var regionName string
		err := s.db.Pool().QueryRow(ctx, "SELECT region_name FROM regions WHERE region_id = $1", *regionID).Scan(&regionName)
		if err == nil {
			node.RegionName = regionName
		}
	}

	return &node, nil
}

// Helper function to get node count for a mesh
func (s *Service) getNodeCount(ctx context.Context, meshID string) (int32, error) {
	query := "SELECT COUNT(*) FROM nodes WHERE mesh_id = $1"
	var count int32
	err := s.db.Pool().QueryRow(ctx, query, meshID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get node count: %w", err)
	}
	return count, nil
}
