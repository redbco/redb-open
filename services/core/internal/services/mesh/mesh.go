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
	mesh.NodeCount, _ = s.getNodeCount(ctx)

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
	mesh.NodeCount, _ = s.getNodeCount(ctx)

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
	mesh.NodeCount, _ = s.getNodeCount(ctx)

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
		       n.region_id, COALESCE(r.region_name, ''), HOST(n.ip_address), n.port, n.status, n.created, n.updated
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
		var regionIDScan *string
		err := rows.Scan(
			&node.ID,
			&node.Name,
			&node.Description,
			&node.Platform,
			&node.Version,
			&regionIDScan,
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

		// Handle the region_id field properly
		if regionIDScan != nil {
			node.RegionID = *regionIDScan
		} else {
			node.RegionID = ""
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
		       n.region_id, COALESCE(r.region_name, ''), HOST(n.ip_address), n.port, n.status, n.created, n.updated
		FROM nodes n
		LEFT JOIN regions r ON n.region_id = r.region_id
		WHERE n.node_id = $1
	`

	var node Node
	var regionIDScan *string
	err := s.db.Pool().QueryRow(ctx, query, nodeID).Scan(
		&node.ID,
		&node.Name,
		&node.Description,
		&node.Platform,
		&node.Version,
		&regionIDScan,
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

	// Handle the region_id field properly
	if regionIDScan != nil {
		node.RegionID = *regionIDScan
	} else {
		node.RegionID = ""
	}

	return &node, nil
}

// GetLocalNode retrieves the local node information from the database
func (s *Service) GetLocalNode(ctx context.Context) (*Node, error) {
	s.logger.Infof("Retrieving local node information from database")

	query := `
		SELECT n.node_id, n.node_name, n.node_description, n.node_platform, n.node_version, 
		       n.region_id, HOST(n.ip_address), n.port, n.status, n.created, n.updated
		FROM nodes n
		JOIN localidentity li ON n.node_id = li.identity_id
		LIMIT 1
	`

	var node Node
	var regionIDScan *string
	err := s.db.Pool().QueryRow(ctx, query).Scan(
		&node.ID,
		&node.Name,
		&node.Description,
		&node.Platform,
		&node.Version,
		&regionIDScan,
		&node.IPAddress,
		&node.Port,
		&node.Status,
		&node.Created,
		&node.Updated,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, errors.New("local node not found - node may not be initialized")
		}
		s.logger.Errorf("Failed to get local node: %v", err)
		return nil, fmt.Errorf("failed to get local node: %w", err)
	}

	// Handle the region_id field properly
	if regionIDScan != nil {
		node.RegionID = *regionIDScan
	} else {
		node.RegionID = ""
	}

	// Get region name if region_id is provided
	if regionIDScan != nil && *regionIDScan != "" {
		var regionName string
		err := s.db.Pool().QueryRow(ctx, "SELECT region_name FROM regions WHERE region_id = $1", *regionIDScan).Scan(&regionName)
		if err == nil {
			node.RegionName = regionName
		}
	}

	s.logger.Infof("Retrieved local node: %s (ID: %s)", node.Name, node.ID)
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
		RETURNING node_id, node_name, node_description, node_platform, node_version, region_id, HOST(ip_address), port, status, created, updated
	`

	var node Node
	var regionIDScan *string
	err = s.db.Pool().QueryRow(ctx, query, nodeName, nodeDescription, platform, version, regionID, ipAddress, port).Scan(
		&node.ID,
		&node.Name,
		&node.Description,
		&node.Platform,
		&node.Version,
		&regionIDScan,
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

	// Handle the region_id field properly
	if regionIDScan != nil {
		node.RegionID = *regionIDScan
	} else {
		node.RegionID = ""
	}

	// Get region name if region_id is provided
	if regionIDScan != nil && *regionIDScan != "" {
		var regionName string
		err := s.db.Pool().QueryRow(ctx, "SELECT region_name FROM regions WHERE region_id = $1", *regionIDScan).Scan(&regionName)
		if err == nil {
			node.RegionName = regionName
		}
	}

	return &node, nil
}

// UpdateMeshStatus updates the status of the mesh
func (s *Service) UpdateMeshStatus(ctx context.Context, meshID, status string) error {
	s.logger.Infof("Updating mesh status to: %s", status)

	_, err := s.db.Pool().Exec(ctx, `
		UPDATE mesh 
		SET status = $1, updated = CURRENT_TIMESTAMP
		WHERE mesh_id = $2
	`, status, meshID)
	if err != nil {
		s.logger.Errorf("Failed to update mesh status: %v", err)
		return fmt.Errorf("failed to update mesh status: %w", err)
	}

	s.logger.Infof("Successfully updated mesh status to: %s", status)
	return nil
}

// UpdateNodeStatus updates the status of a specific node
func (s *Service) UpdateNodeStatus(ctx context.Context, nodeID, status string) error {
	s.logger.Infof("Updating node %s status to: %s", nodeID, status)

	_, err := s.db.Pool().Exec(ctx, `
		UPDATE nodes 
		SET status = $1, updated = CURRENT_TIMESTAMP
		WHERE node_id = $2
	`, status, nodeID)
	if err != nil {
		s.logger.Errorf("Failed to update node status: %v", err)
		return fmt.Errorf("failed to update node status: %w", err)
	}

	s.logger.Infof("Successfully updated node %s status to: %s", nodeID, status)
	return nil
}

// UpdateAllNodesStatus updates the status of all nodes
func (s *Service) UpdateAllNodesStatus(ctx context.Context, status string) error {
	s.logger.Infof("Updating all nodes status to: %s", status)

	_, err := s.db.Pool().Exec(ctx, `
		UPDATE nodes 
		SET status = $1, updated = CURRENT_TIMESTAMP
	`, status)
	if err != nil {
		s.logger.Errorf("Failed to update all nodes status: %v", err)
		return fmt.Errorf("failed to update all nodes status: %w", err)
	}

	s.logger.Infof("Successfully updated all nodes status to: %s", status)
	return nil
}

// SetNodeAsJoining sets the local node status to JOINING when mesh is being seeded
func (s *Service) SetNodeAsJoining(ctx context.Context, nodeID string) error {
	return s.UpdateNodeStatus(ctx, nodeID, "STATUS_JOINING")
}

// GetMeshesByNodeID retrieves all meshes that a node belongs to
func (s *Service) GetMeshesByNodeID(ctx context.Context, nodeID string) ([]*Mesh, error) {
	s.logger.Infof("Getting meshes for node ID: %s", nodeID)

	// Check if the node exists and get its status
	var nodeStatus string
	nodeQuery := `SELECT status FROM nodes WHERE node_id = $1`
	err := s.db.Pool().QueryRow(ctx, nodeQuery, nodeID).Scan(&nodeStatus)
	if err != nil {
		s.logger.Errorf("Failed to get node status: %v", err)
		return nil, fmt.Errorf("node not found: %w", err)
	}

	s.logger.Infof("Node %s has status: %s", nodeID, nodeStatus)

	// For now, we'll use a simple approach - if node exists and has been part of mesh operations,
	// return all available meshes. In a full implementation, this would query a node_mesh_membership table
	// We'll consider a node part of a mesh if:
	// 1. The node exists (which we verified above)
	// 2. There are meshes in the system
	// 3. The node status indicates it has been involved in mesh operations
	var query string
	var args []interface{}

	if nodeStatus == "STATUS_CLEAN" {
		// Clean nodes are not part of any mesh
		s.logger.Infof("Node %s is clean, not part of any mesh", nodeID)
		return []*Mesh{}, nil
	} else {
		// Node has been involved in mesh operations, return available meshes
		query = `
			SELECT m.mesh_id, m.mesh_name, m.mesh_description, m.allow_join, m.status, m.created, m.updated
			FROM mesh m
			ORDER BY m.created DESC
		`
		args = []interface{}{}
	}

	rows, err := s.db.Pool().Query(ctx, query, args...)
	if err != nil {
		s.logger.Errorf("Failed to get meshes for node: %v", err)
		return nil, fmt.Errorf("database query error: %w", err)
	}
	defer rows.Close()

	var meshes []*Mesh
	for rows.Next() {
		var mesh Mesh
		err := rows.Scan(
			&mesh.ID,
			&mesh.Name,
			&mesh.Description,
			&mesh.AllowJoin,
			&mesh.Status,
			&mesh.Created,
			&mesh.Updated,
		)
		if err != nil {
			return nil, err
		}

		// Get node count for each mesh
		mesh.NodeCount, _ = s.getNodeCount(ctx)
		meshes = append(meshes, &mesh)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return meshes, nil
}

// AddNodeToMesh adds a node to a mesh
func (s *Service) AddNodeToMesh(ctx context.Context, meshID, nodeID string) error {
	s.logger.Infof("Adding node %s to mesh %s", nodeID, meshID)

	// For now, we'll just update the node status to indicate mesh membership
	// In a full implementation, this would insert into a node_mesh_membership table
	_, err := s.db.Pool().Exec(ctx, `
		UPDATE nodes 
		SET status = 'STATUS_ACTIVE', updated = CURRENT_TIMESTAMP
		WHERE node_id = $1
	`, nodeID)
	if err != nil {
		s.logger.Errorf("Failed to add node to mesh: %v", err)
		return fmt.Errorf("failed to add node to mesh: %w", err)
	}

	s.logger.Infof("Successfully added node %s to mesh %s", nodeID, meshID)
	return nil
}

// RemoveNodeFromMesh removes a node from a mesh
func (s *Service) RemoveNodeFromMesh(ctx context.Context, meshID, nodeID string) error {
	s.logger.Infof("Removing node %s from mesh %s", nodeID, meshID)

	// For now, we'll just update the node status to clean
	// In a full implementation, this would delete from a node_mesh_membership table
	_, err := s.db.Pool().Exec(ctx, `
		UPDATE nodes 
		SET status = 'STATUS_CLEAN', updated = CURRENT_TIMESTAMP
		WHERE node_id = $1
	`, nodeID)
	if err != nil {
		s.logger.Errorf("Failed to remove node from mesh: %v", err)
		return fmt.Errorf("failed to remove node from mesh: %w", err)
	}

	s.logger.Infof("Successfully removed node %s from mesh %s", nodeID, meshID)
	return nil
}

// GetNodeByID retrieves a node by its ID
func (s *Service) GetNodeByID(ctx context.Context, nodeID string) (*Node, error) {
	s.logger.Infof("Retrieving node from database with ID: %s", nodeID)
	query := `
		SELECT n.node_id, n.node_name, n.node_description, n.node_platform, n.node_version, 
		       n.region_id, COALESCE(r.region_name, ''), n.ip_address, n.port, n.status, n.created, n.updated
		FROM nodes n
		LEFT JOIN regions r ON n.region_id = r.region_id
		WHERE n.node_id = $1
	`

	var node Node
	var regionIDScan *string
	err := s.db.Pool().QueryRow(ctx, query, nodeID).Scan(
		&node.ID,
		&node.Name,
		&node.Description,
		&node.Platform,
		&node.Version,
		&regionIDScan,
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

	// Handle the region_id field properly
	if regionIDScan != nil {
		node.RegionID = *regionIDScan
	} else {
		node.RegionID = ""
	}

	return &node, nil
}

// Helper function to get node count for a mesh
func (s *Service) getNodeCount(ctx context.Context) (int32, error) {
	query := "SELECT COUNT(*) FROM nodes WHERE status = 'STATUS_ACTIVE'"
	var count int32
	err := s.db.Pool().QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get node count: %w", err)
	}
	return count, nil
}
