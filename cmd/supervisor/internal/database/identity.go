package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
)

// NodeIdentity represents the local node identity information
type NodeIdentity struct {
	NodeID    string
	RoutingID int64
	MeshID    string
}

// GetLocalNodeIdentity retrieves the local node_id, routing_id, and mesh_id from the database
func GetLocalNodeIdentity(ctx context.Context, db *database.PostgreSQL) (*NodeIdentity, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	// Get the local node ID from the nodes table via localidentity
	// node_id is now used directly as the routing ID (it's a BIGINT)
	var nodeID int64
	nodeQuery := `
		SELECT n.node_id 
		FROM nodes n
		JOIN localidentity li ON n.node_id = li.identity_id
		LIMIT 1
	`

	err := db.Pool().QueryRow(ctx, nodeQuery).Scan(&nodeID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("local node identity not found - node may not be initialized")
		}
		return nil, fmt.Errorf("failed to query local node identity: %w", err)
	}

	// Try to get the mesh ID from the mesh table
	// If no mesh exists, this is a clean node
	var meshIDInt int64
	var meshID string
	meshQuery := `SELECT mesh_id FROM mesh LIMIT 1`

	err = db.Pool().QueryRow(ctx, meshQuery).Scan(&meshIDInt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No mesh found - this is a clean node
			meshID = "" // Empty mesh ID indicates clean node
		} else {
			return nil, fmt.Errorf("failed to query mesh identity: %w", err)
		}
	} else {
		// Convert BIGINT mesh_id to string for backwards compatibility
		meshID = fmt.Sprintf("%d", meshIDInt)
	}

	return &NodeIdentity{
		NodeID:    fmt.Sprintf("%d", nodeID),
		RoutingID: nodeID,
		MeshID:    meshID,
	}, nil
}

// ValidateNodeExists verifies that the node exists in the nodes table
func ValidateNodeExists(ctx context.Context, db *database.PostgreSQL, nodeID string) error {
	if db == nil {
		return fmt.Errorf("database connection is nil")
	}

	var exists bool
	// node_id is now a BIGINT, so parse the string to int64
	query := `SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = $1::BIGINT)`

	err := db.Pool().QueryRow(ctx, query, nodeID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to validate node existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("node %s not found in nodes table", nodeID)
	}

	return nil
}
