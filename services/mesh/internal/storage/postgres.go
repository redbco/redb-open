package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// PostgresStorage implements the Interface using PostgreSQL via the shared database package
type PostgresStorage struct {
	db     *database.PostgreSQL
	logger *logger.Logger
	config Config
}

// PostgresTransaction implements the Transaction interface for PostgreSQL
type PostgresTransaction struct {
	tx     pgx.Tx
	logger *logger.Logger
}

// NewPostgresStorage creates a new PostgreSQL storage instance using the shared database package
func NewPostgresStorage(ctx context.Context, config Config, logger *logger.Logger) (*PostgresStorage, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Use FromGlobalConfig to get the standard database configuration
	// This connects to the main "redb" database like other microservices
	dbConfig := database.FromGlobalConfig(nil)

	// Create database instance using shared package
	db, err := database.New(ctx, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL instance: %w", err)
	}

	logger.Info("PostgreSQL storage initialized successfully: (host: %s, port: %d, database: %s)",
		dbConfig.Host, dbConfig.Port, dbConfig.Database)

	return &PostgresStorage{
		db:     db,
		logger: logger,
		config: config,
	}, nil
}

// NewPostgresStorageWithGlobalConfig creates a new PostgreSQL storage instance using global config
func NewPostgresStorageWithGlobalConfig(ctx context.Context, globalConfig *config.Config, logger *logger.Logger) (*PostgresStorage, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Use FromGlobalConfig to get the standard database configuration
	dbConfig := database.FromGlobalConfig(globalConfig)

	// Create database instance using shared package
	db, err := database.New(ctx, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL instance: %w", err)
	}

	logger.Info("PostgreSQL storage initialized successfully: (host: %s, port: %d, database: %s)",
		dbConfig.Host, dbConfig.Port, dbConfig.Database)

	return &PostgresStorage{
		db:     db,
		logger: logger,
		config: Config{Type: "postgres"},
	}, nil
}

// Message operations

func (s *PostgresStorage) StoreMessage(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO mesh_messages (id, from_node, to_node, content, timestamp)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			from_node = EXCLUDED.from_node,
			to_node = EXCLUDED.to_node,
			content = EXCLUDED.content,
			timestamp = EXCLUDED.timestamp
	`, msg.ID, msg.From, msg.To, msg.Content, msg.Timestamp)

	if err != nil {
		s.logger.Errorf("Failed to store message (ID: %s): %v", msg.ID, err)
		return fmt.Errorf("failed to store message: %w", err)
	}

	return nil
}

func (s *PostgresStorage) GetMessage(ctx context.Context, id string) (*Message, error) {
	if id == "" {
		return nil, fmt.Errorf("message ID cannot be empty")
	}

	var msg Message
	err := s.db.Pool().QueryRow(ctx, `
		SELECT id, from_node, to_node, content, timestamp
		FROM mesh_messages WHERE id = $1
	`, id).Scan(&msg.ID, &msg.From, &msg.To, &msg.Content, &msg.Timestamp)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("message not found: %s", id)
		}
		s.logger.Errorf("Failed to get message (ID: %s): %v", id, err)
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	return &msg, nil
}

func (s *PostgresStorage) DeleteMessage(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("message ID cannot be empty")
	}

	result, err := s.db.Pool().Exec(ctx, `DELETE FROM mesh_messages WHERE id = $1`, id)
	if err != nil {
		s.logger.Errorf("Failed to delete message (ID: %s): %v", id, err)
		return fmt.Errorf("failed to delete message: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}

// State operations

func (s *PostgresStorage) StoreState(ctx context.Context, key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO mesh_state (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET
			value = EXCLUDED.value,
			updated_at = CURRENT_TIMESTAMP
	`, key, value)

	if err != nil {
		s.logger.Errorf("Failed to store state (key: %s): %v", key, err)
		return fmt.Errorf("failed to store state: %w", err)
	}

	return nil
}

func (s *PostgresStorage) GetState(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	var value []byte
	err := s.db.Pool().QueryRow(ctx, `
		SELECT value FROM mesh_state WHERE key = $1
	`, key).Scan(&value)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("state not found: %s", key)
		}
		s.logger.Errorf("Failed to get state (key: %s): %v", key, err)
		return nil, fmt.Errorf("failed to get state: %w", err)
	}

	return value, nil
}

func (s *PostgresStorage) DeleteState(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	result, err := s.db.Pool().Exec(ctx, `DELETE FROM mesh_state WHERE key = $1`, key)
	if err != nil {
		s.logger.Errorf("Failed to delete state (key: %s): %v", key, err)
		return fmt.Errorf("failed to delete state: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("state not found: %s", key)
	}

	return nil
}

// Node state operations

func (s *PostgresStorage) SaveNodeState(ctx context.Context, nodeID string, state interface{}) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	stateBytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	_, err = s.db.Pool().Exec(ctx, `
		INSERT INTO mesh_node_state (node_id, state)
		VALUES ($1, $2)
		ON CONFLICT (node_id) DO UPDATE SET
			state = EXCLUDED.state,
			updated_at = CURRENT_TIMESTAMP
	`, nodeID, stateBytes)

	if err != nil {
		s.logger.Errorf("Failed to save node state (node ID: %s): %v", nodeID, err)
		return fmt.Errorf("failed to save node state: %w", err)
	}

	return nil
}

func (s *PostgresStorage) GetNodeState(ctx context.Context, nodeID string) (interface{}, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	var stateBytes []byte
	err := s.db.Pool().QueryRow(ctx, `
		SELECT state FROM mesh_node_state WHERE node_id = $1
	`, nodeID).Scan(&stateBytes)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("node state not found: %s", nodeID)
		}
		s.logger.Errorf("Failed to get node state (node ID: %s): %v", nodeID, err)
		return nil, fmt.Errorf("failed to get node state: %w", err)
	}

	var state interface{}
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return state, nil
}

func (s *PostgresStorage) DeleteNodeState(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	result, err := s.db.Pool().Exec(ctx, `DELETE FROM mesh_node_state WHERE node_id = $1`, nodeID)
	if err != nil {
		s.logger.Errorf("Failed to delete node state (node ID: %s): %v", nodeID, err)
		return fmt.Errorf("failed to delete node state: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("node state not found: %s", nodeID)
	}

	return nil
}

// Consensus log operations

func (s *PostgresStorage) AppendLog(ctx context.Context, term uint64, index uint64, entry interface{}) error {
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	_, err = s.db.Pool().Exec(ctx, `
		INSERT INTO mesh_consensus_log (term, index, entry)
		VALUES ($1, $2, $3)
	`, term, index, entryBytes)

	if err != nil {
		s.logger.Errorf("Failed to append log (term: %d, index: %d): %v", term, index, err)
		return fmt.Errorf("failed to append log: %w", err)
	}

	return nil
}

func (s *PostgresStorage) GetLog(ctx context.Context, index uint64) (interface{}, error) {
	var entryBytes []byte
	err := s.db.Pool().QueryRow(ctx, `
		SELECT entry FROM mesh_consensus_log WHERE index = $1
	`, index).Scan(&entryBytes)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("log entry not found: %d", index)
		}
		s.logger.Errorf("Failed to get log (index: %d): %v", index, err)
		return nil, fmt.Errorf("failed to get log: %w", err)
	}

	var entry interface{}
	if err := json.Unmarshal(entryBytes, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
	}

	return entry, nil
}

func (s *PostgresStorage) GetLogs(ctx context.Context, startIndex, endIndex uint64) ([]interface{}, error) {
	rows, err := s.db.Pool().Query(ctx, `
		SELECT entry FROM mesh_consensus_log
		WHERE index >= $1 AND index <= $2
		ORDER BY index
	`, startIndex, endIndex)
	if err != nil {
		s.logger.Errorf("Failed to get logs (start: %d, end: %d): %v", startIndex, endIndex, err)
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}
	defer rows.Close()

	var entries []interface{}
	for rows.Next() {
		var entryBytes []byte
		if err := rows.Scan(&entryBytes); err != nil {
			return nil, fmt.Errorf("failed to scan log entry: %w", err)
		}

		var entry interface{}
		if err := json.Unmarshal(entryBytes, &entry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating log entries: %w", err)
	}

	return entries, nil
}

func (s *PostgresStorage) DeleteLogs(ctx context.Context, startIndex, endIndex uint64) error {
	_, err := s.db.Pool().Exec(ctx, `
		DELETE FROM mesh_consensus_log
		WHERE index >= $1 AND index <= $2
	`, startIndex, endIndex)

	if err != nil {
		s.logger.Errorf("Failed to delete logs (start: %d, end: %d): %v", startIndex, endIndex, err)
		return fmt.Errorf("failed to delete logs: %w", err)
	}

	return nil
}

// Route operations

func (s *PostgresStorage) SaveRoute(ctx context.Context, destination string, route interface{}) error {
	if destination == "" {
		return fmt.Errorf("destination cannot be empty")
	}

	routeBytes, err := json.Marshal(route)
	if err != nil {
		return fmt.Errorf("failed to marshal route: %w", err)
	}

	_, err = s.db.Pool().Exec(ctx, `
		INSERT INTO mesh_routing_table (destination, route)
		VALUES ($1, $2)
		ON CONFLICT (destination) DO UPDATE SET
			route = EXCLUDED.route,
			updated_at = CURRENT_TIMESTAMP
	`, destination, routeBytes)

	if err != nil {
		s.logger.Errorf("Failed to save route (destination: %s): %v", destination, err)
		return fmt.Errorf("failed to save route: %w", err)
	}

	return nil
}

func (s *PostgresStorage) GetRoute(ctx context.Context, destination string) (interface{}, error) {
	if destination == "" {
		return nil, fmt.Errorf("destination cannot be empty")
	}

	var routeBytes []byte
	err := s.db.Pool().QueryRow(ctx, `
		SELECT route FROM mesh_routing_table WHERE destination = $1
	`, destination).Scan(&routeBytes)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("route not found: %s", destination)
		}
		s.logger.Errorf("Failed to get route (destination: %s): %v", destination, err)
		return nil, fmt.Errorf("failed to get route: %w", err)
	}

	var route interface{}
	if err := json.Unmarshal(routeBytes, &route); err != nil {
		return nil, fmt.Errorf("failed to unmarshal route: %w", err)
	}

	return route, nil
}

func (s *PostgresStorage) GetRoutes(ctx context.Context) (map[string]interface{}, error) {
	rows, err := s.db.Pool().Query(ctx, `SELECT destination, route FROM mesh_routing_table`)
	if err != nil {
		s.logger.Errorf("Failed to get routes: %v", err)
		return nil, fmt.Errorf("failed to get routes: %w", err)
	}
	defer rows.Close()

	routes := make(map[string]interface{})
	for rows.Next() {
		var destination string
		var routeBytes []byte
		if err := rows.Scan(&destination, &routeBytes); err != nil {
			return nil, fmt.Errorf("failed to scan route: %w", err)
		}

		var route interface{}
		if err := json.Unmarshal(routeBytes, &route); err != nil {
			return nil, fmt.Errorf("failed to unmarshal route: %w", err)
		}
		routes[destination] = route
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating routes: %w", err)
	}

	return routes, nil
}

func (s *PostgresStorage) DeleteRoute(ctx context.Context, destination string) error {
	if destination == "" {
		return fmt.Errorf("destination cannot be empty")
	}

	result, err := s.db.Pool().Exec(ctx, `DELETE FROM mesh_routing_table WHERE destination = $1`, destination)
	if err != nil {
		s.logger.Errorf("Failed to delete route (destination: %s): %v", destination, err)
		return fmt.Errorf("failed to delete route: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("route not found: %s", destination)
	}

	return nil
}

// Configuration operations

func (s *PostgresStorage) SaveConfig(ctx context.Context, key string, value interface{}) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	valueBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	_, err = s.db.Pool().Exec(ctx, `
		INSERT INTO mesh_runtime_config (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET
			value = EXCLUDED.value,
			updated_at = CURRENT_TIMESTAMP
	`, key, valueBytes)

	if err != nil {
		s.logger.Errorf("Failed to save config (key: %s): %v", key, err)
		return fmt.Errorf("failed to save config: %w", err)
	}

	return nil
}

func (s *PostgresStorage) GetConfig(ctx context.Context, key string) (interface{}, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	var valueBytes []byte
	err := s.db.Pool().QueryRow(ctx, `
		SELECT value FROM mesh_runtime_config WHERE key = $1
	`, key).Scan(&valueBytes)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("config not found: %s", key)
		}
		s.logger.Errorf("Failed to get config (key: %s): %v", key, err)
		return nil, fmt.Errorf("failed to get config: %w", err)
	}

	var value interface{}
	if err := json.Unmarshal(valueBytes, &value); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return value, nil
}

func (s *PostgresStorage) DeleteConfig(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	result, err := s.db.Pool().Exec(ctx, `DELETE FROM mesh_runtime_config WHERE key = $1`, key)
	if err != nil {
		s.logger.Errorf("Failed to delete config (key: %s): %v", key, err)
		return fmt.Errorf("failed to delete config: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("config not found: %s", key)
	}

	return nil
}

// Transaction operations

func (s *PostgresStorage) CreateTransaction(ctx context.Context) (Transaction, error) {
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		s.logger.Errorf("Failed to create transaction: %v", err)
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	return &PostgresTransaction{
		tx:     tx,
		logger: s.logger,
	}, nil
}

// Administrative operations

func (s *PostgresStorage) CreateBackup(ctx context.Context, path string) error {
	// TODO: Implement backup functionality using pg_dump or similar
	s.logger.Warnf("Backup functionality not implemented (path: %s)", path)
	return fmt.Errorf("backup functionality not implemented")
}

func (s *PostgresStorage) RestoreFromBackup(ctx context.Context, path string) error {
	// TODO: Implement restore functionality using pg_restore or similar
	s.logger.Warnf("Restore functionality not implemented (path: %s)", path)
	return fmt.Errorf("restore functionality not implemented")
}

// Connection management

func (s *PostgresStorage) Close() error {
	if s.db != nil {
		s.db.Close()
		s.logger.Info("PostgreSQL storage closed")
	}
	return nil
}

// Mesh initialization operations

func (s *PostgresStorage) GetLocalIdentity(ctx context.Context) (*LocalIdentity, error) {
	var identity LocalIdentity
	err := s.db.Pool().QueryRow(ctx, `
		SELECT identity_id FROM localidentity LIMIT 1
	`).Scan(&identity.IdentityID)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // No local identity found - this is a valid state
		}
		s.logger.Errorf("Failed to get local identity: %v", err)
		return nil, fmt.Errorf("failed to get local identity: %w", err)
	}

	return &identity, nil
}

func (s *PostgresStorage) GetMeshInfo(ctx context.Context) (*MeshInfo, error) {
	var mesh MeshInfo
	err := s.db.Pool().QueryRow(ctx, `
		SELECT mesh_id, mesh_name, mesh_description, 
		       allow_join, status, created, updated
		FROM mesh LIMIT 1
	`).Scan(&mesh.MeshID, &mesh.MeshName, &mesh.MeshDescription,
		&mesh.AllowJoin, &mesh.Status,
		&mesh.Created, &mesh.Updated)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // No mesh found - this is a valid state
		}
		s.logger.Errorf("Failed to get mesh info: %v", err)
		return nil, fmt.Errorf("failed to get mesh info: %w", err)
	}

	return &mesh, nil
}

func (s *PostgresStorage) GetNodeInfo(ctx context.Context, nodeID string) (*NodeInfo, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	var node NodeInfo
	err := s.db.Pool().QueryRow(ctx, `
		SELECT node_id, node_name, node_description, node_platform, node_version,
		       region_id, ip_address, port, status, created, updated
		FROM nodes WHERE node_id = $1
	`, nodeID).Scan(&node.NodeID, &node.NodeName, &node.NodeDescription, &node.NodePlatform,
		&node.NodeVersion, &node.RegionID,
		&node.IPAddress, &node.Port, &node.Status, &node.Created, &node.Updated)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // Node not found
		}
		s.logger.Errorf("Failed to get node info (node ID: %s): %v", nodeID, err)
		return nil, fmt.Errorf("failed to get node info: %w", err)
	}

	return &node, nil
}

func (s *PostgresStorage) GetRoutesForNode(ctx context.Context, nodeID string) ([]*RouteInfo, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	rows, err := s.db.Pool().Query(ctx, `
		SELECT route_id, source_node_id, target_node_id, route_bidirectional,
		       route_latency, route_bandwidth, route_cost, status, created, updated
		FROM routes WHERE source_node_id = $1
	`, nodeID)

	if err != nil {
		s.logger.Errorf("Failed to get routes for node (node ID: %s): %v", nodeID, err)
		return nil, fmt.Errorf("failed to get routes for node: %w", err)
	}
	defer rows.Close()

	var routes []*RouteInfo
	for rows.Next() {
		var route RouteInfo
		err := rows.Scan(&route.RouteID, &route.SourceNodeID, &route.TargetNodeID,
			&route.RouteBidirectional, &route.RouteLatency, &route.RouteBandwidth,
			&route.RouteCost, &route.Status, &route.Created, &route.Updated)
		if err != nil {
			s.logger.Errorf("Failed to scan route: %v", err)
			return nil, fmt.Errorf("failed to scan route: %w", err)
		}
		routes = append(routes, &route)
	}

	if err := rows.Err(); err != nil {
		s.logger.Errorf("Error iterating routes: %v", err)
		return nil, fmt.Errorf("error iterating routes: %w", err)
	}

	return routes, nil
}

// PostgresTransaction implementation

func (tx *PostgresTransaction) StoreMessage(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	_, err := tx.tx.Exec(ctx, `
		INSERT INTO mesh_messages (id, from_node, to_node, content, timestamp)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			from_node = EXCLUDED.from_node,
			to_node = EXCLUDED.to_node,
			content = EXCLUDED.content,
			timestamp = EXCLUDED.timestamp
	`, msg.ID, msg.From, msg.To, msg.Content, msg.Timestamp)

	if err != nil {
		tx.logger.Errorf("Failed to store message in transaction (ID: %s): %v", msg.ID, err)
		return fmt.Errorf("failed to store message: %w", err)
	}

	return nil
}

func (tx *PostgresTransaction) GetMessage(ctx context.Context, id string) (*Message, error) {
	if id == "" {
		return nil, fmt.Errorf("message ID cannot be empty")
	}

	var msg Message
	err := tx.tx.QueryRow(ctx, `
		SELECT id, from_node, to_node, content, timestamp
		FROM mesh_messages WHERE id = $1
	`, id).Scan(&msg.ID, &msg.From, &msg.To, &msg.Content, &msg.Timestamp)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("message not found: %s", id)
		}
		tx.logger.Errorf("Failed to get message in transaction (ID: %s): %v", id, err)
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	return &msg, nil
}

func (tx *PostgresTransaction) StoreState(ctx context.Context, key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	_, err := tx.tx.Exec(ctx, `
		INSERT INTO mesh_state (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET
			value = EXCLUDED.value,
			updated_at = CURRENT_TIMESTAMP
	`, key, value)

	if err != nil {
		tx.logger.Errorf("Failed to store state in transaction (key: %s): %v", key, err)
		return fmt.Errorf("failed to store state: %w", err)
	}

	return nil
}

func (tx *PostgresTransaction) GetState(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	var value []byte
	err := tx.tx.QueryRow(ctx, `
		SELECT value FROM mesh_state WHERE key = $1
	`, key).Scan(&value)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("state not found: %s", key)
		}
		tx.logger.Errorf("Failed to get state in transaction (key: %s): %v", key, err)
		return nil, fmt.Errorf("failed to get state: %w", err)
	}

	return value, nil
}

func (tx *PostgresTransaction) Commit() error {
	err := tx.tx.Commit(context.Background())
	if err != nil {
		tx.logger.Errorf("Failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (tx *PostgresTransaction) Rollback() error {
	err := tx.tx.Rollback(context.Background())
	if err != nil {
		tx.logger.Errorf("Failed to rollback transaction: %v", err)
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	return nil
}
