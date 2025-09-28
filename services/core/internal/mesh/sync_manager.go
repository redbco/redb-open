package mesh

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	meshv1 "github.com/redbco/redb-open/api/proto/mesh/v1"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// DatabaseSyncManager handles database synchronization across mesh nodes
type DatabaseSyncManager struct {
	db           *database.PostgreSQL
	meshManager  *MeshCommunicationManager
	logger       *logger.Logger
	nodeID       uint64
	eventManager *MeshEventManager

	// Configuration
	syncTables []string // Tables to keep synchronized
}

// NewDatabaseSyncManager creates a new database sync manager
func NewDatabaseSyncManager(
	db *database.PostgreSQL,
	meshManager *MeshCommunicationManager,
	logger *logger.Logger,
	nodeID uint64,
) *DatabaseSyncManager {
	return &DatabaseSyncManager{
		db:          db,
		meshManager: meshManager,
		logger:      logger,
		nodeID:      nodeID,
		syncTables: []string{
			"mesh",
			"nodes",
			"mesh_node_membership",
			"mesh_consensus_state",
			// Add other tables that need synchronization
		},
	}
}

// SetEventManager sets the event manager (circular dependency resolution)
func (s *DatabaseSyncManager) SetEventManager(eventManager *MeshEventManager) {
	s.eventManager = eventManager
}

// SyncWithNode synchronizes database with a specific node
func (s *DatabaseSyncManager) SyncWithNode(ctx context.Context, targetNodeID uint64) error {
	s.logger.Infof("Starting database sync with node %d", targetNodeID)

	for _, tableName := range s.syncTables {
		if err := s.syncTable(ctx, tableName, targetNodeID); err != nil {
			s.logger.Errorf("Failed to sync table %s with node %d: %v", tableName, targetNodeID, err)
			return fmt.Errorf("failed to sync table %s: %w", tableName, err)
		}
	}

	s.logger.Infof("Completed database sync with node %d", targetNodeID)
	return nil
}

// FullSync performs full database synchronization with all available nodes
func (s *DatabaseSyncManager) FullSync(ctx context.Context) error {
	s.logger.Infof("Starting full database synchronization")

	// Get list of online nodes
	sessionsResp, err := s.meshManager.meshControlClient.GetSessions(ctx, &meshv1.GetSessionsRequest{})
	if err != nil {
		return fmt.Errorf("failed to get online nodes: %w", err)
	}

	if len(sessionsResp.Sessions) == 0 {
		s.logger.Infof("No other nodes online, skipping full sync")
		return nil
	}

	// Sync with each online node
	for _, session := range sessionsResp.Sessions {
		if err := s.SyncWithNode(ctx, session.PeerNodeId); err != nil {
			s.logger.Errorf("Failed to sync with node %d: %v", session.PeerNodeId, err)
			// Continue with other nodes even if one fails
		}
	}

	s.logger.Infof("Completed full database synchronization")
	return nil
}

// HandleSyncRequest handles a database sync request from another node
func (s *DatabaseSyncManager) HandleSyncRequest(ctx context.Context, req *corev1.HandleDatabaseSyncRequestMessage) (*corev1.HandleDatabaseSyncResponse, error) {
	s.logger.Infof("Handling database sync request for table %s from node %d", req.TableName, req.RequestingNode)

	// Get current table version
	currentVersion, err := s.getTableVersion(ctx, req.TableName)
	if err != nil {
		return &corev1.HandleDatabaseSyncResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get table version: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	// If requesting node is up to date, return empty response
	if req.LastKnownVersion >= currentVersion {
		return &corev1.HandleDatabaseSyncResponse{
			TableName:      req.TableName,
			CurrentVersion: currentVersion,
			Records:        []*corev1.DatabaseRecord{},
			HasMore:        false,
			Success:        true,
			Message:        "Table is up to date",
			Status:         commonv1.Status_STATUS_SUCCESS,
		}, nil
	}

	// Get records newer than the requested version
	records, err := s.getTableRecords(ctx, req.TableName, req.LastKnownVersion, req.NodeIds)
	if err != nil {
		return &corev1.HandleDatabaseSyncResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to get table records: %v", err),
			Status:  commonv1.Status_STATUS_ERROR,
		}, nil
	}

	return &corev1.HandleDatabaseSyncResponse{
		TableName:      req.TableName,
		CurrentVersion: currentVersion,
		Records:        records,
		HasMore:        false, // For simplicity, return all records in one response
		Success:        true,
		Message:        fmt.Sprintf("Returned %d records", len(records)),
		Status:         commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ApplyRemoteChanges applies database changes received from another node
func (s *DatabaseSyncManager) ApplyRemoteChanges(ctx context.Context, tableName string, records []*corev1.DatabaseRecord) error {
	s.logger.Infof("Applying %d remote changes to table %s", len(records), tableName)

	for _, record := range records {
		if err := s.applyRecord(ctx, tableName, record); err != nil {
			s.logger.Errorf("Failed to apply record to table %s: %v", tableName, err)
			return fmt.Errorf("failed to apply record: %w", err)
		}
	}

	// Update table version
	if len(records) > 0 {
		latestVersion := records[len(records)-1].Version
		if err := s.updateTableVersion(ctx, tableName, latestVersion); err != nil {
			s.logger.Errorf("Failed to update table version for %s: %v", tableName, err)
		}
	}

	s.logger.Infof("Successfully applied %d changes to table %s", len(records), tableName)
	return nil
}

// syncTable synchronizes a specific table with a target node
func (s *DatabaseSyncManager) syncTable(ctx context.Context, tableName string, targetNodeID uint64) error {
	// Get our current version of the table
	currentVersion, err := s.getTableVersion(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get current table version: %w", err)
	}

	// Request sync from mesh service
	syncReq := &meshv1.DatabaseSyncRequest{
		TableName:        tableName,
		LastKnownVersion: currentVersion,
		NodeIds:          []string{fmt.Sprintf("%d", s.nodeID)},
	}

	syncResp, err := s.meshManager.RequestDatabaseSync(ctx, syncReq)
	if err != nil {
		return fmt.Errorf("failed to request database sync: %w", err)
	}

	// Convert mesh response to core format and apply changes
	if len(syncResp.Records) > 0 {
		coreRecords := make([]*corev1.DatabaseRecord, len(syncResp.Records))
		for i, record := range syncResp.Records {
			coreRecords[i] = &corev1.DatabaseRecord{
				Operation: record.Operation,
				Data:      record.Data,
				Version:   record.Version,
				Timestamp: record.Timestamp,
			}
		}

		if err := s.ApplyRemoteChanges(ctx, tableName, coreRecords); err != nil {
			return fmt.Errorf("failed to apply remote changes: %w", err)
		}
	}

	return nil
}

// getTableVersion gets the current version of a table
func (s *DatabaseSyncManager) getTableVersion(ctx context.Context, tableName string) (uint64, error) {
	var version uint64
	query := `SELECT version FROM mesh_table_versions WHERE table_name = $1`

	err := s.db.Pool().QueryRow(ctx, query, tableName).Scan(&version)
	if err != nil {
		// If table version doesn't exist, initialize it
		if strings.Contains(err.Error(), "no rows") {
			if err := s.initializeTableVersion(ctx, tableName); err != nil {
				return 0, fmt.Errorf("failed to initialize table version: %w", err)
			}
			return 0, nil
		}
		return 0, err
	}

	return version, nil
}

// updateTableVersion updates the version of a table
func (s *DatabaseSyncManager) updateTableVersion(ctx context.Context, tableName string, version uint64) error {
	query := `
		INSERT INTO mesh_table_versions (table_name, version, last_updated)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (table_name) DO UPDATE SET
			version = EXCLUDED.version,
			last_updated = EXCLUDED.last_updated
	`

	_, err := s.db.Pool().Exec(ctx, query, tableName, version)
	return err
}

// initializeTableVersion initializes the version tracking for a table
func (s *DatabaseSyncManager) initializeTableVersion(ctx context.Context, tableName string) error {
	query := `
		INSERT INTO mesh_table_versions (table_name, version, last_updated)
		VALUES ($1, 0, CURRENT_TIMESTAMP)
		ON CONFLICT (table_name) DO NOTHING
	`

	_, err := s.db.Pool().Exec(ctx, query, tableName)
	return err
}

// getTableRecords gets records from a table for synchronization
func (s *DatabaseSyncManager) getTableRecords(ctx context.Context, tableName string, sinceVersion uint64, nodeIDs []string) ([]*corev1.DatabaseRecord, error) {
	var records []*corev1.DatabaseRecord

	// This is a simplified implementation
	// In a real system, you'd need proper change tracking
	switch tableName {
	case "mesh":
		return s.getMeshRecords(ctx, sinceVersion)
	case "nodes":
		return s.getNodeRecords(ctx, sinceVersion, nodeIDs)
	case "mesh_node_membership":
		return s.getMembershipRecords(ctx, sinceVersion)
	case "mesh_consensus_state":
		return s.getConsensusRecords(ctx, sinceVersion)
	default:
		s.logger.Warnf("Sync not implemented for table: %s", tableName)
		return records, nil
	}
}

// getMeshRecords gets mesh table records for sync
func (s *DatabaseSyncManager) getMeshRecords(ctx context.Context, sinceVersion uint64) ([]*corev1.DatabaseRecord, error) {
	query := `
		SELECT mesh_id, mesh_name, mesh_description, allow_join, status, split_strategy, created, updated
		FROM mesh
		ORDER BY updated DESC
		LIMIT 100
	`

	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*corev1.DatabaseRecord
	version := sinceVersion + 1

	for rows.Next() {
		var meshID, meshName, meshDescription, allowJoin, status, splitStrategy string
		var created, updated time.Time

		if err := rows.Scan(&meshID, &meshName, &meshDescription, &allowJoin, &status, &splitStrategy, &created, &updated); err != nil {
			return nil, err
		}

		record := &corev1.DatabaseRecord{
			Operation: "UPSERT",
			Data: map[string]string{
				"mesh_id":          meshID,
				"mesh_name":        meshName,
				"mesh_description": meshDescription,
				"allow_join":       allowJoin,
				"status":           status,
				"split_strategy":   splitStrategy,
				"created":          created.Format(time.RFC3339),
				"updated":          updated.Format(time.RFC3339),
			},
			Version:   version,
			Timestamp: uint64(updated.Unix()),
		}

		records = append(records, record)
		version++
	}

	return records, rows.Err()
}

// getNodeRecords gets node table records for sync
func (s *DatabaseSyncManager) getNodeRecords(ctx context.Context, sinceVersion uint64, nodeIDs []string) ([]*corev1.DatabaseRecord, error) {
	query := `
		SELECT node_id, node_name, node_description, routing_id, ip_address, port, status, seed_node, created, updated
		FROM nodes
		ORDER BY updated DESC
		LIMIT 100
	`

	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*corev1.DatabaseRecord
	version := sinceVersion + 1

	for rows.Next() {
		var nodeID, nodeName, nodeDescription, ipAddress, status string
		var routingID, port int64
		var seedNode bool
		var created, updated time.Time

		if err := rows.Scan(&nodeID, &nodeName, &nodeDescription, &routingID, &ipAddress, &port, &status, &seedNode, &created, &updated); err != nil {
			return nil, err
		}

		record := &corev1.DatabaseRecord{
			Operation: "UPSERT",
			Data: map[string]string{
				"node_id":          nodeID,
				"node_name":        nodeName,
				"node_description": nodeDescription,
				"routing_id":       strconv.FormatInt(routingID, 10),
				"ip_address":       ipAddress,
				"port":             strconv.FormatInt(port, 10),
				"status":           status,
				"seed_node":        strconv.FormatBool(seedNode),
				"created":          created.Format(time.RFC3339),
				"updated":          updated.Format(time.RFC3339),
			},
			Version:   version,
			Timestamp: uint64(updated.Unix()),
		}

		records = append(records, record)
		version++
	}

	return records, rows.Err()
}

// getMembershipRecords gets membership table records for sync
func (s *DatabaseSyncManager) getMembershipRecords(ctx context.Context, sinceVersion uint64) ([]*corev1.DatabaseRecord, error) {
	query := `
		SELECT mesh_id, node_id, joined_at, status
		FROM mesh_node_membership
		ORDER BY joined_at DESC
		LIMIT 100
	`

	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*corev1.DatabaseRecord
	version := sinceVersion + 1

	for rows.Next() {
		var meshID, nodeID, status string
		var joinedAt time.Time

		if err := rows.Scan(&meshID, &nodeID, &joinedAt, &status); err != nil {
			return nil, err
		}

		record := &corev1.DatabaseRecord{
			Operation: "UPSERT",
			Data: map[string]string{
				"mesh_id":   meshID,
				"node_id":   nodeID,
				"joined_at": joinedAt.Format(time.RFC3339),
				"status":    status,
			},
			Version:   version,
			Timestamp: uint64(joinedAt.Unix()),
		}

		records = append(records, record)
		version++
	}

	return records, rows.Err()
}

// getConsensusRecords gets consensus state records for sync
func (s *DatabaseSyncManager) getConsensusRecords(ctx context.Context, sinceVersion uint64) ([]*corev1.DatabaseRecord, error) {
	query := `
		SELECT mesh_id, total_nodes, online_nodes, split_detected, majority_side, last_consensus_check
		FROM mesh_consensus_state
		ORDER BY last_consensus_check DESC
		LIMIT 10
	`

	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*corev1.DatabaseRecord
	version := sinceVersion + 1

	for rows.Next() {
		var meshID string
		var totalNodes, onlineNodes int
		var splitDetected, majoritySide bool
		var lastCheck time.Time

		if err := rows.Scan(&meshID, &totalNodes, &onlineNodes, &splitDetected, &majoritySide, &lastCheck); err != nil {
			return nil, err
		}

		record := &corev1.DatabaseRecord{
			Operation: "UPSERT",
			Data: map[string]string{
				"mesh_id":              meshID,
				"total_nodes":          strconv.Itoa(totalNodes),
				"online_nodes":         strconv.Itoa(onlineNodes),
				"split_detected":       strconv.FormatBool(splitDetected),
				"majority_side":        strconv.FormatBool(majoritySide),
				"last_consensus_check": lastCheck.Format(time.RFC3339),
			},
			Version:   version,
			Timestamp: uint64(lastCheck.Unix()),
		}

		records = append(records, record)
		version++
	}

	return records, rows.Err()
}

// applyRecord applies a single database record
func (s *DatabaseSyncManager) applyRecord(ctx context.Context, tableName string, record *corev1.DatabaseRecord) error {
	switch record.Operation {
	case "INSERT", "UPSERT":
		return s.upsertRecord(ctx, tableName, record.Data)
	case "UPDATE":
		return s.updateRecord(ctx, tableName, record.Data)
	case "DELETE":
		return s.deleteRecord(ctx, tableName, record.Data)
	default:
		return fmt.Errorf("unknown operation: %s", record.Operation)
	}
}

// upsertRecord inserts or updates a record
func (s *DatabaseSyncManager) upsertRecord(ctx context.Context, tableName string, data map[string]string) error {
	// This is a simplified implementation
	// In a real system, you'd need proper upsert logic for each table
	switch tableName {
	case "mesh":
		return s.upsertMeshRecord(ctx, data)
	case "nodes":
		return s.upsertNodeRecord(ctx, data)
	case "mesh_node_membership":
		return s.upsertMembershipRecord(ctx, data)
	case "mesh_consensus_state":
		return s.upsertConsensusRecord(ctx, data)
	default:
		s.logger.Warnf("Upsert not implemented for table: %s", tableName)
		return nil
	}
}

// upsertMeshRecord upserts a mesh record
func (s *DatabaseSyncManager) upsertMeshRecord(ctx context.Context, data map[string]string) error {
	query := `
		INSERT INTO mesh (mesh_id, mesh_name, mesh_description, allow_join, status, split_strategy)
		VALUES ($1, $2, $3, $4::join_key_enum, $5::status_enum, $6)
		ON CONFLICT (mesh_id) DO UPDATE SET
			mesh_name = EXCLUDED.mesh_name,
			mesh_description = EXCLUDED.mesh_description,
			allow_join = EXCLUDED.allow_join,
			status = EXCLUDED.status,
			split_strategy = EXCLUDED.split_strategy,
			updated = CURRENT_TIMESTAMP
	`

	_, err := s.db.Pool().Exec(ctx, query,
		data["mesh_id"],
		data["mesh_name"],
		data["mesh_description"],
		data["allow_join"],
		data["status"],
		data["split_strategy"],
	)

	return err
}

// upsertNodeRecord upserts a node record
func (s *DatabaseSyncManager) upsertNodeRecord(ctx context.Context, data map[string]string) error {
	routingID, _ := strconv.ParseInt(data["routing_id"], 10, 64)
	port, _ := strconv.ParseInt(data["port"], 10, 32)
	seedNode, _ := strconv.ParseBool(data["seed_node"])

	query := `
		INSERT INTO nodes (node_id, node_name, node_description, routing_id, ip_address, port, status, seed_node)
		VALUES ($1, $2, $3, $4, $5::inet, $6, $7::status_enum, $8)
		ON CONFLICT (node_id) DO UPDATE SET
			node_name = EXCLUDED.node_name,
			node_description = EXCLUDED.node_description,
			routing_id = EXCLUDED.routing_id,
			ip_address = EXCLUDED.ip_address,
			port = EXCLUDED.port,
			status = EXCLUDED.status,
			seed_node = EXCLUDED.seed_node,
			updated = CURRENT_TIMESTAMP
	`

	_, err := s.db.Pool().Exec(ctx, query,
		data["node_id"],
		data["node_name"],
		data["node_description"],
		routingID,
		data["ip_address"],
		int32(port),
		data["status"],
		seedNode,
	)

	return err
}

// upsertMembershipRecord upserts a membership record
func (s *DatabaseSyncManager) upsertMembershipRecord(ctx context.Context, data map[string]string) error {
	query := `
		INSERT INTO mesh_node_membership (mesh_id, node_id, status)
		VALUES ($1, $2, $3)
		ON CONFLICT (mesh_id, node_id) DO UPDATE SET
			status = EXCLUDED.status
	`

	_, err := s.db.Pool().Exec(ctx, query,
		data["mesh_id"],
		data["node_id"],
		data["status"],
	)

	return err
}

// upsertConsensusRecord upserts a consensus state record
func (s *DatabaseSyncManager) upsertConsensusRecord(ctx context.Context, data map[string]string) error {
	totalNodes, _ := strconv.Atoi(data["total_nodes"])
	onlineNodes, _ := strconv.Atoi(data["online_nodes"])
	splitDetected, _ := strconv.ParseBool(data["split_detected"])
	majoritySide, _ := strconv.ParseBool(data["majority_side"])

	query := `
		INSERT INTO mesh_consensus_state (mesh_id, total_nodes, online_nodes, split_detected, majority_side)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (mesh_id) DO UPDATE SET
			total_nodes = EXCLUDED.total_nodes,
			online_nodes = EXCLUDED.online_nodes,
			split_detected = EXCLUDED.split_detected,
			majority_side = EXCLUDED.majority_side,
			last_consensus_check = CURRENT_TIMESTAMP
	`

	_, err := s.db.Pool().Exec(ctx, query,
		data["mesh_id"],
		totalNodes,
		onlineNodes,
		splitDetected,
		majoritySide,
	)

	return err
}

// updateRecord and deleteRecord would be implemented similarly
func (s *DatabaseSyncManager) updateRecord(ctx context.Context, tableName string, data map[string]string) error {
	// For simplicity, treat updates as upserts
	return s.upsertRecord(ctx, tableName, data)
}

func (s *DatabaseSyncManager) deleteRecord(ctx context.Context, tableName string, data map[string]string) error {
	s.logger.Infof("Delete operation not implemented for table %s", tableName)
	return nil
}
