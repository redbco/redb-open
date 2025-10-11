package mesh

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
		SELECT node_id, node_name, node_description, ip_address, port, status, seed_node, created, updated
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
		var nodeName, nodeDescription, ipAddress, status string
		var nodeID, port int64
		var seedNode bool
		var created, updated time.Time

		if err := rows.Scan(&nodeID, &nodeName, &nodeDescription, &ipAddress, &port, &status, &seedNode, &created, &updated); err != nil {
			return nil, err
		}

		record := &corev1.DatabaseRecord{
			Operation: "UPSERT",
			Data: map[string]string{
				"node_id":          strconv.FormatInt(nodeID, 10),
				"node_name":        nodeName,
				"node_description": nodeDescription,
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
	nodeID, _ := strconv.ParseInt(data["node_id"], 10, 64)
	port, _ := strconv.ParseInt(data["port"], 10, 32)
	seedNode, _ := strconv.ParseBool(data["seed_node"])

	query := `
		INSERT INTO nodes (node_id, node_name, node_description, ip_address, port, status, seed_node)
		VALUES ($1, $2, $3, $4::inet, $5, $6::status_enum, $7)
		ON CONFLICT (node_id) DO UPDATE SET
			node_name = EXCLUDED.node_name,
			node_description = EXCLUDED.node_description,
			ip_address = EXCLUDED.ip_address,
			port = EXCLUDED.port,
			status = EXCLUDED.status,
			seed_node = EXCLUDED.seed_node,
			updated = CURRENT_TIMESTAMP
	`

	_, err := s.db.Pool().Exec(ctx, query,
		nodeID,
		data["node_name"],
		data["node_description"],
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

// === Mesh Synchronization Methods ===

// GetMeshDataForSync retrieves mesh, nodes, and routes data for synchronization
func (s *DatabaseSyncManager) GetMeshDataForSync(ctx context.Context) (map[string]interface{}, map[string]interface{}, map[string]interface{}, error) {
	s.logger.Info("Gathering mesh data for synchronization")

	meshData := make(map[string]interface{})
	nodesData := make(map[string]interface{})
	routesData := make(map[string]interface{})

	// Get mesh information
	meshQuery := `
		SELECT mesh_id, mesh_name, mesh_description, allow_join, status, created, updated
		FROM mesh
		LIMIT 1
	`
	var meshID int64
	var meshName, meshDescription, allowJoin, meshStatus string
	var created, updated time.Time

	err := s.db.Pool().QueryRow(ctx, meshQuery).Scan(&meshID, &meshName, &meshDescription, &allowJoin, &meshStatus, &created, &updated)
	if err != nil {
		s.logger.Warnf("No mesh found for sync: %v", err)
	} else {
		meshData["mesh_id"] = meshID
		meshData["mesh_name"] = meshName
		meshData["mesh_description"] = meshDescription
		meshData["allow_join"] = allowJoin
		meshData["status"] = meshStatus
		meshData["created"] = created.Format(time.RFC3339)
		meshData["updated"] = updated.Format(time.RFC3339)
	}

	// Get all nodes (including region_id for later restoration after user data sync)
	nodesQuery := `
		SELECT node_id, node_name, node_description, node_public_key, host(ip_address) as ip_address, port, status, seed_node, region_id
		FROM nodes
		ORDER BY node_id
	`
	rows, err := s.db.Pool().Query(ctx, nodesQuery)
	if err != nil {
		s.logger.Errorf("Failed to query nodes: %v", err)
	} else {
		defer rows.Close()
		nodesList := []map[string]interface{}{}

		for rows.Next() {
			var nodeID, port int64
			var nodeName, nodeDescription, ipAddress, nodeStatus string
			var nodePublicKey []byte
			var seedNode bool
			var regionID *string // nullable

			if err := rows.Scan(&nodeID, &nodeName, &nodeDescription, &nodePublicKey, &ipAddress, &port, &nodeStatus, &seedNode, &regionID); err != nil {
				s.logger.Warnf("Failed to scan node row: %v", err)
				continue
			}

			nodeData := map[string]interface{}{
				"node_id":          fmt.Sprintf("%d", nodeID), // Send as string to preserve int64 precision
				"node_name":        nodeName,
				"node_description": nodeDescription,
				"node_public_key":  string(nodePublicKey), // Convert to string for JSON transmission
				"ip_address":       ipAddress,
				"port":             port,
				"status":           nodeStatus,
				"seed_node":        seedNode,
			}

			// Include original region_id for potential restoration after user data sync
			// But it will be set to NULL during initial node insertion (Step 1)
			if regionID != nil {
				nodeData["original_region_id"] = *regionID
			}

			nodesList = append(nodesList, nodeData)
		}
		nodesData["nodes"] = nodesList
		s.logger.Infof("Found %d nodes for sync", len(nodesList))
	}

	// Get all routes
	routesQuery := `
		SELECT a_node, b_node, latency_ms, status
		FROM routes
		ORDER BY a_node, b_node
	`
	routeRows, err := s.db.Pool().Query(ctx, routesQuery)
	if err != nil {
		s.logger.Errorf("Failed to query routes: %v", err)
	} else {
		defer routeRows.Close()
		routesList := []map[string]interface{}{}

		for routeRows.Next() {
			var aNode, bNode int64
			var latencyMs int32
			var routeStatus string

			if err := routeRows.Scan(&aNode, &bNode, &latencyMs, &routeStatus); err != nil {
				s.logger.Warnf("Failed to scan route row: %v", err)
				continue
			}

			routesList = append(routesList, map[string]interface{}{
				"a_node":     fmt.Sprintf("%d", aNode), // Send as string to preserve int64 precision
				"b_node":     fmt.Sprintf("%d", bNode), // Send as string to preserve int64 precision
				"latency_ms": latencyMs,
				"status":     routeStatus,
			})
		}
		routesData["routes"] = routesList
		s.logger.Infof("Found %d routes for sync", len(routesList))
	}

	return meshData, nodesData, routesData, nil
}

// ApplySyncedMeshData applies synced mesh data to the local database
func (s *DatabaseSyncManager) ApplySyncedMeshData(ctx context.Context, data map[string]interface{}) error {
	s.logger.Info("Applying synced mesh data to local database")

	// Apply mesh data
	if meshData, ok := data["mesh"].(map[string]interface{}); ok {
		s.logger.Infof("Applying mesh data")
		if err := s.upsertMesh(ctx, meshData); err != nil {
			return fmt.Errorf("failed to upsert mesh: %w", err)
		}
	}

	// Apply nodes data
	if nodesData, ok := data["nodes"].(map[string]interface{}); ok {
		if nodesList, ok := nodesData["nodes"].([]interface{}); ok {
			s.logger.Infof("Applying %d nodes", len(nodesList))
			for _, nodeInterface := range nodesList {
				if node, ok := nodeInterface.(map[string]interface{}); ok {
					// Parse node_id from string to preserve int64 precision
					if nodeIDStr, ok := node["node_id"].(string); ok {
						if nodeID, err := strconv.ParseInt(nodeIDStr, 10, 64); err == nil {
							node["node_id"] = nodeID
						} else {
							s.logger.Warnf("Failed to parse node_id '%s': %v", nodeIDStr, err)
							continue
						}
					}
					if err := s.upsertNode(ctx, node); err != nil {
						s.logger.Warnf("Failed to upsert node: %v", err)
					}
				}
			}
		}
	}

	// Apply routes data
	var syncedNodeIDs []int64
	if routesData, ok := data["routes"].(map[string]interface{}); ok {
		if routesList, ok := routesData["routes"].([]interface{}); ok {
			s.logger.Infof("Applying %d routes", len(routesList))
			for _, routeInterface := range routesList {
				if route, ok := routeInterface.(map[string]interface{}); ok {
					// Parse a_node and b_node from string to preserve int64 precision
					if aNodeStr, ok := route["a_node"].(string); ok {
						if aNode, err := strconv.ParseInt(aNodeStr, 10, 64); err == nil {
							route["a_node"] = aNode
						} else {
							s.logger.Warnf("Failed to parse a_node '%s': %v", aNodeStr, err)
							continue
						}
					}
					if bNodeStr, ok := route["b_node"].(string); ok {
						if bNode, err := strconv.ParseInt(bNodeStr, 10, 64); err == nil {
							route["b_node"] = bNode
						} else {
							s.logger.Warnf("Failed to parse b_node '%s': %v", bNodeStr, err)
							continue
						}
					}
					if err := s.upsertRoute(ctx, route); err != nil {
						s.logger.Warnf("Failed to upsert route: %v", err)
					}
				}
			}
		}
	}

	// Create routes to all synced nodes if not already present
	// This handles the case where seed node hasn't created routes yet
	if nodesData, ok := data["nodes"].(map[string]interface{}); ok {
		if nodesList, ok := nodesData["nodes"].([]interface{}); ok {
			for _, nodeInterface := range nodesList {
				if node, ok := nodeInterface.(map[string]interface{}); ok {
					var nodeID int64
					if nodeIDStr, ok := node["node_id"].(string); ok {
						if parsedID, err := strconv.ParseInt(nodeIDStr, 10, 64); err == nil {
							nodeID = parsedID
						}
					} else if nodeIDInt, ok := node["node_id"].(int64); ok {
						nodeID = nodeIDInt
					} else if nodeIDFloat, ok := node["node_id"].(float64); ok {
						nodeID = int64(nodeIDFloat)
					}

					if nodeID != 0 {
						syncedNodeIDs = append(syncedNodeIDs, nodeID)
					}
				}
			}
		}
	}

	// Get local node ID
	localNodeID, err := s.getLocalNodeID(ctx)
	if err == nil && len(syncedNodeIDs) > 0 {
		s.logger.Infof("Creating routes to %d synced nodes", len(syncedNodeIDs))
		for _, remoteNodeID := range syncedNodeIDs {
			if remoteNodeID == localNodeID {
				continue // Skip route to self
			}

			// Create bidirectional routes
			routeData := map[string]interface{}{
				"a_node":     localNodeID,
				"b_node":     remoteNodeID,
				"latency_ms": 0,
				"status":     "STATUS_ACTIVE",
			}
			if err := s.upsertRoute(ctx, routeData); err != nil {
				s.logger.Debugf("Route to node %d may already exist: %v", remoteNodeID, err)
			}

			// Reverse route
			routeData = map[string]interface{}{
				"a_node":     remoteNodeID,
				"b_node":     localNodeID,
				"latency_ms": 0,
				"status":     "STATUS_ACTIVE",
			}
			if err := s.upsertRoute(ctx, routeData); err != nil {
				s.logger.Debugf("Reverse route from node %d may already exist: %v", remoteNodeID, err)
			}
		}
	}

	s.logger.Info("Successfully applied all synced mesh data")
	return nil
}

// AddJoiningNode adds a joining node to the local database
func (s *DatabaseSyncManager) AddJoiningNode(ctx context.Context, nodeID uint64, data map[string]interface{}) error {
	s.logger.Infof("Adding joining node %d to local database (data has %d fields)", nodeID, len(data))

	// Log what data we received
	for k, v := range data {
		if k == "node_public_key" {
			if b, ok := v.([]byte); ok {
				s.logger.Debugf("  %s: []byte (len=%d)", k, len(b))
			} else if str, ok := v.(string); ok {
				s.logger.Debugf("  %s: string (len=%d)", k, len(str))
			} else {
				s.logger.Debugf("  %s: %T", k, v)
			}
		} else {
			s.logger.Debugf("  %s: %v", k, v)
		}
	}

	// Upsert the node
	nodeData := make(map[string]interface{})
	// Use the nodeID parameter (uint64) directly, not from data map where it's float64
	nodeData["node_id"] = int64(nodeID)

	// Extract node information from data, but skip node_id (we already set it correctly)
	for key, value := range data {
		if key != "node_id" {
			nodeData[key] = value
		}
	}

	if err := s.upsertNode(ctx, nodeData); err != nil {
		return fmt.Errorf("failed to upsert joining node: %w", err)
	}

	// Create route to joining node
	localNodeID, err := s.getLocalNodeID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get local node ID: %w", err)
	}

	routeData := map[string]interface{}{
		"a_node":     localNodeID,
		"b_node":     nodeID,
		"latency_ms": 0,
		"status":     "STATUS_ACTIVE",
	}

	if err := s.upsertRoute(ctx, routeData); err != nil {
		s.logger.Warnf("Failed to create route to joining node: %v", err)
	}

	// Create reverse route
	routeData = map[string]interface{}{
		"a_node":     nodeID,
		"b_node":     localNodeID,
		"latency_ms": 0,
		"status":     "STATUS_ACTIVE",
	}

	if err := s.upsertRoute(ctx, routeData); err != nil {
		s.logger.Warnf("Failed to create reverse route: %v", err)
	}

	s.logger.Infof("Successfully added joining node %d", nodeID)
	return nil
}

// Helper methods for upserting individual records

func (s *DatabaseSyncManager) upsertMesh(ctx context.Context, data map[string]interface{}) error {
	query := `
		INSERT INTO mesh (mesh_id, mesh_name, mesh_description, allow_join, status, created, updated)
		VALUES ($1, $2, $3, $4::join_key_enum, $5::status_enum, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (mesh_id) DO UPDATE SET
			mesh_name = EXCLUDED.mesh_name,
			mesh_description = EXCLUDED.mesh_description,
			allow_join = EXCLUDED.allow_join,
			status = EXCLUDED.status,
			updated = CURRENT_TIMESTAMP
	`

	_, err := s.db.Pool().Exec(ctx, query,
		data["mesh_id"],
		data["mesh_name"],
		data["mesh_description"],
		data["allow_join"],
		data["status"],
	)

	if err != nil {
		return fmt.Errorf("failed to upsert mesh: %w", err)
	}

	s.logger.Infof("Upserted mesh: %v", data["mesh_name"])
	return nil
}

func (s *DatabaseSyncManager) upsertNode(ctx context.Context, data map[string]interface{}) error {
	// Note: region_id is intentionally NOT included in this INSERT
	// During mesh sync (Step 1), region_id must be NULL because the regions table
	// hasn't been synced yet. After user data sync (Step 3), region_id can be
	// restored if original_region_id was provided in the sync data.
	query := `
		INSERT INTO nodes (node_id, node_name, node_description, node_public_key, ip_address, port, status, seed_node, region_id, created, updated)
		VALUES ($1, $2, $3, $4, $5::inet, $6, $7::status_enum, $8, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (node_id) DO UPDATE SET
			node_name = EXCLUDED.node_name,
			node_description = EXCLUDED.node_description,
			node_public_key = EXCLUDED.node_public_key,
			ip_address = EXCLUDED.ip_address,
			port = EXCLUDED.port,
			status = EXCLUDED.status,
			seed_node = EXCLUDED.seed_node,
			updated = CURRENT_TIMESTAMP
			-- region_id is NOT updated here - it stays NULL until regions are synced
	`

	seedNode := false
	if val, ok := data["seed_node"].(bool); ok {
		seedNode = val
	}

	// Get node_public_key - it's required
	// Public keys are transmitted as strings (PEM format) to avoid encoding issues
	var nodePublicKey []byte
	if val, ok := data["node_public_key"].(string); ok {
		// Convert string to bytes for database storage
		nodePublicKey = []byte(val)
	} else if val, ok := data["node_public_key"].([]byte); ok {
		// Direct byte array (fallback for compatibility)
		nodePublicKey = val
	} else {
		// If not provided, use empty byte slice (for compatibility)
		s.logger.Warnf("Node public key not found or invalid type for node %v (type: %T)", data["node_id"], data["node_public_key"])
		nodePublicKey = []byte{}
	}

	_, err := s.db.Pool().Exec(ctx, query,
		data["node_id"],
		data["node_name"],
		data["node_description"],
		nodePublicKey,
		data["ip_address"],
		data["port"],
		data["status"],
		seedNode,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert node: %w", err)
	}

	s.logger.Debugf("Upserted node: %v", data["node_id"])
	return nil
}

func (s *DatabaseSyncManager) upsertRoute(ctx context.Context, data map[string]interface{}) error {
	query := `
		INSERT INTO routes (a_node, b_node, latency_ms, status, created, updated)
		VALUES ($1, $2, $3, $4::status_enum, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (a_node, b_node) DO UPDATE SET
			latency_ms = EXCLUDED.latency_ms,
			status = EXCLUDED.status,
			updated = CURRENT_TIMESTAMP
	`

	latencyMs := 0
	if val, ok := data["latency_ms"].(int); ok {
		latencyMs = val
	} else if val, ok := data["latency_ms"].(float64); ok {
		latencyMs = int(val)
	} else if val, ok := data["latency_ms"].(int32); ok {
		latencyMs = int(val)
	}

	routeStatus := "STATUS_ACTIVE"
	if val, ok := data["status"].(string); ok && val != "" {
		routeStatus = val
	}

	_, err := s.db.Pool().Exec(ctx, query,
		data["a_node"],
		data["b_node"],
		latencyMs,
		routeStatus,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert route: %w", err)
	}

	s.logger.Debugf("Upserted route: %v -> %v", data["a_node"], data["b_node"])
	return nil
}

func (s *DatabaseSyncManager) getLocalNodeID(ctx context.Context) (int64, error) {
	var nodeID int64
	query := `SELECT identity_id FROM localidentity LIMIT 1`
	err := s.db.Pool().QueryRow(ctx, query).Scan(&nodeID)
	if err != nil {
		return 0, fmt.Errorf("failed to get local node ID: %w", err)
	}
	return nodeID, nil
}

// === User-Level Table Synchronization ===

// GetUserDataForSync retrieves all user-level table data for synchronization
// Returns data in a single structure to be sent as one message
func (s *DatabaseSyncManager) GetUserDataForSync(ctx context.Context) (map[string]interface{}, error) {
	s.logger.Info("Gathering user-level data for synchronization")

	userData := make(map[string]interface{})

	// Define the order of tables to sync (respecting foreign key constraints)
	// Order matters: parent tables before child tables
	tableOrder := []string{
		"tenants",
		"users",
		"workspaces",
		"regions",
		"environments",
		"instances",
		"databases",
		"repos",
		"branches",
		"commits",
		"mapping_rules",
		"mappings",
		"mapping_rule_mappings",
		"relationships",
	}

	for _, tableName := range tableOrder {
		tableData, err := s.getUserTableData(ctx, tableName)
		if err != nil {
			s.logger.Warnf("Failed to get data for table %s: %v", tableName, err)
			// Continue with other tables even if one fails
			userData[tableName] = []map[string]interface{}{}
			continue
		}
		userData[tableName] = tableData
		s.logger.Debugf("Gathered %d rows from %s", len(tableData), tableName)
	}

	s.logger.Infof("Successfully gathered user-level data from %d tables", len(tableOrder))
	return userData, nil
}

// getUserTableData retrieves all rows from a user-level table
func (s *DatabaseSyncManager) getUserTableData(ctx context.Context, tableName string) ([]map[string]interface{}, error) {
	// Get all columns for the table
	columnsQuery := `
		SELECT column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	colRows, err := s.db.Pool().Query(ctx, columnsQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for %s: %w", tableName, err)
	}
	defer colRows.Close()

	var columns []string
	for colRows.Next() {
		var colName, dataType, udtName string
		if err := colRows.Scan(&colName, &dataType, &udtName); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		columns = append(columns, colName)
	}

	if len(columns) == 0 {
		return []map[string]interface{}{}, nil
	}

	// Query all rows
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY 1", strings.Join(columns, ", "), tableName)
	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table %s: %w", tableName, err)
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			s.logger.Warnf("Failed to scan row from %s: %v", tableName, err)
			continue
		}

		rowData := make(map[string]interface{})
		for i, colName := range columns {
			// Convert int64 values to strings to preserve precision in JSON
			if v, ok := values[i].(int64); ok {
				rowData[colName] = fmt.Sprintf("%d", v)
			} else if v, ok := values[i].(int32); ok {
				rowData[colName] = fmt.Sprintf("%d", v)
			} else {
				rowData[colName] = values[i]
			}
		}
		result = append(result, rowData)
	}

	return result, nil
}

// ApplyUserDataSync applies user-level data to the local database
// This overwrites all user-level data on the joining node
func (s *DatabaseSyncManager) ApplyUserDataSync(ctx context.Context, userData map[string]interface{}) error {
	s.logger.Info("Applying user-level data to local database")

	// Clean up existing user sessions before overwriting user data
	if err := s.cleanupUserSessions(ctx); err != nil {
		s.logger.Warnf("Failed to cleanup user sessions: %v", err)
		// Continue anyway - sessions will eventually timeout
	}

	// Define the order of tables to apply (respecting foreign key constraints)
	// Must match the order in GetUserDataForSync
	tableOrder := []string{
		"tenants",
		"users",
		"workspaces",
		"regions",
		"environments",
		"instances",
		"databases",
		"repos",
		"branches",
		"commits",
		"mapping_rules",
		"mappings",
		"mapping_rule_mappings",
		"relationships",
	}

	// Start a transaction for all user-level data
	s.logger.Info("Beginning transaction for user-level data sync")
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	s.logger.Info("Transaction started successfully")
	defer tx.Rollback(ctx)

	// Set a statement timeout to prevent indefinite waiting on locks
	_, err = tx.Exec(ctx, "SET LOCAL statement_timeout = '30s'")
	if err != nil {
		s.logger.Warnf("Failed to set statement timeout: %v", err)
	} else {
		s.logger.Info("Set statement timeout to 30s for this transaction")
	}

	// Delete existing data from user-level tables only
	// This ensures mesh consistency - all nodes have identical user-level data
	// We DELETE (not TRUNCATE) from specific tables to:
	// 1. Avoid TRUNCATE CASCADE which would wipe system tables (mesh, nodes, routes)
	// 2. Respect FK constraints (instances/databases reference nodes, but deleting them doesn't affect nodes)
	// 3. Allow nodes.region_id to be SET NULL when regions is deleted (per FK definition)
	// Delete in reverse order to respect FK dependencies (leaf tables first)
	s.logger.Info("Deleting existing user-level data (mesh consistency)")

	// Reverse order: delete leaf tables first to avoid FK violations
	for i := len(tableOrder) - 1; i >= 0; i-- {
		tableName := tableOrder[i]
		s.logger.Debugf("Deleting all rows from %s", tableName)
		result, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s", tableName))
		if err != nil {
			s.logger.Errorf("Failed to delete from %s: %v", tableName, err)

			// Check if it's a lock timeout or cancellation
			errStr := err.Error()
			if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "lock") {
				s.logger.Errorf("DELETE on %s failed due to lock/timeout", tableName)
				s.logBlockingQueries(ctx, tableName)
			}

			return fmt.Errorf("failed to delete from %s: %w", tableName, err)
		}
		rowsDeleted := result.RowsAffected()
		s.logger.Debugf("Deleted %d rows from %s", rowsDeleted, tableName)
	}

	s.logger.Info("User-level data cleared, starting to apply synced data")

	// Apply data for each table in forward order
	for _, tableName := range tableOrder {
		s.logger.Debugf("Processing table %s", tableName)
		tableDataInterface, ok := userData[tableName]
		if !ok {
			s.logger.Debugf("No data for table %s", tableName)
			continue
		}

		s.logger.Debugf("Converting data for table %s", tableName)
		// Handle both []interface{} and []map[string]interface{} from JSON
		var tableData []interface{}
		switch v := tableDataInterface.(type) {
		case []interface{}:
			tableData = v
		case []map[string]interface{}:
			// Convert to []interface{}
			for _, item := range v {
				tableData = append(tableData, item)
			}
		default:
			s.logger.Warnf("Invalid data format for table %s (type: %T)", tableName, tableDataInterface)
			continue
		}

		if len(tableData) == 0 {
			s.logger.Debugf("Table %s is empty, skipping", tableName)
			continue
		}

		s.logger.Infof("Applying %d rows to %s", len(tableData), tableName)

		// Debug: Log the data being inserted for instances and databases
		if tableName == "instances" || tableName == "databases" {
			for i, rowInterface := range tableData {
				if row, ok := rowInterface.(map[string]interface{}); ok {
					s.logger.Debugf("  Row %d: connected_to_node_id=%v (type=%T)", i, row["connected_to_node_id"], row["connected_to_node_id"])
				}
			}
		}

		s.logger.Debugf("About to call applyTableData for %s", tableName)
		if err := s.applyTableData(ctx, tx, tableName, tableData); err != nil {
			return fmt.Errorf("failed to apply data for %s: %w", tableName, err)
		}
		s.logger.Debugf("Completed applyTableData for %s", tableName)
	}

	s.logger.Info("All table data applied successfully, committing transaction")
	// Commit the transaction - FK constraints will be checked here
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.logger.Info("Successfully applied all user-level data")
	return nil
}

// cleanupUserSessions removes all active user sessions
func (s *DatabaseSyncManager) cleanupUserSessions(ctx context.Context) error {
	s.logger.Info("Cleaning up user sessions before data sync")

	// Check if user_sessions table exists
	checkQuery := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = 'user_sessions'
		)
	`
	var exists bool
	if err := s.db.Pool().QueryRow(ctx, checkQuery).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check for user_sessions table: %w", err)
	}

	if !exists {
		s.logger.Debug("user_sessions table does not exist, skipping cleanup")
		return nil
	}

	// Delete all sessions
	result, err := s.db.Pool().Exec(ctx, "DELETE FROM user_sessions")
	if err != nil {
		return fmt.Errorf("failed to delete user sessions: %w", err)
	}

	rowsAffected := result.RowsAffected()
	s.logger.Infof("Cleaned up %d user sessions", rowsAffected)
	return nil
}

// applyTableData inserts data into a single table
func (s *DatabaseSyncManager) applyTableData(ctx context.Context, tx pgx.Tx, tableName string, data []interface{}) error {
	s.logger.Debugf("applyTableData: entered for table %s with %d rows", tableName, len(data))
	if len(data) == 0 {
		return nil
	}

	// Get the first row to determine columns
	firstRow, ok := data[0].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid row format for %s", tableName)
	}

	// Build column list
	var columns []string
	for colName := range firstRow {
		columns = append(columns, colName)
	}
	s.logger.Debugf("applyTableData: built column list for %s, %d columns", tableName, len(columns))

	// Get the actual primary key columns from the database
	s.logger.Debugf("applyTableData: getting primary key columns for %s", tableName)
	pkColumns, err := s.getPrimaryKeyColumns(ctx, tx, tableName)
	s.logger.Debugf("applyTableData: got primary key columns for %s: %v (err: %v)", tableName, pkColumns, err)
	if err != nil {
		s.logger.Warnf("Failed to get primary key for %s: %v", tableName, err)
		// Fallback: use simple INSERT without ON CONFLICT
		return s.applyTableDataSimple(ctx, tx, tableName, columns, data)
	}

	// Build INSERT statement with ON CONFLICT DO UPDATE
	var placeholders []string
	var updateClauses []string
	for i, col := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		// Update all columns except primary keys on conflict
		isPK := false
		for _, pkCol := range pkColumns {
			if col == pkCol {
				isPK = true
				break
			}
		}
		if !isPK {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}

	var query string
	if len(pkColumns) > 0 {
		// Build ON CONFLICT clause with actual primary keys
		pkConstraint := strings.Join(pkColumns, ", ")

		if len(updateClauses) > 0 {
			query = fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
				tableName,
				strings.Join(columns, ", "),
				strings.Join(placeholders, ", "),
				pkConstraint,
				strings.Join(updateClauses, ", "),
			)
		} else {
			// All columns are primary keys - just skip on conflict
			query = fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
				tableName,
				strings.Join(columns, ", "),
				strings.Join(placeholders, ", "),
				pkConstraint,
			)
		}
	} else {
		// No primary key - just insert
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)
	}

	// Insert each row
	rowsInserted := 0
	for _, rowInterface := range data {
		row, ok := rowInterface.(map[string]interface{})
		if !ok {
			s.logger.Warnf("Skipping invalid row in %s", tableName)
			continue
		}

		// Build values array in same order as columns
		var values []interface{}
		for _, col := range columns {
			val := row[col]

			// Convert different numeric types to appropriate format
			switch v := val.(type) {
			case string:
				// Try to parse as int64 for ID columns
				if strings.HasSuffix(col, "_id") || strings.HasSuffix(col, "_node") {
					if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
						values = append(values, intVal)
						continue
					}
				}
				values = append(values, v)
			case float64:
				// JSON unmarshaling often produces float64 for numbers
				// Convert to int64 for ID columns
				if strings.HasSuffix(col, "_id") || strings.HasSuffix(col, "_node") {
					values = append(values, int64(v))
				} else {
					values = append(values, v)
				}
			case int:
				values = append(values, int64(v))
			case int64:
				values = append(values, v)
			case nil:
				values = append(values, nil)
			default:
				values = append(values, val)
			}
		}

		// Execute insert
		_, err := tx.Exec(ctx, query, values...)
		if err != nil {
			// Enhanced error logging for debugging foreign key issues
			if tableName == "instances" || tableName == "databases" {
				s.logger.Errorf("Failed to insert row into %s: %v", tableName, err)
				s.logger.Errorf("  Query: %s", query)
				s.logger.Errorf("  Values: %+v", values)
				// Find the connected_to_node_id column and log its value
				for i, col := range columns {
					if col == "connected_to_node_id" && i < len(values) {
						s.logger.Errorf("  connected_to_node_id value: %v (type=%T)", values[i], values[i])
					}
				}
			} else {
				s.logger.Warnf("Failed to insert row into %s: %v", tableName, err)
			}
			continue
		}

		rowsInserted++
	}

	s.logger.Debugf("Inserted/updated %d rows in %s", rowsInserted, tableName)
	return nil
}

// getPrimaryKeyColumns retrieves the primary key column names for a table
func (s *DatabaseSyncManager) getPrimaryKeyColumns(ctx context.Context, tx pgx.Tx, tableName string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		  AND i.indisprimary
		ORDER BY a.attnum
	`

	rows, err := tx.Query(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary keys: %w", err)
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		pkColumns = append(pkColumns, colName)
	}

	return pkColumns, nil
}

// applyTableDataSimple inserts data without ON CONFLICT handling (fallback method)
func (s *DatabaseSyncManager) applyTableDataSimple(ctx context.Context, tx pgx.Tx, tableName string, columns []string, data []interface{}) error {
	// Build simple INSERT statement
	var placeholders []string
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Insert each row
	rowsInserted := 0
	for _, rowInterface := range data {
		row, ok := rowInterface.(map[string]interface{})
		if !ok {
			s.logger.Warnf("Skipping invalid row in %s", tableName)
			continue
		}

		// Build values array in same order as columns
		var values []interface{}
		for _, col := range columns {
			val := row[col]

			// Convert different numeric types to appropriate format
			switch v := val.(type) {
			case string:
				// Try to parse as int64 for ID columns
				if strings.HasSuffix(col, "_id") || strings.HasSuffix(col, "_node") {
					if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
						values = append(values, intVal)
						continue
					}
				}
				values = append(values, v)
			case float64:
				// JSON unmarshaling often produces float64 for numbers
				// Convert to int64 for ID columns
				if strings.HasSuffix(col, "_id") || strings.HasSuffix(col, "_node") {
					values = append(values, int64(v))
				} else {
					values = append(values, v)
				}
			case int:
				values = append(values, int64(v))
			case int64:
				values = append(values, v)
			case nil:
				values = append(values, nil)
			default:
				values = append(values, val)
			}
		}

		// Execute insert
		_, err := tx.Exec(ctx, query, values...)
		if err != nil {
			s.logger.Warnf("Failed to insert row into %s: %v", tableName, err)
			continue
		}

		rowsInserted++
	}

	s.logger.Debugf("Inserted %d rows in %s (simple mode)", rowsInserted, tableName)
	return nil
}

// logBlockingQueries attempts to log information about queries blocking table operations
func (s *DatabaseSyncManager) logBlockingQueries(ctx context.Context, tableName string) {
	// Query to find blocking queries
	query := `
		SELECT 
			pid,
			usename,
			application_name,
			state,
			query,
			state_change,
			NOW() - state_change AS duration
		FROM pg_stat_activity
		WHERE state != 'idle'
		  AND pid != pg_backend_pid()
		ORDER BY state_change
		LIMIT 10
	`

	// Use a separate connection to avoid transaction issues
	rows, err := s.db.Pool().Query(ctx, query)
	if err != nil {
		s.logger.Warnf("Failed to query blocking processes: %v", err)
		return
	}
	defer rows.Close()

	s.logger.Warnf("Active queries that may be blocking TRUNCATE on %s:", tableName)
	count := 0
	for rows.Next() {
		var pid int
		var username, appName, state, query, duration string
		var stateChange time.Time
		if err := rows.Scan(&pid, &username, &appName, &state, &query, &stateChange, &duration); err != nil {
			s.logger.Warnf("Failed to scan row: %v", err)
			continue
		}

		// Truncate long queries
		if len(query) > 100 {
			query = query[:100] + "..."
		}

		s.logger.Warnf("  [PID %d] %s/%s: %s (duration: %s)", pid, username, appName, query, duration)
		count++
	}

	if count == 0 {
		s.logger.Warn("  No active queries found (they may have completed)")
	}
}
