package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/database"
	"github.com/redbco/redb-open/services/core/internal/services/mapping"
	"github.com/redbco/redb-open/services/core/internal/services/relationship"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
)

// StartRelationship initiates a relationship by performing initial data copy and setting up CDC
func (s *Server) StartRelationship(req *corev1.StartRelationshipRequest, stream corev1.RelationshipService_StartRelationshipServer) error {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	ctx := stream.Context()

	// Send initial status
	if err := stream.Send(&corev1.StartRelationshipResponse{
		Message: "Initializing relationship...",
		Success: true,
		Status:  commonv1.Status_STATUS_PENDING,
		Phase:   "initializing",
	}); err != nil {
		return err
	}

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)
	mappingService := mapping.NewService(s.engine.db, s.engine.logger)
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get relationship
	rel, err := relationshipService.GetByName(ctx, req.TenantId, workspaceID, req.RelationshipName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "relationship not found: %v", err)
	}

	// Get mapping rules by mapping ID
	mappingRules, err := mappingService.GetMappingRulesForMappingByID(ctx, req.TenantId, workspaceID, rel.MappingID)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "mapping rules not found: %v", err)
	}

	if len(mappingRules) == 0 {
		s.engine.IncrementErrors()
		return status.Errorf(codes.FailedPrecondition, "mapping has no rules")
	}

	// Get source and target databases
	sourceDB, err := databaseService.GetByID(ctx, rel.SourceDatabaseID)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	targetDB, err := databaseService.GetByID(ctx, rel.TargetDatabaseID)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "target database not found: %v", err)
	}

	s.engine.logger.Infof("Starting relationship '%s': %s -> %s", rel.Name, sourceDB.Name, targetDB.Name)

	// Check if we should skip initial data copy by checking if target table already has data
	// This is more reliable than checking replication sources (which might exist from a previous attempt)
	skipDataCopy := false
	targetRowCount, rowCountErr := s.getTargetTableRowCount(ctx, targetDB, rel.TargetTableName)
	if rowCountErr == nil && targetRowCount > 0 {
		// Target table has data, assume it was already copied
		skipDataCopy = true
		s.engine.logger.Infof("Target table %s already has %d rows, skipping initial data copy", rel.TargetTableName, targetRowCount)
	} else if rowCountErr != nil {
		s.engine.logger.Warnf("Could not check target table row count: %v, will perform initial data copy", rowCountErr)
	}

	var totalRows int64

	if !skipDataCopy {
		// Update relationship status to active/starting
		if _, err := relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, rel.Name, map[string]interface{}{
			"status":         "STATUS_ACTIVE",
			"status_message": "Starting initial data copy",
		}); err != nil {
			s.engine.logger.Warnf("Failed to update relationship status: %v", err)
		}

		// Phase 1: Copy initial data using the mapping
		if err := stream.Send(&corev1.StartRelationshipResponse{
			Message: "Starting initial data copy...",
			Success: true,
			Status:  commonv1.Status_STATUS_PENDING,
			Phase:   "copying_data",
		}); err != nil {
			return err
		}

		batchSize := int32(1000)
		if req.BatchSize != nil && *req.BatchSize > 0 {
			batchSize = *req.BatchSize
		}

		// Perform initial data copy
		var err error
		totalRows, err = s.performInitialDataCopy(ctx, stream, mappingRules, sourceDB, targetDB, batchSize)
		if err != nil {
			s.engine.IncrementErrors()
			// Update relationship status to error (truncate message to fit DB limit)
			errMsg := fmt.Sprintf("Initial data copy failed: %v", err)
			if len(errMsg) > 250 {
				errMsg = errMsg[:250] + "..."
			}
			relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, rel.Name, map[string]interface{}{
				"status":         "STATUS_ERROR",
				"status_message": errMsg,
			})
			return status.Errorf(codes.Internal, "failed to copy initial data: %v", err)
		}

		s.engine.logger.Infof("Initial data copy completed: %d rows copied", totalRows)
	} else {
		// Skipping data copy, just update status
		if _, err := relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, rel.Name, map[string]interface{}{
			"status":         "STATUS_ACTIVE",
			"status_message": "Resuming CDC replication",
		}); err != nil {
			s.engine.logger.Warnf("Failed to update relationship status: %v", err)
		}

		if err := stream.Send(&corev1.StartRelationshipResponse{
			Message: "Resuming relationship (data already synced)...",
			Success: true,
			Status:  commonv1.Status_STATUS_PENDING,
			Phase:   "resuming_cdc",
		}); err != nil {
			return err
		}

		totalRows = 0 // Unknown, but data already exists
	}

	// Phase 2: Set up CDC replication
	if err := stream.Send(&corev1.StartRelationshipResponse{
		Message:            fmt.Sprintf("Initial data copy completed (%d rows). Setting up CDC...", totalRows),
		Success:            true,
		Status:             commonv1.Status_STATUS_PENDING,
		Phase:              "setting_up_cdc",
		RowsCopied:         totalRows,
		TotalRows:          totalRows,
		ProgressPercentage: 100,
	}); err != nil {
		return err
	}

	// Setup CDC replication via Anchor service
	cdcStatus, err := s.setupCDCReplication(ctx, rel, sourceDB, targetDB, mappingRules)
	if err != nil {
		s.engine.IncrementErrors()
		// Update relationship status to error (truncate message to fit DB limit)
		errMsg := fmt.Sprintf("CDC setup failed: %v", err)
		if len(errMsg) > 250 {
			errMsg = errMsg[:250] + "..."
		}
		relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, rel.Name, map[string]interface{}{
			"status":         "STATUS_ERROR",
			"status_message": errMsg,
		})
		return status.Errorf(codes.Internal, "failed to setup CDC: %v", err)
	}

	// Update relationship status to active
	if _, err := relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, rel.Name, map[string]interface{}{
		"status":         "STATUS_ACTIVE",
		"status_message": "Relationship active, CDC replication running",
	}); err != nil {
		s.engine.logger.Warnf("Failed to update relationship status: %v", err)
	}

	// Send final success status
	if err := stream.Send(&corev1.StartRelationshipResponse{
		Message:            "Relationship activated successfully. CDC replication is now running.",
		Success:            true,
		Status:             commonv1.Status_STATUS_SUCCESS,
		Phase:              "active",
		RowsCopied:         totalRows,
		TotalRows:          totalRows,
		CdcStatus:          cdcStatus,
		ProgressPercentage: 100,
	}); err != nil {
		return err
	}

	s.engine.logger.Infof("Relationship '%s' started successfully", rel.Name)
	return nil
}

// StopRelationship pauses a relationship without removing it
func (s *Server) StopRelationship(ctx context.Context, req *corev1.StopRelationshipRequest) (*corev1.StopRelationshipResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get relationship
	rel, err := relationshipService.GetByName(ctx, req.TenantId, workspaceID, req.RelationshipName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "relationship not found: %v", err)
	}

	s.engine.logger.Infof("Stopping relationship '%s'", rel.Name)

	// Get replication sources for this relationship
	replicationSources, err := s.getReplicationSourcesForRelationship(ctx, rel.ID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get replication sources: %v", err)
	}

	// Stop CDC replication via Anchor service
	anchorClient, err := s.getAnchorClient()
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service: %v", err)
	}

	for _, source := range replicationSources {
		stopReq := &anchorv1.StopCDCReplicationRequest{
			TenantId:            req.TenantId,
			WorkspaceId:         workspaceID,
			ReplicationSourceId: source.ReplicationSourceID,
			PreserveState:       &[]bool{true}[0], // Preserve state for potential resume
		}

		_, err := anchorClient.StopCDCReplication(ctx, stopReq)
		if err != nil {
			s.engine.logger.Errorf("Failed to stop CDC for source %s: %v", source.ReplicationSourceID, err)
			// Continue trying to stop other sources
		}
	}

	// Update relationship status to stopped
	if _, err := relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, rel.Name, map[string]interface{}{
		"status":         "STATUS_STOPPED",
		"status_message": "Relationship stopped, CDC replication paused",
	}); err != nil {
		s.engine.logger.Warnf("Failed to update relationship status: %v", err)
	}

	return &corev1.StopRelationshipResponse{
		Message: fmt.Sprintf("Relationship '%s' stopped successfully", rel.Name),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ResumeRelationship restarts a stopped relationship
func (s *Server) ResumeRelationship(req *corev1.ResumeRelationshipRequest, stream corev1.RelationshipService_ResumeRelationshipServer) error {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	ctx := stream.Context()

	// Send initial status
	if err := stream.Send(&corev1.ResumeRelationshipResponse{
		Message: "Resuming relationship...",
		Success: true,
		Status:  commonv1.Status_STATUS_PENDING,
		Phase:   "resuming",
	}); err != nil {
		return err
	}

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get relationship
	rel, err := relationshipService.GetByName(ctx, req.TenantId, workspaceID, req.RelationshipName)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.NotFound, "relationship not found: %v", err)
	}

	s.engine.logger.Infof("Resuming relationship '%s'", rel.Name)

	// Get replication sources for this relationship
	replicationSources, err := s.getReplicationSourcesForRelationship(ctx, rel.ID)
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.Internal, "failed to get replication sources: %v", err)
	}

	// Resume CDC replication via Anchor service
	anchorClient, err := s.getAnchorClient()
	if err != nil {
		s.engine.IncrementErrors()
		return status.Errorf(codes.Internal, "failed to connect to anchor service: %v", err)
	}

	for _, source := range replicationSources {
		// Get the saved CDC state
		resumeState := make(map[string]string)
		if source.CDCState != "" {
			if err := json.Unmarshal([]byte(source.CDCState), &resumeState); err != nil {
				s.engine.logger.Warnf("Failed to parse CDC state: %v", err)
			}
		}

		resumeReq := &anchorv1.ResumeCDCReplicationRequest{
			TenantId:            req.TenantId,
			WorkspaceId:         workspaceID,
			ReplicationSourceId: source.ReplicationSourceID,
			ResumeState:         resumeState,
		}

		_, err := anchorClient.ResumeCDCReplication(ctx, resumeReq)
		if err != nil {
			s.engine.logger.Errorf("Failed to resume CDC for source %s: %v", source.ReplicationSourceID, err)
			return status.Errorf(codes.Internal, "failed to resume CDC: %v", err)
		}
	}

	// Update relationship status to active
	if _, err := relationshipService.UpdateByName(ctx, req.TenantId, workspaceID, rel.Name, map[string]interface{}{
		"status":         "STATUS_ACTIVE",
		"status_message": "Relationship resumed, CDC replication running",
	}); err != nil {
		s.engine.logger.Warnf("Failed to update relationship status: %v", err)
	}

	// Send final success status
	if err := stream.Send(&corev1.ResumeRelationshipResponse{
		Message:   fmt.Sprintf("Relationship '%s' resumed successfully", rel.Name),
		Success:   true,
		Status:    commonv1.Status_STATUS_SUCCESS,
		Phase:     "active",
		CdcStatus: "active",
	}); err != nil {
		return err
	}

	s.engine.logger.Infof("Relationship '%s' resumed successfully", rel.Name)
	return nil
}

// RemoveRelationship stops and completely removes a relationship
func (s *Server) RemoveRelationship(ctx context.Context, req *corev1.RemoveRelationshipRequest) (*corev1.RemoveRelationshipResponse, error) {
	s.engine.TrackOperation()
	defer s.engine.UntrackOperation()
	s.engine.IncrementRequestsProcessed()

	// Get services
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	relationshipService := relationship.NewService(s.engine.db, s.engine.logger)

	// Get workspace ID
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "workspace not found: %v", err)
	}

	// Get relationship
	rel, err := relationshipService.GetByName(ctx, req.TenantId, workspaceID, req.RelationshipName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "relationship not found: %v", err)
	}

	s.engine.logger.Infof("Removing relationship '%s'", rel.Name)

	// Get replication sources for this relationship
	replicationSources, err := s.getReplicationSourcesForRelationship(ctx, rel.ID)
	if err != nil && (req.Force == nil || !*req.Force) {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get replication sources: %v", err)
	}

	// Stop and remove CDC replication via Anchor service
	anchorClient, err := s.getAnchorClient()
	if err != nil && (req.Force == nil || !*req.Force) {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to connect to anchor service: %v", err)
	}

	if anchorClient != nil {
		for _, source := range replicationSources {
			// First stop the CDC
			stopReq := &anchorv1.StopCDCReplicationRequest{
				TenantId:            req.TenantId,
				WorkspaceId:         workspaceID,
				ReplicationSourceId: source.ReplicationSourceID,
				PreserveState:       &[]bool{false}[0], // Don't preserve state
			}

			_, err := anchorClient.StopCDCReplication(ctx, stopReq)
			if err != nil {
				s.engine.logger.Errorf("Failed to stop CDC for source %s: %v", source.ReplicationSourceID, err)
				if req.Force == nil || !*req.Force {
					return nil, status.Errorf(codes.Internal, "failed to stop CDC: %v", err)
				}
			}

			// Then remove the replication source
			removeReq := &anchorv1.RemoveReplicationSourceRequest{
				TenantId:            req.TenantId,
				WorkspaceId:         workspaceID,
				DatabaseId:          source.DatabaseID,
				ReplicationSourceId: source.ReplicationSourceID,
			}

			_, err = anchorClient.RemoveReplicationSource(ctx, removeReq)
			if err != nil {
				s.engine.logger.Errorf("Failed to remove replication source %s: %v", source.ReplicationSourceID, err)
				if req.Force == nil || !*req.Force {
					return nil, status.Errorf(codes.Internal, "failed to remove replication source: %v", err)
				}
			}
		}
	}

	// Delete relationship from database (cascades to replication_sources)
	if err := relationshipService.DeleteByName(ctx, req.TenantId, workspaceID, rel.Name); err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete relationship: %v", err)
	}

	return &corev1.RemoveRelationshipResponse{
		Message: fmt.Sprintf("Relationship '%s' removed successfully", rel.Name),
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// Helper functions

// performInitialDataCopy copies all data from source to target using the mapping
func (s *Server) performInitialDataCopy(ctx context.Context, stream corev1.RelationshipService_StartRelationshipServer, mappingRules []*mapping.Rule, sourceDB, targetDB *database.Database, batchSize int32) (int64, error) {
	if len(mappingRules) == 0 {
		return 0, fmt.Errorf("mapping has no rules")
	}

	// Build table pairs from mapping rules (similar to copy-data)
	tablePairs := s.groupMappingRulesByTables(mappingRules)

	var totalRowsCopied int64

	// Copy data for each table pair
	for _, tablePair := range tablePairs {
		rowsCopied, err := s.copyTableData(ctx, tablePair, batchSize)
		if err != nil {
			return totalRowsCopied, fmt.Errorf("failed to copy table %s: %v", tablePair.SourceTable, err)
		}

		totalRowsCopied += rowsCopied

		// Send progress update
		if err := stream.Send(&corev1.StartRelationshipResponse{
			Message:      fmt.Sprintf("Copied %d rows from %s", rowsCopied, tablePair.SourceTable),
			Success:      true,
			Status:       commonv1.Status_STATUS_PENDING,
			Phase:        "copying_data",
			RowsCopied:   totalRowsCopied,
			CurrentTable: tablePair.SourceTable,
		}); err != nil {
			s.engine.logger.Warnf("Failed to send progress update: %v", err)
		}
	}

	return totalRowsCopied, nil
}

// setupCDCReplication sets up CDC replication for the relationship
func (s *Server) setupCDCReplication(ctx context.Context, rel *relationship.Relationship, sourceDB, targetDB *database.Database, mappingRules []*mapping.Rule) (string, error) {
	// Extract table names from mapping rules
	tableNames := make([]string, 0)
	tableNameMap := make(map[string]bool)
	for _, rule := range mappingRules {
		// Extract source identifier from metadata
		sourceIdentifier, ok := rule.Metadata["source_identifier"].(string)
		if !ok || sourceIdentifier == "" {
			continue
		}

		// Parse source identifier to get table name
		sourceInfo, err := s.parseDatabaseIdentifier(sourceIdentifier)
		if err != nil {
			continue
		}
		if !tableNameMap[sourceInfo.TableName] {
			tableNames = append(tableNames, sourceInfo.TableName)
			tableNameMap[sourceInfo.TableName] = true
		}
	}

	if len(tableNames) == 0 {
		return "", fmt.Errorf("no tables found in mapping rules")
	}

	// Connect to Anchor service
	anchorClient, err := s.getAnchorClient()
	if err != nil {
		return "", fmt.Errorf("failed to connect to anchor service: %v", err)
	}

	// Create replication source in database first
	replicationSourceID, err := s.createReplicationSourceRecord(ctx, rel, sourceDB, targetDB, tableNames[0], mappingRules)
	if err != nil {
		return "", fmt.Errorf("failed to create replication source record: %v", err)
	}

	// Prepare mapping rules as JSON
	mappingRulesJSON, err := json.Marshal(mappingRules)
	if err != nil {
		return "", fmt.Errorf("failed to marshal mapping rules: %v", err)
	}

	// Start CDC replication via Anchor
	startCDCReq := &anchorv1.StartCDCReplicationRequest{
		TenantId:            rel.TenantID,
		WorkspaceId:         rel.WorkspaceID,
		SourceDatabaseId:    sourceDB.ID,
		TargetDatabaseId:    targetDB.ID,
		RelationshipId:      rel.ID,
		ReplicationSourceId: replicationSourceID,
		TableNames:          tableNames,
		MappingRules:        mappingRulesJSON,
	}

	cdcResp, err := anchorClient.StartCDCReplication(ctx, startCDCReq)
	if err != nil {
		return "", fmt.Errorf("failed to start CDC replication: %v", err)
	}

	if !cdcResp.Success {
		return "", fmt.Errorf("CDC replication failed: %s", cdcResp.Message)
	}

	// Update replication source with CDC details
	if err := s.updateReplicationSourceCDCDetails(ctx, replicationSourceID, cdcResp.CdcConnectionId, cdcResp.CdcDetails); err != nil {
		s.engine.logger.Warnf("Failed to update replication source CDC details: %v", err)
	}

	return "active", nil
}

// createReplicationSourceRecord creates a replication source record in the database
func (s *Server) createReplicationSourceRecord(ctx context.Context, rel *relationship.Relationship, sourceDB, targetDB *database.Database, tableName string, mappingRules []*mapping.Rule) (string, error) {
	// Check if replication source already exists for this database/table/relationship
	checkQuery := `
		SELECT replication_source_id 
		FROM replication_sources 
		WHERE workspace_id = $1 AND database_id = $2 AND table_name = $3 AND relationship_id = $4
	`

	var existingID string
	err := s.engine.db.Pool().QueryRow(ctx, checkQuery, rel.WorkspaceID, sourceDB.ID, tableName, rel.ID).Scan(&existingID)
	if err == nil {
		// Replication source already exists, return its ID
		s.engine.logger.Infof("Replication source already exists for relationship %s: %s", rel.ID, existingID)
		return existingID, nil
	}

	// Marshal mapping rules to JSON
	mappingRulesJSON, err := json.Marshal(mappingRules)
	if err != nil {
		return "", err
	}

	// Generate slot and publication names
	slotName := fmt.Sprintf("redb_rel_%s", rel.ID[:8])
	publicationName := fmt.Sprintf("redb_pub_%s", rel.ID[:8])

	query := `
		INSERT INTO replication_sources (
			tenant_id, workspace_id, database_id, table_name, relationship_id,
			publication_name, slot_name, target_database_id, target_table_name,
			mapping_rules, status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING replication_source_id
	`

	var replicationSourceID string
	err = s.engine.db.Pool().QueryRow(ctx, query,
		rel.TenantID,
		rel.WorkspaceID,
		sourceDB.ID,
		tableName,
		rel.ID,
		publicationName,
		slotName,
		targetDB.ID,
		rel.TargetTableName,
		string(mappingRulesJSON),
		"STATUS_PENDING",
	).Scan(&replicationSourceID)

	if err != nil {
		return "", fmt.Errorf("failed to create replication source record: %v", err)
	}

	s.engine.logger.Infof("Created new replication source for relationship %s: %s", rel.ID, replicationSourceID)
	return replicationSourceID, nil
}

// updateReplicationSourceCDCDetails updates replication source with CDC connection details
func (s *Server) updateReplicationSourceCDCDetails(ctx context.Context, replicationSourceID, cdcConnectionID string, cdcDetails map[string]string) error {
	cdcDetailsJSON, err := json.Marshal(cdcDetails)
	if err != nil {
		return err
	}

	query := `
		UPDATE replication_sources
		SET cdc_connection_id = $1,
		    cdc_state = $2,
		    status = $3,
		    status_message = $4,
		    updated = CURRENT_TIMESTAMP
		WHERE replication_source_id = $5
	`

	_, err = s.engine.db.Pool().Exec(ctx, query,
		cdcConnectionID,
		string(cdcDetailsJSON),
		"STATUS_ACTIVE",
		"CDC replication active",
		replicationSourceID,
	)

	return err
}

// ReplicationSourceInfo holds information about a replication source
type ReplicationSourceInfo struct {
	ReplicationSourceID string
	DatabaseID          string
	TableName           string
	CDCState            string
	CDCConnectionID     string
}

// getReplicationSourcesForRelationship retrieves all replication sources for a relationship
func (s *Server) getReplicationSourcesForRelationship(ctx context.Context, relationshipID string) ([]*ReplicationSourceInfo, error) {
	query := `
		SELECT replication_source_id, database_id, table_name, 
		       COALESCE(cdc_state::text, '{}'), COALESCE(cdc_connection_id, '')
		FROM replication_sources
		WHERE relationship_id = $1
	`

	rows, err := s.engine.db.Pool().Query(ctx, query, relationshipID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []*ReplicationSourceInfo
	for rows.Next() {
		var source ReplicationSourceInfo
		if err := rows.Scan(&source.ReplicationSourceID, &source.DatabaseID, &source.TableName, &source.CDCState, &source.CDCConnectionID); err != nil {
			return nil, err
		}
		sources = append(sources, &source)
	}

	return sources, rows.Err()
}

// getTargetTableRowCount gets the row count for the target table
func (s *Server) getTargetTableRowCount(ctx context.Context, db *database.Database, tableName string) (int64, error) {
	// Call Anchor service to get row count
	countReq := &anchorv1.GetTableRowCountRequest{
		TenantId:   db.TenantID,
		DatabaseId: db.ID,
		TableName:  tableName,
	}

	countResp, err := s.engine.anchorClient.GetTableRowCount(ctx, countReq)
	if err != nil {
		return 0, err
	}

	return countResp.RowCount, nil
}
