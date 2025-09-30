package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	unifiedmodelv1 "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/services/core/internal/services/commit"
	"github.com/redbco/redb-open/services/core/internal/services/database"
	"github.com/redbco/redb-open/services/core/internal/services/instance"
	"github.com/redbco/redb-open/services/core/internal/services/repo"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// CloneDatabase clones current schema (and optionally data) from source database
func (s *Server) CloneDatabase(ctx context.Context, req *corev1.CloneDatabaseRequest) (*corev1.CloneDatabaseResponse, error) {
	defer s.trackOperation()()

	s.engine.logger.Infof("Starting database clone: source=%s", req.SourceDatabaseName)

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Step 1: Get source database and its current schema
	sourceDB, err := databaseService.Get(ctx, req.TenantId, workspaceID, req.SourceDatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source database not found: %v", err)
	}

	if sourceDB.Status != "STATUS_CONNECTED" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.FailedPrecondition, "source database is not connected")
	}

	// Get current schema from databases table (not commits)
	currentSchema, err := databaseService.GetDatabaseSchema(ctx, sourceDB.ID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get source database schema: %v", err)
	}

	if currentSchema == "" {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.FailedPrecondition, "source database has no schema stored")
	}

	// Step 2: Handle target database creation or validation
	var targetDB *database.Database
	var targetDatabaseID string

	switch target := req.Target.(type) {
	case *corev1.CloneDatabaseRequest_NewDatabase:
		// Create new database on specified instance
		targetDB, targetDatabaseID, err = s.createNewDatabaseForClone(ctx, req.TenantId, workspaceID, req.WorkspaceName, target.NewDatabase)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, err
		}

	case *corev1.CloneDatabaseRequest_ExistingDatabase:
		// Use existing database
		targetDB, err = databaseService.Get(ctx, req.TenantId, workspaceID, target.ExistingDatabase.DatabaseName)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.NotFound, "target database not found: %v", err)
		}
		targetDatabaseID = targetDB.ID

		if targetDB.Status != "STATUS_CONNECTED" {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.FailedPrecondition, "target database is not connected")
		}

	default:
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "target must be specified")
	}

	// Step 3: Convert schema if cross-database type
	deploySchema := currentSchema
	var warnings []string

	if sourceDB.Type != targetDB.Type {
		s.engine.logger.Infof("Converting schema from %s to %s", sourceDB.Type, targetDB.Type)

		convertedSchema, convertWarnings, err := s.convertSchemaViaUnifiedModel(ctx, currentSchema, sourceDB.Type, targetDB.Type)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert schema: %v", err)
		}

		deploySchema = convertedSchema
		warnings = append(warnings, convertWarnings...)
	}

	// Step 4: Deploy schema to target database
	err = s.deploySchemaToDatabase(ctx, targetDatabaseID, deploySchema, req.Options)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to deploy schema: %v", err)
	}

	// Step 5: Copy data if requested
	var rowsCopied int64 = 0
	if req.Options != nil && req.Options.WithData {
		s.engine.logger.Infof("Copying data from source to target database")

		rowsCopied, err = s.copyDatabaseData(ctx, sourceDB, targetDB, req.Options)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to copy data: %v", err)
		}
	}

	// Step 6: Wait for anchor to discover schema and create repo/commit
	repoID, branchID, commitID, err := s.waitForAnchorDiscovery(ctx, targetDatabaseID, 60*time.Second)
	if err != nil {
		s.engine.logger.Warnf("Failed to wait for anchor discovery: %v", err)
		warnings = append(warnings, fmt.Sprintf("Schema deployed but anchor discovery failed: %v", err))
	}

	return &corev1.CloneDatabaseResponse{
		Message:          "Database cloned successfully",
		Success:          true,
		Status:           commonv1.Status_STATUS_SUCCESS,
		TargetDatabaseId: targetDatabaseID,
		TargetRepoId:     repoID,
		TargetBranchId:   branchID,
		TargetCommitId:   commitID,
		Warnings:         warnings,
		RowsCopied:       rowsCopied,
	}, nil
}

// createNewDatabaseForClone creates a new database for cloning operations
func (s *Server) createNewDatabaseForClone(ctx context.Context, tenantID, workspaceID, workspaceName string, target *corev1.NewDatabaseTarget) (*database.Database, string, error) {
	// Get instance service
	instanceService := instance.NewService(s.engine.db, s.engine.logger)

	// Get target instance
	instanceObj, err := instanceService.Get(ctx, tenantID, workspaceName, target.InstanceName)
	if err != nil {
		return nil, "", status.Errorf(codes.NotFound, "target instance not found: %v", err)
	}

	if instanceObj.Status != "STATUS_CONNECTED" {
		return nil, "", status.Errorf(codes.FailedPrecondition, "target instance is not connected")
	}

	// Get database service
	databaseService := database.NewService(s.engine.db, s.engine.logger)

	// Create database object in core
	databaseObj, err := databaseService.Create(
		ctx,
		tenantID,
		workspaceID,
		target.DatabaseName,
		fmt.Sprintf("Cloned database: %s", target.DatabaseName),
		instanceObj.Type,
		instanceObj.Vendor,
		instanceObj.Username,
		"",                  // Password is inherited from instance, don't double-encrypt
		target.DatabaseName, // Use database name as db_name
		&instanceObj.ConnectedToNodeID,
		true, // enabled
		func() string {
			if instanceObj.EnvironmentID != nil {
				return *instanceObj.EnvironmentID
			}
			return ""
		}(),
		instanceObj.ID,
		instanceObj.OwnerID,
	)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "failed to create database object: %v", err)
	}

	// Update the database record with the encrypted password from the instance
	// This ensures the database_password column is populated correctly
	if instanceObj.Password != "" {
		updates := map[string]interface{}{
			"database_password": instanceObj.Password,
		}
		_, err = databaseService.Update(ctx, tenantID, workspaceID, target.DatabaseName, updates)
		if err != nil {
			s.engine.logger.Warnf("Failed to update database password: %v", err)
			// Don't fail the operation, just log the warning
		}
	}

	// Connect to anchor service to create the logical database
	anchorAddr := s.engine.getServiceAddress("anchor")
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "failed to connect to anchor service: %v", err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// Create the logical database via anchor
	createDBReq := &anchorv1.CreateDatabaseRequest{
		TenantId:     tenantID,
		WorkspaceId:  workspaceID,
		InstanceId:   instanceObj.ID,
		DatabaseName: target.DatabaseName,
		Options:      []byte("{}"), // Empty options for now
	}

	createDBResp, err := anchorClient.CreateDatabase(ctx, createDBReq)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "failed to create logical database: %v", err)
	}

	if !createDBResp.Success {
		return nil, "", status.Errorf(codes.Internal, "anchor failed to create database: %s", createDBResp.Message)
	}

	// Connect the database via anchor
	connectDBReq := &anchorv1.ConnectDatabaseRequest{
		TenantId:    tenantID,
		WorkspaceId: workspaceID,
		DatabaseId:  databaseObj.ID,
	}

	connectDBResp, err := anchorClient.ConnectDatabase(ctx, connectDBReq)
	if err != nil {
		return nil, "", status.Errorf(codes.Internal, "failed to connect database via anchor: %v", err)
	}

	if !connectDBResp.Success {
		return nil, "", status.Errorf(codes.Internal, "anchor failed to connect database: %s", connectDBResp.Message)
	}

	return databaseObj, databaseObj.ID, nil
}

// convertSchemaViaUnifiedModel converts schema between database types using the unifiedmodel service
func (s *Server) convertSchemaViaUnifiedModel(ctx context.Context, sourceSchema, sourceType, targetType string) (string, []string, error) {
	// Connect to unifiedmodel service
	umAddr := s.engine.getServiceAddress("unifiedmodel")
	umConn, err := grpc.Dial(umAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", nil, fmt.Errorf("failed to connect to unifiedmodel service: %w", err)
	}
	defer umConn.Close()

	umClient := unifiedmodelv1.NewUnifiedModelServiceClient(umConn)

	// Parse source schema as UnifiedModel
	var sourceUnifiedModel unifiedmodelv1.UnifiedModel
	err = json.Unmarshal([]byte(sourceSchema), &sourceUnifiedModel)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse source schema: %w", err)
	}

	// Create translation request
	translateReq := &unifiedmodelv1.TranslationRequest{
		SourceType:      sourceType,
		TargetType:      targetType,
		SourceStructure: &sourceUnifiedModel,
	}

	translateResp, err := umClient.Translate(ctx, translateReq)
	if err != nil {
		return "", nil, fmt.Errorf("failed to translate schema: %w", err)
	}

	// Note: UnifiedModel service doesn't have Success field, check for errors differently
	if translateResp.TargetStructure == nil {
		return "", nil, fmt.Errorf("schema translation failed: no target structure returned")
	}

	// Convert result back to JSON
	convertedSchemaBytes, err := json.Marshal(translateResp.TargetStructure)
	if err != nil {
		return "", nil, fmt.Errorf("failed to serialize converted schema: %w", err)
	}

	return string(convertedSchemaBytes), translateResp.Warnings, nil
}

// deploySchemaToDatabase deploys schema to target database via anchor service
func (s *Server) deploySchemaToDatabase(ctx context.Context, databaseID, schema string, options *corev1.CloneOptions) error {
	// Connect to anchor service
	anchorAddr := s.engine.getServiceAddress("anchor")
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to anchor service: %w", err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// Handle wipe option
	if options != nil && options.Wipe {
		s.engine.logger.Infof("Wiping target database before deployment")

		wipeReq := &anchorv1.WipeDatabaseRequest{
			DatabaseId: databaseID,
		}

		wipeResp, err := anchorClient.WipeDatabase(ctx, wipeReq)
		if err != nil {
			return fmt.Errorf("failed to wipe database: %w", err)
		}

		if !wipeResp.Success {
			return fmt.Errorf("failed to wipe database: %s", wipeResp.Message)
		}
	}

	// Deploy schema
	deployReq := &anchorv1.DeployDatabaseSchemaRequest{
		DatabaseId: databaseID,
		Schema:     []byte(schema),
	}

	deployResp, err := anchorClient.DeployDatabaseSchema(ctx, deployReq)
	if err != nil {
		return fmt.Errorf("failed to deploy schema: %w", err)
	}

	if !deployResp.Success {
		return fmt.Errorf("schema deployment failed: %s", deployResp.Message)
	}

	return nil
}

// copyDatabaseData copies data from source to target database
func (s *Server) copyDatabaseData(ctx context.Context, sourceDB, targetDB *database.Database, options *corev1.CloneOptions) (int64, error) {
	// Connect to anchor service
	anchorAddr := s.engine.getServiceAddress("anchor")
	anchorConn, err := grpc.Dial(anchorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, fmt.Errorf("failed to connect to anchor service: %w", err)
	}
	defer anchorConn.Close()

	anchorClient := anchorv1.NewAnchorServiceClient(anchorConn)

	// Get table list from source database
	tablesData, err := s.engine.db.Pool().Query(ctx,
		"SELECT database_tables FROM databases WHERE database_id = $1", sourceDB.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to get source tables: %w", err)
	}
	defer tablesData.Close()

	var tablesJSON string
	if tablesData.Next() {
		err = tablesData.Scan(&tablesJSON)
		if err != nil {
			return 0, fmt.Errorf("failed to scan tables data: %w", err)
		}
	}

	if tablesJSON == "" {
		s.engine.logger.Infof("No tables found in source database, skipping data copy")
		return 0, nil
	}

	// Parse tables JSON to get table names
	var tables map[string]interface{}
	err = json.Unmarshal([]byte(tablesJSON), &tables)
	if err != nil {
		return 0, fmt.Errorf("failed to parse tables JSON: %w", err)
	}

	var totalRowsCopied int64 = 0

	// Copy data for each table
	for tableName := range tables {
		s.engine.logger.Infof("Copying data for table: %s", tableName)

		// Fetch data from source
		fetchReq := &anchorv1.FetchDataRequest{
			DatabaseId: sourceDB.ID,
			TableName:  tableName,
			Options:    []byte("{}"),
		}

		fetchResp, err := anchorClient.FetchData(ctx, fetchReq)
		if err != nil {
			s.engine.logger.Warnf("Failed to fetch data for table %s: %v", tableName, err)
			continue
		}

		if !fetchResp.Success {
			s.engine.logger.Warnf("Failed to fetch data for table %s: %s", tableName, fetchResp.Message)
			continue
		}

		// Insert data into target (anchor will handle any necessary transformations)
		insertReq := &anchorv1.InsertDataRequest{
			DatabaseId: targetDB.ID,
			TableName:  tableName,
			Data:       fetchResp.Data,
		}

		insertResp, err := anchorClient.InsertData(ctx, insertReq)
		if err != nil {
			s.engine.logger.Warnf("Failed to insert data for table %s: %v", tableName, err)
			continue
		}

		if !insertResp.Success {
			s.engine.logger.Warnf("Failed to insert data for table %s: %s", tableName, insertResp.Message)
			continue
		}

		totalRowsCopied += insertResp.RowsAffected
		s.engine.logger.Infof("Copied %d rows for table %s", insertResp.RowsAffected, tableName)
	}

	return totalRowsCopied, nil
}

// waitForAnchorDiscovery waits for anchor service to discover the deployed schema and create repo/commit
func (s *Server) waitForAnchorDiscovery(ctx context.Context, databaseID string, timeout time.Duration) (string, string, string, error) {
	deadline := time.Now().Add(timeout)

	// Get services
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	for time.Now().Before(deadline) {
		// Check if repo and branch exist for this database
		repoAndBranch, err := repoService.FindRepoAndBranchByDatabaseID(ctx, databaseID)
		if err == nil && repoAndBranch != nil && repoAndBranch.Success {
			// Get the database to find tenant and workspace IDs
			databaseService := database.NewService(s.engine.db, s.engine.logger)
			db, err := databaseService.GetByID(ctx, databaseID)
			if err == nil {
				// Check if there are any commits in the branch
				commits, err := commitService.List(ctx, db.TenantID, db.WorkspaceID, repoAndBranch.RepoID, repoAndBranch.BranchID)
				if err == nil && len(commits) > 0 {
					// Found repo, branch, and at least one commit
					return repoAndBranch.RepoID, repoAndBranch.BranchID, fmt.Sprintf("%d", commits[0].ID), nil
				}
			}
		}

		// Wait a bit before checking again
		select {
		case <-ctx.Done():
			return "", "", "", ctx.Err()
		case <-time.After(2 * time.Second):
			// Continue checking
		}
	}

	return "", "", "", fmt.Errorf("timeout waiting for anchor discovery")
}
