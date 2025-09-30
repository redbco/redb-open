package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	anchorv1 "github.com/redbco/redb-open/api/proto/anchor/v1"
	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/branch"
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

// DeployCommitSchema deploys schema from a historical commit to a target database
func (s *Server) DeployCommitSchema(ctx context.Context, req *corev1.DeployCommitSchemaRequest) (*corev1.DeployCommitSchemaResponse, error) {
	defer s.trackOperation()()

	s.engine.logger.Infof("Starting commit schema deployment: repo=%s, branch=%s, commit=%s",
		req.RepoName, req.BranchName, req.CommitCode)

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Step 1: Get source commit and its schema from commits table
	sourceCommit, sourceDBType, err := s.getCommitSchema(ctx, req.TenantId, workspaceID, req.RepoName, req.BranchName, req.CommitCode)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "failed to get source commit: %v", err)
	}

	// Step 2: Handle target database creation or validation
	var targetDB *database.Database
	var targetDatabaseID string

	switch target := req.Target.(type) {
	case *corev1.DeployCommitSchemaRequest_NewDatabase:
		// Create new database on specified instance
		targetDB, targetDatabaseID, err = s.createNewDatabaseForDeploy(ctx, req.TenantId, workspaceID, req.WorkspaceName, target.NewDatabase)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, err
		}

	case *corev1.DeployCommitSchemaRequest_ExistingDatabase:
		// Use existing database
		databaseService := database.NewService(s.engine.db, s.engine.logger)
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
	deploySchema := sourceCommit.SchemaStructure
	var warnings []string

	if sourceDBType != targetDB.Type {
		s.engine.logger.Infof("Converting schema from %s to %s", sourceDBType, targetDB.Type)

		// Convert schema structure to JSON string for conversion
		schemaJSON, err := json.Marshal(sourceCommit.SchemaStructure)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to serialize source schema: %v", err)
		}

		convertedSchemaStr, convertWarnings, err := s.convertSchemaViaUnifiedModel(ctx, string(schemaJSON), sourceDBType, targetDB.Type)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert schema: %v", err)
		}

		// Parse converted schema back to map
		err = json.Unmarshal([]byte(convertedSchemaStr), &deploySchema)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to parse converted schema: %v", err)
		}

		warnings = append(warnings, convertWarnings...)
	}

	// Step 4: Deploy schema to target database
	deploySchemaJSON, err := json.Marshal(deploySchema)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to serialize deploy schema: %v", err)
	}

	err = s.deploySchemaToDatabase(ctx, targetDatabaseID, string(deploySchemaJSON), &corev1.CloneOptions{
		Wipe:  req.Options != nil && req.Options.Wipe,
		Merge: req.Options != nil && req.Options.Merge,
	})
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to deploy schema: %v", err)
	}

	// Step 5: Wait for anchor to discover schema and create repo/commit
	repoID, branchID, commitID, err := s.waitForAnchorDiscovery(ctx, targetDatabaseID, 60*time.Second)
	if err != nil {
		s.engine.logger.Warnf("Failed to wait for anchor discovery: %v", err)
		warnings = append(warnings, fmt.Sprintf("Schema deployed but anchor discovery failed: %v", err))
	}

	return &corev1.DeployCommitSchemaResponse{
		Message:          "Commit schema deployed successfully",
		Success:          true,
		Status:           commonv1.Status_STATUS_SUCCESS,
		TargetDatabaseId: targetDatabaseID,
		TargetRepoId:     repoID,
		TargetBranchId:   branchID,
		TargetCommitId:   commitID,
		Warnings:         warnings,
	}, nil
}

// getCommitSchema retrieves schema from a specific commit in the commits table
func (s *Server) getCommitSchema(ctx context.Context, tenantID, workspaceID, repoName, branchName, commitCode string) (*commit.Commit, string, error) {
	// Get services
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	branchService := branch.NewService(s.engine.db, s.engine.logger)
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	// Get repo by name
	repoObj, err := repoService.GetByName(ctx, tenantID, workspaceID, repoName)
	if err != nil {
		return nil, "", fmt.Errorf("repo not found: %w", err)
	}

	// Get branch by name
	branchObj, err := branchService.GetByName(ctx, tenantID, workspaceID, repoObj.ID, branchName)
	if err != nil {
		return nil, "", fmt.Errorf("branch not found: %w", err)
	}

	// Get commit by code (or HEAD if empty)
	var commitObj *commit.Commit
	if commitCode == "" || commitCode == "HEAD" {
		// Get the latest commit (head)
		commits, err := commitService.List(ctx, tenantID, workspaceID, repoObj.ID, branchObj.ID)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list commits: %w", err)
		}
		if len(commits) == 0 {
			return nil, "", fmt.Errorf("no commits found in branch")
		}
		// Find the head commit
		for _, c := range commits {
			if c.IsHead {
				commitObj = c
				break
			}
		}
		if commitObj == nil {
			// Fallback to the first commit if no head is marked
			commitObj = commits[0]
		}
	} else {
		// Get specific commit by code
		commitObj, err = commitService.GetByCode(ctx, tenantID, workspaceID, repoObj.ID, branchObj.ID, commitCode)
		if err != nil {
			return nil, "", fmt.Errorf("commit not found: %w", err)
		}
	}

	// Validate that commit has schema
	if len(commitObj.SchemaStructure) == 0 {
		return nil, "", fmt.Errorf("commit has no schema structure")
	}

	// Get the source database type from the schema type field or infer from connected database
	sourceDBType := commitObj.SchemaType
	if sourceDBType == "" {
		// Try to get database type from connected database if branch is connected
		if branchObj.ConnectedToDatabase && branchObj.ConnectedDatabaseID != nil {
			databaseService := database.NewService(s.engine.db, s.engine.logger)
			connectedDB, err := databaseService.GetByID(ctx, *branchObj.ConnectedDatabaseID)
			if err == nil {
				sourceDBType = connectedDB.Type
			}
		}
	}

	if sourceDBType == "" {
		return nil, "", fmt.Errorf("unable to determine source database type from commit")
	}

	return commitObj, sourceDBType, nil
}

// createNewDatabaseForDeploy creates a new database for deployment operations
func (s *Server) createNewDatabaseForDeploy(ctx context.Context, tenantID, workspaceID, workspaceName string, target *corev1.NewDatabaseTarget) (*database.Database, string, error) {
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
		fmt.Sprintf("Deployed from commit: %s", target.DatabaseName),
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
