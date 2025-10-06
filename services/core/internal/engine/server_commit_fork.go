package engine

import (
	"context"
	"encoding/json"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/commit"
	"github.com/redbco/redb-open/services/core/internal/services/repo"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ForkCommit creates a copy of a commit into a new repository
func (s *Server) ForkCommit(ctx context.Context, req *corev1.ForkCommitRequest) (*corev1.ForkCommitResponse, error) {
	defer s.trackOperation()()

	s.engine.logger.Infof("Starting commit fork: repo=%s, branch=%s, commit=%s, target_repo=%s",
		req.RepoName, req.BranchName, req.CommitCode, req.TargetRepoName)

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

	// Step 2: Determine target database type (if schema conversion is needed)
	targetDBType := sourceDBType
	var warnings []string

	if req.TargetDbType != "" {
		targetDBType = req.TargetDbType
	}

	// Step 3: Convert schema if needed
	targetSchema := sourceCommit.SchemaStructure

	if sourceDBType != targetDBType {
		s.engine.logger.Infof("Converting schema from %s to %s", sourceDBType, targetDBType)

		// Convert schema structure to JSON string for conversion
		schemaJSON, err := json.Marshal(sourceCommit.SchemaStructure)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to serialize source schema: %v", err)
		}

		convertedSchemaStr, convertWarnings, err := s.convertSchemaViaUnifiedModel(ctx, string(schemaJSON), sourceDBType, targetDBType)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to convert schema: %v", err)
		}

		// Parse converted schema back to map
		err = json.Unmarshal([]byte(convertedSchemaStr), &targetSchema)
		if err != nil {
			s.engine.IncrementErrors()
			return nil, status.Errorf(codes.Internal, "failed to parse converted schema: %v", err)
		}

		warnings = append(warnings, convertWarnings...)
	}

	// Step 4: Create new repository
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Get the source repo to get owner_id
	sourceRepo, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "source repository not found: %v", err)
	}

	// Create target repository
	targetRepo, err := repoService.Create(
		ctx,
		req.TenantId,
		workspaceID,
		req.TargetRepoName,
		fmt.Sprintf("Forked from %s/%s commit %s", req.RepoName, req.BranchName, req.CommitCode),
		sourceRepo.OwnerID,
	)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create target repository: %v", err)
	}

	// Step 5: Create main branch in the target repository
	var targetBranchID string
	err = s.engine.db.Pool().QueryRow(ctx, `
		INSERT INTO branches (tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, connected_to_database, connected_database_id)
		VALUES ($1, $2, $3, $4, NULL, false, NULL)
		RETURNING branch_id
	`, req.TenantId, workspaceID, targetRepo.ID, "main").Scan(&targetBranchID)
	if err != nil {
		s.engine.IncrementErrors()
		// Clean up the created repo if branch creation fails
		_ = repoService.Delete(ctx, req.TenantId, workspaceID, req.TargetRepoName, true)
		return nil, status.Errorf(codes.Internal, "failed to create main branch: %v", err)
	}

	// Step 6: Create commit in the target branch
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	// Create commit message
	commitMessage := fmt.Sprintf("Forked from %s/%s commit %s", req.RepoName, req.BranchName, req.CommitCode)
	if sourceDBType != targetDBType {
		commitMessage += fmt.Sprintf(" (converted from %s to %s)", sourceDBType, targetDBType)
	}

	targetCommit, err := commitService.CreateCommitByAnchor(
		ctx,
		targetBranchID,
		commitMessage,
		targetDBType,
		targetSchema,
	)
	if err != nil {
		s.engine.IncrementErrors()
		// Clean up the created repo and branch if commit creation fails
		_ = repoService.Delete(ctx, req.TenantId, workspaceID, req.TargetRepoName, true)
		return nil, status.Errorf(codes.Internal, "failed to create commit: %v", err)
	}

	s.engine.logger.Infof("Successfully forked commit: source=%s/%s/%s, target=%s/main/%s",
		req.RepoName, req.BranchName, req.CommitCode, req.TargetRepoName, targetCommit.CommitID)

	return &corev1.ForkCommitResponse{
		Message:        "Commit forked successfully",
		Success:        true,
		Status:         commonv1.Status_STATUS_SUCCESS,
		TargetRepoId:   targetRepo.ID,
		TargetRepoName: targetRepo.Name,
		TargetBranchId: targetBranchID,
		TargetCommitId: targetCommit.CommitID,
		Warnings:       warnings,
	}, nil
}
