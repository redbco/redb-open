package engine

import (
	"context"
	"encoding/json"
	"fmt"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	corev1 "github.com/redbco/redb-open/api/proto/core/v1"
	"github.com/redbco/redb-open/services/core/internal/services/branch"
	"github.com/redbco/redb-open/services/core/internal/services/commit"
	"github.com/redbco/redb-open/services/core/internal/services/repo"
	"github.com/redbco/redb-open/services/core/internal/services/workspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// RepoService gRPC handlers
// ============================================================================

func (s *Server) ListRepos(ctx context.Context, req *corev1.ListReposRequest) (*corev1.ListReposResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// List repos for the tenant and workspace
	repos, err := repoService.List(ctx, req.TenantId, workspaceID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to list repos: %v", err)
	}

	// Convert to protobuf format
	protoRepos := make([]*corev1.Repo, len(repos))
	for i, r := range repos {
		protoRepos[i] = s.repoToProto(r)
	}

	return &corev1.ListReposResponse{
		Repos: protoRepos,
	}, nil
}

func (s *Server) ShowRepo(ctx context.Context, req *corev1.ShowRepoRequest) (*corev1.ShowRepoResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Get the repo with full nested structure
	r, err := repoService.GetByNameWithBranches(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Convert to protobuf format
	protoRepo := s.repoToProto(&repo.Repo{
		ID:          r.ID,
		Name:        r.Name,
		Description: r.Description,
		OwnerID:     r.OwnerID,
	})
	protoBranches := make([]*corev1.Branch, len(r.Branches))
	for i, b := range r.Branches {
		protoBranches[i] = s.branchToProtoWithCommits(b)
	}

	return &corev1.ShowRepoResponse{
		Repo: &corev1.FullRepo{
			TenantId:        protoRepo.TenantId,
			WorkspaceId:     protoRepo.WorkspaceId,
			RepoId:          protoRepo.RepoId,
			RepoName:        protoRepo.RepoName,
			RepoDescription: protoRepo.RepoDescription,
			OwnerId:         protoRepo.OwnerId,
			Branches:        protoBranches,
		},
	}, nil
}

func (s *Server) AddRepo(ctx context.Context, req *corev1.AddRepoRequest) (*corev1.AddRepoResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Create the repo
	createdRepo, err := repoService.Create(ctx, req.TenantId, workspaceID, req.RepoName, req.RepoDescription, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create repo: %v", err)
	}

	// Convert to protobuf format
	protoRepo := s.repoToProto(createdRepo)

	return &corev1.AddRepoResponse{
		Message: "Repository created successfully",
		Success: true,
		Repo:    protoRepo,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyRepo(ctx context.Context, req *corev1.ModifyRepoRequest) (*corev1.ModifyRepoResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Build update map
	updates := make(map[string]interface{})
	if req.RepoNameNew != nil {
		updates["repo_name"] = *req.RepoNameNew
	}
	if req.RepoDescription != nil {
		updates["repo_description"] = *req.RepoDescription
	}

	// Update the repo
	updatedRepo, err := repoService.Update(ctx, req.TenantId, workspaceID, req.RepoName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update repo: %v", err)
	}

	// Convert to protobuf format
	protoRepo := s.repoToProto(updatedRepo)

	return &corev1.ModifyRepoResponse{
		Message: "Repository updated successfully",
		Success: true,
		Repo:    protoRepo,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) CloneRepo(ctx context.Context, req *corev1.CloneRepoRequest) (*corev1.CloneRepoResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Create the cloned repo
	clonedRepo, err := repoService.Create(ctx, req.TenantId, workspaceID, req.CloneRepoName, req.CloneRepoDescription, req.OwnerId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to clone repo: %v", err)
	}

	// Convert to protobuf format
	protoRepo := s.repoToProto(clonedRepo)

	return &corev1.CloneRepoResponse{
		Message: "Repository cloned successfully",
		Success: true,
		Repo:    protoRepo,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteRepo(ctx context.Context, req *corev1.DeleteRepoRequest) (*corev1.DeleteRepoResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Delete the repo
	err = repoService.Delete(ctx, req.TenantId, workspaceID, req.RepoName, req.Force)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete repo: %v", err)
	}

	return &corev1.DeleteRepoResponse{
		Message: "Repository deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// CreateRepoByAnchor creates an empty repo and branch based on the database details and links it to the database
func (s *Server) CreateRepoByAnchor(ctx context.Context, req *corev1.CreateRepoByAnchorRequest) (*corev1.CreateRepoByAnchorResponse, error) {
	defer s.trackOperation()()

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Create the repo
	createdRepo, err := repoService.CreateWithMainBranchAndLinkToDatabase(ctx, req.RepoName, req.RepoDescription, req.DatabaseId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create repo: %v", err)
	}

	return &corev1.CreateRepoByAnchorResponse{
		Message:  "Repository created successfully",
		Success:  createdRepo.Success,
		RepoId:   createdRepo.RepoID,
		BranchId: createdRepo.BranchID,
		Status:   commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// FindRepoAndBranchByDatabaseID finds the repo and branch for a given database id
func (s *Server) FindRepoAndBranchByDatabaseID(ctx context.Context, req *corev1.FindRepoAndBranchByDatabaseIDRequest) (*corev1.FindRepoAndBranchByDatabaseIDResponse, error) {
	defer s.trackOperation()()

	// Get repo service
	repoService := repo.NewService(s.engine.db, s.engine.logger)

	// Find the repo and branch for the given database id
	repo, err := repoService.FindRepoAndBranchByDatabaseID(ctx, req.DatabaseId)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to find repo and branch: %v", err)
	}

	return &corev1.FindRepoAndBranchByDatabaseIDResponse{
		Message:  "Repo and branch found successfully",
		Success:  repo.Success,
		RepoId:   repo.RepoID,
		BranchId: repo.BranchID,
		Status:   commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ============================================================================
// BranchService gRPC handlers
// ============================================================================

func (s *Server) ShowBranch(ctx context.Context, req *corev1.ShowBranchRequest) (*corev1.ShowBranchResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service
	branchService := branch.NewService(s.engine.db, s.engine.logger)

	// Get the branch
	b, err := branchService.GetByNameWithCommits(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "branch not found: %v", err)
	}

	// Convert to protobuf format
	protoBranch := s.branchWithCommitsToProto(b)

	return &corev1.ShowBranchResponse{
		Branch: protoBranch,
	}, nil
}

func (s *Server) AttachBranch(ctx context.Context, req *corev1.AttachBranchRequest) (*corev1.AttachBranchResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service
	branchService := branch.NewService(s.engine.db, s.engine.logger)

	// Attach the branch to the database
	attachedBranch, err := branchService.AttachToDatabase(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName, req.DatabaseName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to attach branch: %v", err)
	}

	// Convert to protobuf format
	protoBranch := s.branchToProto(attachedBranch)

	return &corev1.AttachBranchResponse{
		Message: "Branch attached successfully",
		Success: true,
		Branch:  protoBranch,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DetachBranch(ctx context.Context, req *corev1.DetachBranchRequest) (*corev1.DetachBranchResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.Get(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service
	branchService := branch.NewService(s.engine.db, s.engine.logger)

	// Detach the branch from the database
	detachedBranch, err := branchService.DetachFromDatabase(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to detach branch: %v", err)
	}

	// Convert to protobuf format
	protoBranch := s.branchToProto(detachedBranch)

	return &corev1.DetachBranchResponse{
		Message: "Branch detached successfully",
		Success: true,
		Branch:  protoBranch,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) ModifyBranch(ctx context.Context, req *corev1.ModifyBranchRequest) (*corev1.ModifyBranchResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service
	branchService := branch.NewService(s.engine.db, s.engine.logger)

	// Build update map
	updates := make(map[string]interface{})
	if req.BranchNameNew != nil {
		updates["branch_name"] = *req.BranchNameNew
	}

	// Update the branch
	updatedBranch, err := branchService.Update(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName, updates)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to update branch: %v", err)
	}

	// Convert to protobuf format
	protoBranch := s.branchToProto(updatedBranch)

	return &corev1.ModifyBranchResponse{
		Message: "Branch updated successfully",
		Success: true,
		Branch:  protoBranch,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeleteBranch(ctx context.Context, req *corev1.DeleteBranchRequest) (*corev1.DeleteBranchResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service
	branchService := branch.NewService(s.engine.db, s.engine.logger)

	// Delete the branch
	err = branchService.Delete(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName, req.Force)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to delete branch: %v", err)
	}

	return &corev1.DeleteBranchResponse{
		Message: "Branch deleted successfully",
		Success: true,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ============================================================================
// CommitService gRPC handlers
// ============================================================================

func (s *Server) ShowCommit(ctx context.Context, req *corev1.ShowCommitRequest) (*corev1.ShowCommitResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service to get branch ID
	branchService := branch.NewService(s.engine.db, s.engine.logger)
	branchObj, err := branchService.GetByName(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "branch not found: %v", err)
	}

	// Get commit service
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	// Get the commit by code
	c, err := commitService.GetByCode(ctx, req.TenantId, workspaceID, repoObj.ID, branchObj.ID, req.CommitCode)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "commit not found: %v", err)
	}

	// Convert to protobuf format
	protoCommit := s.commitToProto(c)

	return &corev1.ShowCommitResponse{
		Commit: protoCommit,
	}, nil
}

func (s *Server) BranchCommit(ctx context.Context, req *corev1.BranchCommitRequest) (*corev1.BranchCommitResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service to get branch ID
	branchService := branch.NewService(s.engine.db, s.engine.logger)
	branchObj, err := branchService.GetByName(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "branch not found: %v", err)
	}

	// Get commit service
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	// Get the commit by code
	commitObj, err := commitService.GetByCode(ctx, req.TenantId, workspaceID, repoObj.ID, branchObj.ID, req.CommitCode)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "commit not found: %v", err)
	}

	// Create new branch from commit
	newBranchCommit, err := commitService.CreateBranchFromCommit(ctx, req.TenantId, workspaceID, repoObj.ID, branchObj.ID, commitObj.ID, req.NewBranchName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create branch from commit: %v", err)
	}

	// Convert to protobuf format
	protoCommit := s.commitToProto(newBranchCommit)

	return &corev1.BranchCommitResponse{
		Message: "Branch created from commit successfully",
		Success: true,
		Commit:  protoCommit,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) MergeCommit(ctx context.Context, req *corev1.MergeCommitRequest) (*corev1.MergeCommitResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service to get branch ID
	branchService := branch.NewService(s.engine.db, s.engine.logger)
	branchObj, err := branchService.Get(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "branch not found: %v", err)
	}

	// Get commit service
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	// Get the commit by code
	commitObj, err := commitService.GetByCode(ctx, req.TenantId, workspaceID, repoObj.ID, branchObj.ID, req.CommitCode)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "commit not found: %v", err)
	}

	// Merge commit to parent branch
	mergedCommit, err := commitService.MergeToParent(ctx, req.TenantId, workspaceID, repoObj.ID, branchObj.ID, commitObj.ID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to merge commit: %v", err)
	}

	// Convert to protobuf format
	protoCommit := s.commitToProto(mergedCommit)

	return &corev1.MergeCommitResponse{
		Message: "Commit merged successfully",
		Success: true,
		Commit:  protoCommit,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

func (s *Server) DeployCommit(ctx context.Context, req *corev1.DeployCommitRequest) (*corev1.DeployCommitResponse, error) {
	defer s.trackOperation()()

	// Get workspace service to convert workspace name to ID
	workspaceService := workspace.NewService(s.engine.db, s.engine.logger)
	workspaceID, err := workspaceService.GetWorkspaceID(ctx, req.TenantId, req.WorkspaceName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to get workspace ID: %v", err)
	}

	// Get repo service to get repo ID
	repoService := repo.NewService(s.engine.db, s.engine.logger)
	repoObj, err := repoService.GetByName(ctx, req.TenantId, workspaceID, req.RepoName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "repo not found: %v", err)
	}

	// Get branch service to get branch ID
	branchService := branch.NewService(s.engine.db, s.engine.logger)
	branchObj, err := branchService.GetByName(ctx, req.TenantId, workspaceID, repoObj.ID, req.BranchName)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "branch not found: %v", err)
	}

	// Get commit service
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	// Get the commit by code
	commitObj, err := commitService.GetByCode(ctx, req.TenantId, workspaceID, repoObj.ID, branchObj.ID, req.CommitCode)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.NotFound, "commit not found: %v", err)
	}

	// Deploy the commit
	deployedCommit, err := commitService.Deploy(ctx, req.TenantId, workspaceID, repoObj.ID, branchObj.ID, commitObj.ID)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to deploy commit: %v", err)
	}

	// Convert to protobuf format
	protoCommit := s.commitToProto(deployedCommit)

	return &corev1.DeployCommitResponse{
		Message: "Commit deployed successfully",
		Success: true,
		Commit:  protoCommit,
		Status:  commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// CreateCommitByAnchor creates a commit for a given repo and branch
func (s *Server) CreateCommitByAnchor(ctx context.Context, req *corev1.CreateCommitByAnchorRequest) (*corev1.CreateCommitByAnchorResponse, error) {
	defer s.trackOperation()()

	// Get commit service
	commitService := commit.NewService(s.engine.db, s.engine.logger)

	// Convert schema structure to map[string]interface{}
	schemaStructureMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(req.SchemaStructure), &schemaStructureMap)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.InvalidArgument, "invalid schema structure: %v", err)
	}

	// Create the commit
	createdCommit, err := commitService.CreateCommitByAnchor(ctx, req.BranchId, req.CommitMessage, req.SchemaType, schemaStructureMap)
	if err != nil {
		s.engine.IncrementErrors()
		return nil, status.Errorf(codes.Internal, "failed to create commit: %v", err)
	}

	return &corev1.CreateCommitByAnchorResponse{
		Message:  "Commit created successfully",
		Success:  createdCommit.Success,
		CommitId: createdCommit.CommitID,
		BranchId: createdCommit.BranchID,
		Status:   commonv1.Status_STATUS_SUCCESS,
	}, nil
}

// ============================================================================
// Conversion functions
// ============================================================================

// Helper function to convert branch to protobuf
func (s *Server) branchToProto(b *branch.Branch) *corev1.Branch {
	var parentBranchID, parentBranchName, databaseID string
	if b.ParentBranchID != nil {
		parentBranchID = *b.ParentBranchID
		// Get parent branch name
		branchService := branch.NewService(s.engine.db, s.engine.logger)
		if parentName, err := branchService.GetBranchNameByID(context.Background(), b.TenantID, b.WorkspaceID, b.RepoID, parentBranchID); err == nil {
			parentBranchName = parentName
		}
	}
	if b.ConnectedDatabaseID != nil {
		databaseID = *b.ConnectedDatabaseID
	}

	return &corev1.Branch{
		TenantId:            b.TenantID,
		WorkspaceId:         b.WorkspaceID,
		RepoId:              b.RepoID,
		BranchId:            b.ID,
		BranchName:          b.Name,
		ParentBranchId:      parentBranchID,
		ParentBranchName:    parentBranchName,
		ConnectedToDatabase: b.ConnectedToDatabase,
		DatabaseId:          databaseID,
		Status:              statusStringToProto(b.Status),
	}
}

// Helper function to convert commit to protobuf
func (s *Server) commitToProto(c *commit.Commit) *corev1.Commit {
	// Convert schema structure to JSON string
	schemaStructureJSON := "{}"
	if c.SchemaStructure != nil {
		if jsonBytes, err := json.Marshal(c.SchemaStructure); err == nil {
			schemaStructureJSON = string(jsonBytes)
		}
	}

	return &corev1.Commit{
		TenantId:        c.TenantID,
		WorkspaceId:     c.WorkspaceID,
		RepoId:          c.RepoID,
		BranchId:        c.BranchID,
		CommitId:        fmt.Sprintf("%d", c.ID),
		CommitCode:      c.Code,
		IsHead:          c.IsHead,
		CommitMessage:   c.Message,
		SchemaType:      c.SchemaType,
		SchemaStructure: schemaStructureJSON,
		CommitDate:      c.Created.Format("2006-01-02T15:04:05Z"),
	}
}

func (s *Server) branchToProtoWithCommits(b *repo.Branch) *corev1.Branch {
	var parentBranchID, parentBranchName, databaseID string
	if b.ParentBranchID != nil {
		parentBranchID = *b.ParentBranchID
		// Get parent branch name
		branchService := branch.NewService(s.engine.db, s.engine.logger)
		if parentName, err := branchService.GetBranchNameByID(context.Background(), b.TenantID, b.WorkspaceID, b.RepoID, parentBranchID); err == nil {
			parentBranchName = parentName
		}
	}
	if b.ConnectedDatabaseID != nil {
		databaseID = *b.ConnectedDatabaseID
	}

	// Convert commits
	protoCommits := make([]*corev1.Commit, len(b.Commits))
	for i, c := range b.Commits {
		protoCommits[i] = s.commitToProto(&commit.Commit{
			ID:              c.ID,
			TenantID:        c.TenantID,
			WorkspaceID:     c.WorkspaceID,
			RepoID:          c.RepoID,
			BranchID:        c.BranchID,
			Code:            c.Code,
			IsHead:          c.IsHead,
			Message:         c.Message,
			SchemaType:      c.SchemaType,
			SchemaStructure: c.SchemaStructure,
			PolicyIDs:       c.PolicyIDs,
			Created:         c.Created,
			Updated:         c.Updated,
		})
	}

	return &corev1.Branch{
		TenantId:            b.TenantID,
		WorkspaceId:         b.WorkspaceID,
		RepoId:              b.RepoID,
		BranchId:            b.ID,
		BranchName:          b.Name,
		ParentBranchId:      parentBranchID,
		ParentBranchName:    parentBranchName,
		ConnectedToDatabase: b.ConnectedToDatabase,
		DatabaseId:          databaseID,
		Commits:             protoCommits,
		Status:              statusStringToProto(b.Status),
	}
}

// Helper function to convert branch with commits to protobuf
func (s *Server) branchWithCommitsToProto(b *branch.BranchWithCommits) *corev1.Branch {
	var parentBranchID, parentBranchName, databaseID string
	if b.ParentBranchID != nil {
		parentBranchID = *b.ParentBranchID
		// Get parent branch name
		branchService := branch.NewService(s.engine.db, s.engine.logger)
		if parentName, err := branchService.GetBranchNameByID(context.Background(), b.TenantID, b.WorkspaceID, b.RepoID, parentBranchID); err == nil {
			parentBranchName = parentName
		}
	}
	if b.ConnectedDatabaseID != nil {
		databaseID = *b.ConnectedDatabaseID
	}

	// Convert commits
	protoCommits := make([]*corev1.Commit, len(b.Commits))
	for i, c := range b.Commits {
		protoCommits[i] = s.commitToProto(&commit.Commit{
			ID:              c.ID,
			TenantID:        c.TenantID,
			WorkspaceID:     c.WorkspaceID,
			RepoID:          c.RepoID,
			BranchID:        c.BranchID,
			Code:            c.Code,
			IsHead:          c.IsHead,
			Message:         c.Message,
			SchemaType:      c.SchemaType,
			SchemaStructure: c.SchemaStructure,
			PolicyIDs:       c.PolicyIDs,
			Created:         c.Created,
			Updated:         c.Updated,
		})
	}

	// Convert child branches
	protoChildBranches := make([]*corev1.Branch, len(b.ChildBranches))
	for i, child := range b.ChildBranches {
		protoChildBranches[i] = s.branchToProto(child)
	}

	return &corev1.Branch{
		TenantId:            b.TenantID,
		WorkspaceId:         b.WorkspaceID,
		RepoId:              b.RepoID,
		BranchId:            b.ID,
		BranchName:          b.Name,
		ParentBranchId:      parentBranchID,
		ParentBranchName:    parentBranchName,
		ConnectedToDatabase: b.ConnectedToDatabase,
		DatabaseId:          databaseID,
		Branches:            protoChildBranches,
		Commits:             protoCommits,
		Status:              statusStringToProto(b.Status),
	}
}
