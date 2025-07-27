package commit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles commit-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new commit service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Commit represents a commit in the system
type Commit struct {
	ID              int32
	TenantID        string
	WorkspaceID     string
	RepoID          string
	BranchID        string
	Code            string
	IsHead          bool
	Message         string
	SchemaType      string
	SchemaStructure map[string]interface{}
	PolicyIDs       []string
	Created         time.Time
	Updated         time.Time
}

type AnchorCommit struct {
	CommitID string
	BranchID string
	Success  bool
}

// Get retrieves a commit by ID
func (s *Service) Get(ctx context.Context, tenantID, workspaceID, repoID, branchID string, id int32) (*Commit, error) {
	s.logger.Infof("Retrieving commit from database with ID: %d", id)
	query := `
		SELECT commit_id, tenant_id, workspace_id, repo_id, branch_id, commit_code, 
		       commit_is_head, commit_message, schema_type, COALESCE(schema_structure, '{}') as schema_structure,
		       COALESCE(policy_ids, '{}') as policy_ids, created, updated
		FROM commits
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4 AND commit_id = $5
	`

	var commit Commit
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID, branchID, id).Scan(
		&commit.ID,
		&commit.TenantID,
		&commit.WorkspaceID,
		&commit.RepoID,
		&commit.BranchID,
		&commit.Code,
		&commit.IsHead,
		&commit.Message,
		&commit.SchemaType,
		&commit.SchemaStructure,
		&commit.PolicyIDs,
		&commit.Created,
		&commit.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("commit not found")
		}
		s.logger.Errorf("Failed to get commit: %v", err)
		return nil, err
	}

	return &commit, nil
}

// GetByCode retrieves a commit by code
func (s *Service) GetByCode(ctx context.Context, tenantID, workspaceID, repoID, branchID, code string) (*Commit, error) {
	s.logger.Infof("Retrieving commit from database with code: %s", code)
	query := `
		SELECT commit_id, tenant_id, workspace_id, repo_id, branch_id, commit_code, 
		       commit_is_head, commit_message, schema_type, COALESCE(schema_structure, '{}') as schema_structure,
		       COALESCE(policy_ids, '{}') as policy_ids, created, updated
		FROM commits
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4 AND commit_code = $5
	`

	var commit Commit
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID, branchID, code).Scan(
		&commit.ID,
		&commit.TenantID,
		&commit.WorkspaceID,
		&commit.RepoID,
		&commit.BranchID,
		&commit.Code,
		&commit.IsHead,
		&commit.Message,
		&commit.SchemaType,
		&commit.SchemaStructure,
		&commit.PolicyIDs,
		&commit.Created,
		&commit.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("commit not found")
		}
		s.logger.Errorf("Failed to get commit by code: %v", err)
		return nil, err
	}

	return &commit, nil
}

// CreateBranchFromCommit creates a new branch from an existing commit
func (s *Service) CreateBranchFromCommit(ctx context.Context, tenantID, workspaceID, repoID, branchID string, commitID int32, newBranchName string) (*Commit, error) {
	s.logger.Infof("Creating branch %s from commit %d", newBranchName, commitID)

	// Check if commit exists
	commit, err := s.Get(ctx, tenantID, workspaceID, repoID, branchID, commitID)
	if err != nil {
		return nil, err
	}

	// Check if source branch exists
	var sourceBranchExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM branches WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4)",
		tenantID, workspaceID, repoID, branchID).Scan(&sourceBranchExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check source branch existence: %w", err)
	}
	if !sourceBranchExists {
		return nil, errors.New("source branch not found")
	}

	// Begin transaction
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create new branch
	var newBranchID string
	err = tx.QueryRow(ctx, `
		INSERT INTO branches (tenant_id, workspace_id, repo_id, branch_name, parent_branch_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING branch_id
	`, tenantID, workspaceID, repoID, newBranchName, branchID).Scan(&newBranchID)
	if err != nil {
		return nil, fmt.Errorf("failed to create new branch: %w", err)
	}

	// TODO: In a full implementation, you would copy all commits up to this point
	// For now, we just return the original commit

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return commit, nil
}

// MergeToParent merges a commit to the parent branch
func (s *Service) MergeToParent(ctx context.Context, tenantID, workspaceID, repoID, branchID string, commitID int32) (*Commit, error) {
	s.logger.Infof("Merging commit %d to parent branch", commitID)

	// Check if commit exists
	commit, err := s.Get(ctx, tenantID, workspaceID, repoID, branchID, commitID)
	if err != nil {
		return nil, err
	}

	// Get parent branch ID
	var parentBranchID *string
	err = s.db.Pool().QueryRow(ctx, "SELECT parent_branch_id FROM branches WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4",
		tenantID, workspaceID, repoID, branchID).Scan(&parentBranchID)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent branch: %w", err)
	}
	if parentBranchID == nil {
		return nil, errors.New("branch has no parent to merge to")
	}

	// TODO: In a full implementation, you would perform the actual merge logic
	// This might involve creating a new commit in the parent branch, resolving conflicts, etc.
	// For now, we just return the commit

	return commit, nil
}

// Deploy performs deployment operations for a commit
func (s *Service) Deploy(ctx context.Context, tenantID, workspaceID, repoID, branchID string, commitID int32) (*Commit, error) {
	s.logger.Infof("Deploying commit %d", commitID)

	// Check if commit exists
	commit, err := s.Get(ctx, tenantID, workspaceID, repoID, branchID, commitID)
	if err != nil {
		return nil, err
	}

	// TODO: In a full implementation, you would perform actual deployment logic
	// This might involve applying schema changes, running migrations, etc.
	// For now, we just return the commit

	return commit, nil
}

// List retrieves all commits for a branch
func (s *Service) List(ctx context.Context, tenantID, workspaceID, repoID, branchID string) ([]*Commit, error) {
	s.logger.Infof("Listing commits for branch: %s", branchID)
	query := `
		SELECT commit_id, tenant_id, workspace_id, repo_id, branch_id, commit_code, 
		       commit_is_head, commit_message, schema_type, COALESCE(schema_structure, '{}') as schema_structure,
		       COALESCE(policy_ids, '{}') as policy_ids, created, updated
		FROM commits
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4
		ORDER BY created DESC
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID, repoID, branchID)
	if err != nil {
		s.logger.Errorf("Failed to list commits: %v", err)
		return nil, err
	}
	defer rows.Close()

	var commits []*Commit
	for rows.Next() {
		var commit Commit
		err := rows.Scan(
			&commit.ID,
			&commit.TenantID,
			&commit.WorkspaceID,
			&commit.RepoID,
			&commit.BranchID,
			&commit.Code,
			&commit.IsHead,
			&commit.Message,
			&commit.SchemaType,
			&commit.SchemaStructure,
			&commit.PolicyIDs,
			&commit.Created,
			&commit.Updated,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan commit: %v", err)
			return nil, err
		}
		commits = append(commits, &commit)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning commits: %v", err)
		return nil, err
	}

	return commits, nil
}

// CreateCommitByAnchor creates a commit for a given repo and branch
func (s *Service) CreateCommitByAnchor(ctx context.Context, branchID string, commitMessage string, schemaType string, schemaStructure map[string]interface{}) (*AnchorCommit, error) {
	s.logger.Infof("Creating commit for branch: %s", branchID)

	// Begin transaction
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get branch details to get tenant_id and workspace_id
	var tenantID, workspaceID, repoID string
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, workspace_id, repo_id
		FROM branches 
		WHERE branch_id = $1
	`, branchID).Scan(&tenantID, &workspaceID, &repoID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("branch not found")
		}
		return nil, fmt.Errorf("failed to get branch details: %w", err)
	}

	// Set all existing commits in this branch to not be head
	_, err = tx.Exec(ctx, `
		UPDATE commits 
		SET commit_is_head = false 
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4
	`, tenantID, workspaceID, repoID, branchID)
	if err != nil {
		return nil, fmt.Errorf("failed to update existing commits: %w", err)
	}

	// Create new commit and set it as head
	var commitID int32
	err = tx.QueryRow(ctx, `
		INSERT INTO commits (tenant_id, workspace_id, repo_id, branch_id, commit_message, schema_type, schema_structure, commit_is_head)
		VALUES ($1, $2, $3, $4, $5, $6, $7, true)
		RETURNING commit_id
	`, tenantID, workspaceID, repoID, branchID, commitMessage, schemaType, schemaStructure).Scan(&commitID)
	if err != nil {
		return nil, fmt.Errorf("failed to create commit: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &AnchorCommit{
		CommitID: fmt.Sprintf("%d", commitID),
		BranchID: branchID,
		Success:  true,
	}, nil
}
