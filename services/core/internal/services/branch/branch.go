package branch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles branch-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new branch service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Branch represents a branch in the system
type Branch struct {
	ID                  string
	TenantID            string
	WorkspaceID         string
	RepoID              string
	Name                string
	ParentBranchID      *string
	ConnectedToDatabase bool
	ConnectedDatabaseID *string
	PolicyIDs           []string
	Status              string
	Created             time.Time
	Updated             time.Time
}

// BranchWithCommits represents a branch with its commits and child branches
type BranchWithCommits struct {
	ID                  string
	TenantID            string
	WorkspaceID         string
	RepoID              string
	Name                string
	ParentBranchID      *string
	ConnectedToDatabase bool
	ConnectedDatabaseID *string
	PolicyIDs           []string
	Status              string
	Created             time.Time
	Updated             time.Time
	Commits             []*Commit
	ChildBranches       []*Branch
}

// Commit represents a commit in the system (duplicated from commit package for convenience)
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

// Get retrieves a branch by ID
func (s *Service) Get(ctx context.Context, tenantID, workspaceID, repoID, branchID string) (*Branch, error) {
	s.logger.Infof("Retrieving branch from database with ID: %s", branchID)
	query := `
		SELECT branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, 
		       connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, 
		       status, created, updated
		FROM branches
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4
	`

	var branch Branch
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID, branchID).Scan(
		&branch.ID,
		&branch.TenantID,
		&branch.WorkspaceID,
		&branch.RepoID,
		&branch.Name,
		&branch.ParentBranchID,
		&branch.ConnectedToDatabase,
		&branch.ConnectedDatabaseID,
		&branch.PolicyIDs,
		&branch.Status,
		&branch.Created,
		&branch.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("branch not found")
		}
		s.logger.Errorf("Failed to get branch: %v", err)
		return nil, err
	}

	return &branch, nil
}

// GetByName retrieves a branch by name
func (s *Service) GetByName(ctx context.Context, tenantID, workspaceID, repoID, branchName string) (*Branch, error) {
	s.logger.Infof("Retrieving branch from database with name: %s", branchName)
	query := `
		SELECT branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, 
		       connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, 
		       status, created, updated
		FROM branches
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_name = $4
	`

	var branch Branch
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID, branchName).Scan(
		&branch.ID,
		&branch.TenantID,
		&branch.WorkspaceID,
		&branch.RepoID,
		&branch.Name,
		&branch.ParentBranchID,
		&branch.ConnectedToDatabase,
		&branch.ConnectedDatabaseID,
		&branch.PolicyIDs,
		&branch.Status,
		&branch.Created,
		&branch.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("branch not found")
		}
		s.logger.Errorf("Failed to get branch by name: %v", err)
		return nil, err
	}

	return &branch, nil
}

// GetDatabaseIDByName gets database ID by database name
func (s *Service) GetDatabaseIDByName(ctx context.Context, tenantID, workspaceID, databaseName string) (string, error) {
	s.logger.Infof("Getting database ID for database name: %s", databaseName)
	query := `
		SELECT database_id
		FROM databases
		WHERE tenant_id = $1 AND workspace_id = $2 AND database_name = $3
	`

	var databaseID string
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, databaseName).Scan(&databaseID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", errors.New("database not found")
		}
		s.logger.Errorf("Failed to get database ID by name: %v", err)
		return "", err
	}

	return databaseID, nil
}

// AttachToDatabase attaches a branch to a database
func (s *Service) AttachToDatabase(ctx context.Context, tenantID, workspaceID, repoID, branchName, databaseName string) (*Branch, error) {
	s.logger.Infof("Attaching branch %s to database %s", branchName, databaseName)

	// Get branch by name
	branch, err := s.GetByName(ctx, tenantID, workspaceID, repoID, branchName)
	if err != nil {
		return nil, err
	}

	// Get database ID by name
	databaseID, err := s.GetDatabaseIDByName(ctx, tenantID, workspaceID, databaseName)
	if err != nil {
		return nil, err
	}

	// Update branch to attach to database
	query := `
		UPDATE branches 
		SET connected_to_database = true, connected_database_id = $5, updated = CURRENT_TIMESTAMP
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4
		RETURNING branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, 
		          connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, 
		          status, created, updated
	`

	var updatedBranch Branch
	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID, branch.ID, databaseID).Scan(
		&updatedBranch.ID,
		&updatedBranch.TenantID,
		&updatedBranch.WorkspaceID,
		&updatedBranch.RepoID,
		&updatedBranch.Name,
		&updatedBranch.ParentBranchID,
		&updatedBranch.ConnectedToDatabase,
		&updatedBranch.ConnectedDatabaseID,
		&updatedBranch.PolicyIDs,
		&updatedBranch.Status,
		&updatedBranch.Created,
		&updatedBranch.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("branch not found")
		}
		s.logger.Errorf("Failed to attach branch to database: %v", err)
		return nil, err
	}

	return &updatedBranch, nil
}

// DetachFromDatabase detaches a branch from a database
func (s *Service) DetachFromDatabase(ctx context.Context, tenantID, workspaceID, repoID, branchName string) (*Branch, error) {
	s.logger.Infof("Detaching branch %s from database", branchName)

	// Get branch by name
	branch, err := s.GetByName(ctx, tenantID, workspaceID, repoID, branchName)
	if err != nil {
		return nil, err
	}

	// Update branch to detach from database
	query := `
		UPDATE branches 
		SET connected_to_database = false, connected_database_id = NULL, updated = CURRENT_TIMESTAMP
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4
		RETURNING branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, 
		          connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, 
		          status, created, updated
	`

	var updatedBranch Branch
	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID, branch.ID).Scan(
		&updatedBranch.ID,
		&updatedBranch.TenantID,
		&updatedBranch.WorkspaceID,
		&updatedBranch.RepoID,
		&updatedBranch.Name,
		&updatedBranch.ParentBranchID,
		&updatedBranch.ConnectedToDatabase,
		&updatedBranch.ConnectedDatabaseID,
		&updatedBranch.PolicyIDs,
		&updatedBranch.Status,
		&updatedBranch.Created,
		&updatedBranch.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("branch not found")
		}
		s.logger.Errorf("Failed to detach branch from database: %v", err)
		return nil, err
	}

	return &updatedBranch, nil
}

// Update updates a branch
func (s *Service) Update(ctx context.Context, tenantID, workspaceID, repoID, branchName string, updates map[string]interface{}) (*Branch, error) {
	s.logger.Infof("Updating branch with name: %s", branchName)

	// Get branch by name
	branch, err := s.GetByName(ctx, tenantID, workspaceID, repoID, branchName)
	if err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return branch, nil
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, workspaceID, repoID, branch.ID}
	argIndex := 5

	for field, value := range updates {
		switch field {
		case "branch_name":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		default:
			s.logger.Warnf("Ignoring invalid update field: %s", field)
		}
	}

	if len(setParts) == 0 {
		return branch, nil
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE branches 
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4
		RETURNING branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, 
		          connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, 
		          status, created, updated
	`, setClause)

	var updatedBranch Branch
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&updatedBranch.ID,
		&updatedBranch.TenantID,
		&updatedBranch.WorkspaceID,
		&updatedBranch.RepoID,
		&updatedBranch.Name,
		&updatedBranch.ParentBranchID,
		&updatedBranch.ConnectedToDatabase,
		&updatedBranch.ConnectedDatabaseID,
		&updatedBranch.PolicyIDs,
		&updatedBranch.Status,
		&updatedBranch.Created,
		&updatedBranch.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("branch not found")
		}
		s.logger.Errorf("Failed to update branch: %v", err)
		return nil, err
	}

	return &updatedBranch, nil
}

// Delete deletes a branch
func (s *Service) Delete(ctx context.Context, tenantID, workspaceID, repoID, branchName string, force bool) error {
	s.logger.Infof("Deleting branch with name: %s", branchName)

	// Get branch by name
	branch, err := s.GetByName(ctx, tenantID, workspaceID, repoID, branchName)
	if err != nil {
		return err
	}

	// Check for existing commits if not force delete
	if !force {
		var commitCount int
		err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM commits WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4",
			tenantID, workspaceID, repoID, branch.ID).Scan(&commitCount)
		if err != nil {
			return fmt.Errorf("failed to check for existing commits: %w", err)
		}
		if commitCount > 0 {
			return errors.New("cannot delete branch with existing commits (use force=true to override)")
		}
	}

	// Check for child branches
	var childCount int
	err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM branches WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND parent_branch_id = $4",
		tenantID, workspaceID, repoID, branch.ID).Scan(&childCount)
	if err != nil {
		return fmt.Errorf("failed to check for child branches: %w", err)
	}
	if childCount > 0 {
		return errors.New("cannot delete branch with child branches")
	}

	// Begin transaction for cascading deletes
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Delete commits first
	_, err = tx.Exec(ctx, "DELETE FROM commits WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4",
		tenantID, workspaceID, repoID, branch.ID)
	if err != nil {
		return fmt.Errorf("failed to delete commits: %w", err)
	}

	// Delete branch
	result, err := tx.Exec(ctx, "DELETE FROM branches WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4",
		tenantID, workspaceID, repoID, branch.ID)
	if err != nil {
		return fmt.Errorf("failed to delete branch: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("branch not found")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetBranchNameByID gets branch name by branch ID
func (s *Service) GetBranchNameByID(ctx context.Context, tenantID, workspaceID, repoID, branchID string) (string, error) {
	s.logger.Infof("Getting branch name for branch ID: %s", branchID)
	query := `
		SELECT branch_name
		FROM branches
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND branch_id = $4
	`

	var branchName string
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID, branchID).Scan(&branchName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", errors.New("branch not found")
		}
		s.logger.Errorf("Failed to get branch name by ID: %v", err)
		return "", err
	}

	return branchName, nil
}

// GetByNameWithCommits retrieves a branch by name with its commits and child branches
func (s *Service) GetByNameWithCommits(ctx context.Context, tenantID, workspaceID, repoID, branchName string) (*BranchWithCommits, error) {
	s.logger.Infof("Retrieving branch with commits and child branches from database with name: %s", branchName)

	// First get the branch
	branch, err := s.GetByName(ctx, tenantID, workspaceID, repoID, branchName)
	if err != nil {
		return nil, err
	}

	// Create BranchWithCommits from the base branch
	branchWithCommits := &BranchWithCommits{
		ID:                  branch.ID,
		TenantID:            branch.TenantID,
		WorkspaceID:         branch.WorkspaceID,
		RepoID:              branch.RepoID,
		Name:                branch.Name,
		ParentBranchID:      branch.ParentBranchID,
		ConnectedToDatabase: branch.ConnectedToDatabase,
		ConnectedDatabaseID: branch.ConnectedDatabaseID,
		PolicyIDs:           branch.PolicyIDs,
		Status:              branch.Status,
		Created:             branch.Created,
		Updated:             branch.Updated,
	}

	// Get commits for this branch
	commits, err := s.GetCommitsForBranch(ctx, tenantID, workspaceID, repoID, branch.ID)
	if err != nil {
		s.logger.Errorf("Failed to get commits for branch %s: %v", branch.ID, err)
		// Don't fail the entire operation if commits can't be fetched
		branchWithCommits.Commits = []*Commit{}
	} else {
		branchWithCommits.Commits = commits
	}

	// Get child branches
	childBranches, err := s.GetChildBranches(ctx, tenantID, workspaceID, repoID, branch.ID)
	if err != nil {
		s.logger.Errorf("Failed to get child branches for branch %s: %v", branch.ID, err)
		// Don't fail the entire operation if child branches can't be fetched
		branchWithCommits.ChildBranches = []*Branch{}
	} else {
		branchWithCommits.ChildBranches = childBranches
	}

	return branchWithCommits, nil
}

// GetCommitsForBranch retrieves all commits for a specific branch
func (s *Service) GetCommitsForBranch(ctx context.Context, tenantID, workspaceID, repoID, branchID string) ([]*Commit, error) {
	s.logger.Infof("Retrieving commits for branch: %s", branchID)
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
		s.logger.Errorf("Failed to get commits: %v", err)
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

// GetChildBranches retrieves all child branches for a specific branch
func (s *Service) GetChildBranches(ctx context.Context, tenantID, workspaceID, repoID, branchID string) ([]*Branch, error) {
	s.logger.Infof("Retrieving child branches for branch: %s", branchID)
	query := `
		SELECT branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, 
		       connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, 
		       status, created, updated
		FROM branches
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3 AND parent_branch_id = $4
		ORDER BY branch_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID, repoID, branchID)
	if err != nil {
		s.logger.Errorf("Failed to get child branches: %v", err)
		return nil, err
	}
	defer rows.Close()

	var childBranches []*Branch
	for rows.Next() {
		var branch Branch
		err := rows.Scan(
			&branch.ID,
			&branch.TenantID,
			&branch.WorkspaceID,
			&branch.RepoID,
			&branch.Name,
			&branch.ParentBranchID,
			&branch.ConnectedToDatabase,
			&branch.ConnectedDatabaseID,
			&branch.PolicyIDs,
			&branch.Status,
			&branch.Created,
			&branch.Updated,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan child branch: %v", err)
			return nil, err
		}
		childBranches = append(childBranches, &branch)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning child branches: %v", err)
		return nil, err
	}

	return childBranches, nil
}
