package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redbco/redb-open/pkg/database"
	"github.com/redbco/redb-open/pkg/logger"
)

// Service handles repo-related operations
type Service struct {
	db     *database.PostgreSQL
	logger *logger.Logger
}

// NewService creates a new repo service
func NewService(db *database.PostgreSQL, logger *logger.Logger) *Service {
	return &Service{
		db:     db,
		logger: logger,
	}
}

// Repo represents a repository in the system
type Repo struct {
	ID          string
	TenantID    string
	WorkspaceID string
	Name        string
	Description string
	PolicyIDs   []string
	OwnerID     string
	Status      string
	Created     time.Time
	Updated     time.Time
}

type FullRepo struct {
	ID          string
	TenantID    string
	WorkspaceID string
	Name        string
	Description string
	PolicyIDs   []string
	OwnerID     string
	Status      string
	Created     time.Time
	Updated     time.Time
	Branches    []*Branch
}

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
	Commits             []*Commit
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

type RepoWithBranch struct {
	RepoID   string
	BranchID string
	Success  bool
}

// Create creates a new repo
func (s *Service) Create(ctx context.Context, tenantID, workspaceID, name, description, ownerID string) (*Repo, error) {
	s.logger.Infof("Creating repo in database for tenant: %s, workspace: %s, name: %s", tenantID, workspaceID, name)

	// Check if the tenant exists
	var tenantExists bool
	err := s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1)", tenantID).Scan(&tenantExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check tenant existence: %w", err)
	}
	if !tenantExists {
		return nil, errors.New("tenant not found")
	}

	// Check if the workspace exists and belongs to the tenant
	var workspaceExists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workspaces WHERE workspace_id = $1 AND tenant_id = $2)", workspaceID, tenantID).Scan(&workspaceExists)
	if err != nil {
		return nil, fmt.Errorf("failed to check workspace existence: %w", err)
	}
	if !workspaceExists {
		return nil, errors.New("workspace not found in tenant")
	}

	// Check if repo with the same name already exists in this workspace
	var exists bool
	err = s.db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM repos WHERE tenant_id = $1 AND workspace_id = $2 AND repo_name = $3)", tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check repo existence: %w", err)
	}
	if exists {
		return nil, errors.New("repo with this name already exists in the workspace")
	}

	// Insert the repo into the database
	query := `
		INSERT INTO repos (tenant_id, workspace_id, repo_name, repo_description, owner_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING repo_id, tenant_id, workspace_id, repo_name, repo_description, COALESCE(policy_ids, '{}') as policy_ids, owner_id, status, created, updated
	`

	var repo Repo
	err = s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, name, description, ownerID).Scan(
		&repo.ID,
		&repo.TenantID,
		&repo.WorkspaceID,
		&repo.Name,
		&repo.Description,
		&repo.PolicyIDs,
		&repo.OwnerID,
		&repo.Status,
		&repo.Created,
		&repo.Updated,
	)
	if err != nil {
		s.logger.Errorf("Failed to create repo: %v", err)
		return nil, err
	}

	return &repo, nil
}

// CreateWithMainBranchAndLinkToDatabase creates a new repo with a main branch linked to a database
func (s *Service) CreateWithMainBranchAndLinkToDatabase(ctx context.Context, name, description, databaseID string) (*RepoWithBranch, error) {
	s.logger.Infof("Creating new repo for database: %s", databaseID)

	// Begin transaction
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// First, get the database details to inherit tenant, workspace, and owner
	var tenantID, workspaceID, ownerID string
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, workspace_id, owner_id 
		FROM databases 
		WHERE database_id = $1
	`, databaseID).Scan(&tenantID, &workspaceID, &ownerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("database not found")
		}
		return nil, fmt.Errorf("failed to get database details: %w", err)
	}

	// Check if repo with the same name already exists in this workspace
	var exists bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM repos WHERE tenant_id = $1 AND workspace_id = $2 AND repo_name = $3)
	`, tenantID, workspaceID, name).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check repo existence: %w", err)
	}
	if exists {
		return nil, errors.New("repo with this name already exists in the workspace")
	}

	// Create the repository
	var repoID string
	err = tx.QueryRow(ctx, `
		INSERT INTO repos (tenant_id, workspace_id, repo_name, repo_description, owner_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING repo_id
	`, tenantID, workspaceID, name, description, ownerID).Scan(&repoID)
	if err != nil {
		return nil, fmt.Errorf("failed to create repo: %w", err)
	}

	// Create the main branch linked to the database
	var branchID string
	err = tx.QueryRow(ctx, `
		INSERT INTO branches (tenant_id, workspace_id, repo_id, branch_name, connected_to_database, connected_database_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING branch_id
	`, tenantID, workspaceID, repoID, "main", true, databaseID).Scan(&branchID)
	if err != nil {
		return nil, fmt.Errorf("failed to create main branch: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &RepoWithBranch{
		RepoID:   repoID,
		BranchID: branchID,
		Success:  true,
	}, nil
}

// Get retrieves a repo by ID
func (s *Service) Get(ctx context.Context, tenantID, workspaceID, repoID string) (*Repo, error) {
	s.logger.Infof("Retrieving repo from database with ID: %s", repoID)
	query := `
		SELECT repo_id, tenant_id, workspace_id, repo_name, repo_description, COALESCE(policy_ids, '{}') as policy_ids, owner_id, status, created, updated
		FROM repos
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3
	`

	var repo Repo
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoID).Scan(
		&repo.ID,
		&repo.TenantID,
		&repo.WorkspaceID,
		&repo.Name,
		&repo.Description,
		&repo.PolicyIDs,
		&repo.OwnerID,
		&repo.Status,
		&repo.Created,
		&repo.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("repo not found")
		}
		s.logger.Errorf("Failed to get repo: %v", err)
		return nil, err
	}

	return &repo, nil
}

// GetByName retrieves a repo by name
func (s *Service) GetByName(ctx context.Context, tenantID, workspaceID, repoName string) (*Repo, error) {
	s.logger.Infof("Retrieving repo from database with name: %s", repoName)
	query := `
		SELECT repo_id, tenant_id, workspace_id, repo_name, repo_description, COALESCE(policy_ids, '{}') as policy_ids, owner_id, status, created, updated
		FROM repos
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_name = $3
	`

	var repo Repo
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoName).Scan(
		&repo.ID,
		&repo.TenantID,
		&repo.WorkspaceID,
		&repo.Name,
		&repo.Description,
		&repo.PolicyIDs,
		&repo.OwnerID,
		&repo.Status,
		&repo.Created,
		&repo.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("repo not found")
		}
		s.logger.Errorf("Failed to get repo by name: %v", err)
		return nil, err
	}

	return &repo, nil
}

// GetByNameWithBranches retrieves a repo by name with its branches and commits
func (s *Service) GetByNameWithBranches(ctx context.Context, tenantID, workspaceID, repoName string) (*FullRepo, error) {
	s.logger.Infof("Retrieving repo with branches and commits from database with name: %s", repoName)
	query := `
		SELECT repo_id, tenant_id, workspace_id, repo_name, repo_description, COALESCE(policy_ids, '{}') as policy_ids, owner_id, status, created, updated
		FROM repos
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_name = $3
	`

	var repo FullRepo
	err := s.db.Pool().QueryRow(ctx, query, tenantID, workspaceID, repoName).Scan(
		&repo.ID,
		&repo.TenantID,
		&repo.WorkspaceID,
		&repo.Name,
		&repo.Description,
		&repo.PolicyIDs,
		&repo.OwnerID,
		&repo.Status,
		&repo.Created,
		&repo.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("repo not found")
		}
		s.logger.Errorf("Failed to get repo by name: %v", err)
		return nil, err
	}

	// Get branches for the repo
	branches, err := s.GetBranchesWithCommits(ctx, tenantID, workspaceID, repo.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get branches: %w", err)
	}
	repo.Branches = branches

	return &repo, nil
}

// GetBranches retrieves all branches for a repo
func (s *Service) GetBranches(ctx context.Context, tenantID, workspaceID, repoID string) ([]*Branch, error) {
	s.logger.Infof("Retrieving branches for repo: %s", repoID)
	query := `
		SELECT branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, status, created, updated
		FROM branches
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID, repoID)
	if err != nil {
		s.logger.Errorf("Failed to get branches: %v", err)
		return nil, err
	}
	defer rows.Close()

	var branches []*Branch
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
			s.logger.Errorf("Failed to scan branch: %v", err)
			return nil, err
		}
		branches = append(branches, &branch)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning branches: %v", err)
		return nil, err
	}

	return branches, nil
}

// GetBranchesWithCommits retrieves all branches for a repo with their commits
func (s *Service) GetBranchesWithCommits(ctx context.Context, tenantID, workspaceID, repoID string) ([]*Branch, error) {
	s.logger.Infof("Retrieving branches with commits for repo: %s", repoID)
	query := `
		SELECT branch_id, tenant_id, workspace_id, repo_id, branch_name, parent_branch_id, connected_to_database, connected_database_id, COALESCE(policy_ids, '{}') as policy_ids, status, created, updated
		FROM branches
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3
		ORDER BY branch_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID, repoID)
	if err != nil {
		s.logger.Errorf("Failed to get branches: %v", err)
		return nil, err
	}
	defer rows.Close()

	var branches []*Branch
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
			s.logger.Errorf("Failed to scan branch: %v", err)
			return nil, err
		}
		branches = append(branches, &branch)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning branches: %v", err)
		return nil, err
	}

	// For each branch, get its commits
	for _, branch := range branches {
		commits, err := s.GetCommitsForBranch(ctx, tenantID, workspaceID, repoID, branch.ID)
		if err != nil {
			s.logger.Errorf("Failed to get commits for branch %s: %v", branch.ID, err)
			continue
		}
		branch.Commits = commits
	}

	return branches, nil
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

// List retrieves all repos for a workspace
func (s *Service) List(ctx context.Context, tenantID, workspaceID string) ([]*Repo, error) {
	s.logger.Infof("Listing repos for tenant: %s, workspace: %s", tenantID, workspaceID)
	query := `
		SELECT repo_id, tenant_id, workspace_id, repo_name, repo_description, COALESCE(policy_ids, '{}') as policy_ids, owner_id, status, created, updated
		FROM repos
		WHERE tenant_id = $1 AND workspace_id = $2
		ORDER BY repo_name
	`

	rows, err := s.db.Pool().Query(ctx, query, tenantID, workspaceID)
	if err != nil {
		s.logger.Errorf("Failed to list repos: %v", err)
		return nil, err
	}
	defer rows.Close()

	var repos []*Repo
	for rows.Next() {
		var repo Repo
		err := rows.Scan(
			&repo.ID,
			&repo.TenantID,
			&repo.WorkspaceID,
			&repo.Name,
			&repo.Description,
			&repo.PolicyIDs,
			&repo.OwnerID,
			&repo.Status,
			&repo.Created,
			&repo.Updated,
		)
		if err != nil {
			s.logger.Errorf("Failed to scan repo: %v", err)
			return nil, err
		}
		repos = append(repos, &repo)
	}

	if err = rows.Err(); err != nil {
		s.logger.Errorf("Error after scanning repos: %v", err)
		return nil, err
	}

	return repos, nil
}

// Update updates a repo
func (s *Service) Update(ctx context.Context, tenantID, workspaceID, repoName string, updates map[string]interface{}) (*Repo, error) {
	s.logger.Infof("Updating repo with name: %s", repoName)

	// Get repo by name
	repo, err := s.GetByName(ctx, tenantID, workspaceID, repoName)
	if err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return repo, nil
	}

	// Build dynamic update query
	setParts := []string{}
	args := []interface{}{tenantID, workspaceID, repo.ID}
	argIndex := 4

	for field, value := range updates {
		switch field {
		case "repo_name", "repo_description":
			setParts = append(setParts, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		default:
			s.logger.Warnf("Ignoring invalid update field: %s", field)
		}
	}

	if len(setParts) == 0 {
		return repo, nil
	}

	// Add updated timestamp
	setParts = append(setParts, "updated = CURRENT_TIMESTAMP")

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause += ", " + setParts[i]
	}

	query := fmt.Sprintf(`
		UPDATE repos 
		SET %s
		WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3
		RETURNING repo_id, tenant_id, workspace_id, repo_name, repo_description, COALESCE(policy_ids, '{}') as policy_ids, owner_id, status, created, updated
	`, setClause)

	var updatedRepo Repo
	err = s.db.Pool().QueryRow(ctx, query, args...).Scan(
		&updatedRepo.ID,
		&updatedRepo.TenantID,
		&updatedRepo.WorkspaceID,
		&updatedRepo.Name,
		&updatedRepo.Description,
		&updatedRepo.PolicyIDs,
		&updatedRepo.OwnerID,
		&updatedRepo.Status,
		&updatedRepo.Created,
		&updatedRepo.Updated,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("repo not found")
		}
		s.logger.Errorf("Failed to update repo: %v", err)
		return nil, err
	}

	return &updatedRepo, nil
}

// Clone creates a copy of an existing repo
func (s *Service) Clone(ctx context.Context, tenantID, workspaceID, sourceName, newName, newDescription, ownerID string) (*Repo, error) {
	s.logger.Infof("Cloning repo %s to %s", sourceName, newName)

	// Check if source repo exists
	_, err := s.GetByName(ctx, tenantID, workspaceID, sourceName)
	if err != nil {
		return nil, fmt.Errorf("source repo not found: %w", err)
	}

	// Create new repo
	return s.Create(ctx, tenantID, workspaceID, newName, newDescription, ownerID)
}

// Delete deletes a repo
func (s *Service) Delete(ctx context.Context, tenantID, workspaceID, repoName string, force bool) error {
	s.logger.Infof("Deleting repo with name: %s", repoName)

	// Get repo by name
	repo, err := s.GetByName(ctx, tenantID, workspaceID, repoName)
	if err != nil {
		return err
	}

	// Check for existing branches if not force delete
	if !force {
		var branchCount int
		err = s.db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM branches WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3",
			tenantID, workspaceID, repo.ID).Scan(&branchCount)
		if err != nil {
			return fmt.Errorf("failed to check for existing branches: %w", err)
		}
		if branchCount > 0 {
			return errors.New("cannot delete repo with existing branches (use force=true to override)")
		}
	}

	// Begin transaction for cascading deletes
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Delete commits first
	_, err = tx.Exec(ctx, "DELETE FROM commits WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3",
		tenantID, workspaceID, repo.ID)
	if err != nil {
		return fmt.Errorf("failed to delete commits: %w", err)
	}

	// Delete branches
	_, err = tx.Exec(ctx, "DELETE FROM branches WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3",
		tenantID, workspaceID, repo.ID)
	if err != nil {
		return fmt.Errorf("failed to delete branches: %w", err)
	}

	// Delete repo
	result, err := tx.Exec(ctx, "DELETE FROM repos WHERE tenant_id = $1 AND workspace_id = $2 AND repo_id = $3",
		tenantID, workspaceID, repo.ID)
	if err != nil {
		return fmt.Errorf("failed to delete repo: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("repo not found")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// FindRepoAndBranchByDatabaseID finds the repo and branch for a given database id
func (s *Service) FindRepoAndBranchByDatabaseID(ctx context.Context, databaseID string) (*RepoWithBranch, error) {
	s.logger.Infof("Finding repo and branch for database: %s", databaseID)

	// Query to find branch connected to the database and get the associated repo
	query := `
		SELECT b.repo_id, b.branch_id
		FROM branches b
		WHERE b.connected_database_id = $1 AND b.connected_to_database = true
		LIMIT 1
	`

	var repoID, branchID string
	err := s.db.Pool().QueryRow(ctx, query, databaseID).Scan(&repoID, &branchID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No branch found connected to this database
			return &RepoWithBranch{
				RepoID:   "",
				BranchID: "",
				Success:  false,
			}, nil
		}
		return nil, fmt.Errorf("failed to find repo and branch: %w", err)
	}

	return &RepoWithBranch{
		RepoID:   repoID,
		BranchID: branchID,
		Success:  true,
	}, nil
}
