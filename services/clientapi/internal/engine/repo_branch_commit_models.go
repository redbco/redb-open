package engine

// Repo represents a repository
type Repo struct {
	TenantID        string `json:"tenant_id"`
	WorkspaceID     string `json:"workspace_id"`
	RepoID          string `json:"repo_id"`
	RepoName        string `json:"repo_name"`
	RepoDescription string `json:"repo_description,omitempty"`
	OwnerID         string `json:"owner_id"`
}

type FullRepo struct {
	TenantID        string   `json:"tenant_id"`
	WorkspaceID     string   `json:"workspace_id"`
	RepoID          string   `json:"repo_id"`
	RepoName        string   `json:"repo_name"`
	RepoDescription string   `json:"repo_description,omitempty"`
	OwnerID         string   `json:"owner_id"`
	Branches        []Branch `json:"branches"`
}

type ListReposResponse struct {
	Repos []Repo `json:"repos"`
}

type ShowRepoResponse struct {
	Repo FullRepo `json:"repo"`
}

type AddRepoRequest struct {
	RepoName        string `json:"repo_name" validate:"required"`
	RepoDescription string `json:"repo_description" validate:"required"`
}

type AddRepoResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Repo    Repo   `json:"repo"`
	Status  Status `json:"status"`
}

type ModifyRepoRequest struct {
	RepoNameNew     string `json:"repo_name_new,omitempty"`
	RepoDescription string `json:"repo_description,omitempty"`
}

type ModifyRepoResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Repo    Repo   `json:"repo"`
	Status  Status `json:"status"`
}

type CloneRepoRequest struct {
	CloneRepoName        string `json:"clone_repo_name" validate:"required"`
	CloneRepoDescription string `json:"clone_repo_description,omitempty"`
}

type CloneRepoResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Repo    Repo   `json:"repo"`
	Status  Status `json:"status"`
}

type DeleteRepoRequest struct {
	Force bool `json:"force"`
}

type DeleteRepoResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// Branch models

// Branch represents a branch
type Branch struct {
	TenantID            string   `json:"tenant_id"`
	WorkspaceID         string   `json:"workspace_id"`
	RepoID              string   `json:"repo_id"`
	BranchID            string   `json:"branch_id"`
	BranchName          string   `json:"branch_name"`
	ParentBranchID      string   `json:"parent_branch_id"`
	ParentBranchName    string   `json:"parent_branch_name"`
	ConnectedToDatabase bool     `json:"connected_to_database"`
	DatabaseID          string   `json:"database_id"`
	Branches            []Branch `json:"branches"`
	Commits             []Commit `json:"commits"`
	Status              Status   `json:"status"`
}

type ShowBranchResponse struct {
	Branch Branch `json:"branch"`
}

type AttachBranchRequest struct {
	DatabaseName string `json:"database_name" validate:"required"`
}

type AttachBranchResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Branch  Branch `json:"branch"`
	Status  Status `json:"status"`
}

type DetachBranchResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Branch  Branch `json:"branch"`
	Status  Status `json:"status"`
}

type ModifyBranchRequest struct {
	BranchNameNew string `json:"branch_name_new,omitempty"`
}

type ModifyBranchResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Branch  Branch `json:"branch"`
	Status  Status `json:"status"`
}

type DeleteBranchRequest struct {
	Force bool `json:"force"`
}

type DeleteBranchResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  Status `json:"status"`
}

// Commit models

// Commit represents a commit
type Commit struct {
	TenantID        string `json:"tenant_id"`
	WorkspaceID     string `json:"workspace_id"`
	RepoID          string `json:"repo_id"`
	BranchID        string `json:"branch_id"`
	CommitID        string `json:"commit_id"`
	CommitCode      string `json:"commit_code"`
	IsHead          bool   `json:"is_head"`
	CommitMessage   string `json:"commit_message"`
	SchemaType      string `json:"schema_type"`
	SchemaStructure string `json:"schema_structure"`
	CommitDate      string `json:"commit_date"`
}

type ShowCommitResponse struct {
	Commit Commit `json:"commit"`
}

type BranchCommitRequest struct {
	NewBranchName string `json:"new_branch_name" validate:"required"`
}

type BranchCommitResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Commit  Commit `json:"commit"`
	Status  Status `json:"status"`
}

type MergeCommitResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Commit  Commit `json:"commit"`
	Status  Status `json:"status"`
}

type DeployCommitResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Commit  Commit `json:"commit"`
	Status  Status `json:"status"`
}
