package commits

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

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

type BranchCommitRequest struct {
	NewBranchName string `json:"new_branch_name"`
}

// parseRepoBranchCommit parses repo/branch/commit format and returns repo, branch, and commit names
func parseRepoBranchCommit(repoBranchCommitStr string) (repoName, branchName, commitCode string, err error) {
	parts := strings.Split(repoBranchCommitStr, "/")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid format. Expected repo/branch/commit")
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2]), nil
}

// ShowCommit displays details of a specific commit
func ShowCommit(repoBranchCommitStr string) error {
	repoName, branchName, commitCode, err := parseRepoBranchCommit(repoBranchCommitStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" || commitCode == "" {
		return fmt.Errorf("repository name, branch name, and commit code are required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s/commits/%s", repoName, branchName, commitCode))
	if err != nil {
		return err
	}

	var commitResponse struct {
		Commit Commit `json:"commit"`
	}
	if err := client.Get(url, &commitResponse); err != nil {
		return fmt.Errorf("failed to get commit details: %v", err)
	}

	commit := commitResponse.Commit
	fmt.Println()
	fmt.Printf("Commit Details for '%s' in branch '%s' of repository '%s'\n", commitCode, branchName, repoName)
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Commit ID:       %s\n", commit.CommitID)
	fmt.Printf("Commit Code:     %s\n", commit.CommitCode)
	fmt.Printf("Message:         %s\n", commit.CommitMessage)
	fmt.Printf("Is Head:         %t\n", commit.IsHead)
	fmt.Printf("Schema Type:     %s\n", commit.SchemaType)
	fmt.Printf("Commit Date:     %s\n", commit.CommitDate)
	fmt.Printf("Tenant ID:       %s\n", commit.TenantID)
	fmt.Printf("Workspace ID:    %s\n", commit.WorkspaceID)
	fmt.Printf("Repo ID:         %s\n", commit.RepoID)
	fmt.Printf("Branch ID:       %s\n", commit.BranchID)

	if commit.SchemaStructure != "" && commit.SchemaStructure != "{}" {
		fmt.Println("\nSchema Structure:")
		// Pretty print JSON
		var schemaInterface interface{}
		if err := json.Unmarshal([]byte(commit.SchemaStructure), &schemaInterface); err == nil {
			if prettyJSON, err := json.MarshalIndent(schemaInterface, "", "  "); err == nil {
				fmt.Println(string(prettyJSON))
			} else {
				fmt.Println(commit.SchemaStructure)
			}
		} else {
			fmt.Println(commit.SchemaStructure)
		}
	}

	fmt.Println()
	return nil
}

// BranchCommit creates a new branch from a commit
func BranchCommit(repoBranchCommitStr string, args []string) error {
	repoName, branchName, commitCode, err := parseRepoBranchCommit(repoBranchCommitStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" || commitCode == "" {
		return fmt.Errorf("repository name, branch name, and commit code are required")
	}

	reader := bufio.NewReader(os.Stdin)

	// Get new branch name
	var newBranchName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--new-branch=") {
		newBranchName = strings.TrimPrefix(args[0], "--new-branch=")
	} else {
		fmt.Print("New Branch Name: ")
		newBranchName, _ = reader.ReadString('\n')
		newBranchName = strings.TrimSpace(newBranchName)
	}

	if newBranchName == "" {
		return fmt.Errorf("new branch name is required")
	}

	// Create the branch request
	branchReq := BranchCommitRequest{
		NewBranchName: newBranchName,
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s/commits/%s/branch", repoName, branchName, commitCode))
	if err != nil {
		return err
	}

	var branchResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Commit  Commit `json:"commit"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, branchReq, &branchResponse); err != nil {
		return fmt.Errorf("failed to create branch from commit: %v", err)
	}

	fmt.Printf("Successfully created branch '%s' from commit '%s' in repository '%s'\n", newBranchName, commitCode, repoName)
	return nil
}

// MergeCommit merges a commit to the parent branch
func MergeCommit(repoBranchCommitStr string, _ []string) error {
	repoName, branchName, commitCode, err := parseRepoBranchCommit(repoBranchCommitStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" || commitCode == "" {
		return fmt.Errorf("repository name, branch name, and commit code are required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s/commits/%s/merge", repoName, branchName, commitCode))
	if err != nil {
		return err
	}

	var mergeResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Commit  Commit `json:"commit"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, nil, &mergeResponse); err != nil {
		return fmt.Errorf("failed to merge commit: %v", err)
	}

	fmt.Printf("Successfully merged commit '%s' from branch '%s' to parent branch in repository '%s'\n", commitCode, branchName, repoName)
	return nil
}

// DeployCommit deploys a commit to the database attached to the branch
func DeployCommit(repoBranchCommitStr string, _ []string) error {
	repoName, branchName, commitCode, err := parseRepoBranchCommit(repoBranchCommitStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" || commitCode == "" {
		return fmt.Errorf("repository name, branch name, and commit code are required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s/commits/%s/deploy", repoName, branchName, commitCode))
	if err != nil {
		return err
	}

	var deployResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Commit  Commit `json:"commit"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, nil, &deployResponse); err != nil {
		return fmt.Errorf("failed to deploy commit: %v", err)
	}

	fmt.Printf("Successfully deployed commit '%s' from branch '%s' in repository '%s' to the attached database\n", commitCode, branchName, repoName)
	return nil
}
