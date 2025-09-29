package branches

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

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
	Status              string   `json:"status"`
}

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

type AttachBranchRequest struct {
	DatabaseName string `json:"database_name"`
}

type ModifyBranchRequest struct {
	BranchNameNew string `json:"branch_name_new"`
}

type DeleteBranchRequest struct {
	Force bool `json:"force"`
}

// parseRepoBranch parses repo/branch format and returns repo and branch names
func parseRepoBranch(repoBranchStr string) (repoName, branchName string, err error) {
	parts := strings.Split(repoBranchStr, "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid format. Expected repo/branch")
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

// ShowBranch displays details of a specific branch
func ShowBranch(repoBranchStr string) error {
	repoName, branchName, err := parseRepoBranch(repoBranchStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" {
		return fmt.Errorf("repository name and branch name are required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s", repoName, branchName))
	if err != nil {
		return err
	}

	var branchResponse struct {
		Branch Branch `json:"branch"`
	}
	if err := client.Get(url, &branchResponse); err != nil {
		return fmt.Errorf("failed to get branch details: %v", err)
	}

	branch := branchResponse.Branch
	fmt.Println()
	fmt.Printf("Branch Details for '%s' in repository '%s'\n", branchName, repoName)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Branch ID:           %s\n", branch.BranchID)
	fmt.Printf("Branch Name:         %s\n", branch.BranchName)
	fmt.Printf("Parent Branch ID:    %s\n", branch.ParentBranchID)
	fmt.Printf("Parent Branch Name:  %s\n", branch.ParentBranchName)
	fmt.Printf("Connected to DB:     %t\n", branch.ConnectedToDatabase)
	fmt.Printf("Database ID:         %s\n", branch.DatabaseID)
	fmt.Printf("Status:              %s\n", branch.Status)
	fmt.Printf("Tenant ID:           %s\n", branch.TenantID)
	fmt.Printf("Workspace ID:        %s\n", branch.WorkspaceID)
	fmt.Printf("Repo ID:             %s\n", branch.RepoID)

	if len(branch.Commits) > 0 {
		fmt.Println("\nCommits:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "Code\tMessage\tHead\tDate")
		fmt.Fprintln(w, "----\t-------\t----\t----")
		for _, commit := range branch.Commits {
			head := "No"
			if commit.IsHead {
				head = "Yes"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				commit.CommitCode,
				commit.CommitMessage,
				head,
				commit.CommitDate)
		}
		_ = w.Flush()
	}

	if len(branch.Branches) > 0 {
		fmt.Println("\nChild Branches:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "Name\tConnected to DB\tDatabase ID\tStatus")
		fmt.Fprintln(w, "----\t---------------\t-----------\t------")
		for _, childBranch := range branch.Branches {
			connected := "No"
			if childBranch.ConnectedToDatabase {
				connected = "Yes"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				childBranch.BranchName,
				connected,
				childBranch.DatabaseID,
				childBranch.Status)
		}
		_ = w.Flush()
	}

	fmt.Println()
	return nil
}

// AttachBranch attaches a branch to a connected database
func AttachBranch(repoBranchStr string, args []string) error {
	repoName, branchName, err := parseRepoBranch(repoBranchStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" {
		return fmt.Errorf("repository name and branch name are required")
	}

	reader := bufio.NewReader(os.Stdin)

	// Get database name
	var databaseName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--database=") {
		databaseName = strings.TrimPrefix(args[0], "--database=")
	} else {
		fmt.Print("Database Name: ")
		databaseName, _ = reader.ReadString('\n')
		databaseName = strings.TrimSpace(databaseName)
	}

	if databaseName == "" {
		return fmt.Errorf("database name is required")
	}

	// Create the attach request
	attachReq := AttachBranchRequest{
		DatabaseName: databaseName,
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s/attach", repoName, branchName))
	if err != nil {
		return err
	}

	var attachResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Branch  Branch `json:"branch"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, attachReq, &attachResponse); err != nil {
		return fmt.Errorf("failed to attach branch: %v", err)
	}

	fmt.Printf("Successfully attached branch '%s' in repository '%s' to database '%s'\n", branchName, repoName, databaseName)
	return nil
}

// DetachBranch detaches a branch from an attached database
func DetachBranch(repoBranchStr string, _ []string) error {
	repoName, branchName, err := parseRepoBranch(repoBranchStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" {
		return fmt.Errorf("repository name and branch name are required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s/detach", repoName, branchName))
	if err != nil {
		return err
	}

	var detachResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Branch  Branch `json:"branch"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, nil, &detachResponse); err != nil {
		return fmt.Errorf("failed to detach branch: %v", err)
	}

	fmt.Printf("Successfully detached branch '%s' in repository '%s' from database\n", branchName, repoName)
	return nil
}

// ModifyBranch modifies an existing branch
func ModifyBranch(repoBranchStr string, args []string) error {
	repoName, branchName, err := parseRepoBranch(repoBranchStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" {
		return fmt.Errorf("repository name and branch name are required")
	}

	// First get the branch to show current values
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s", repoName, branchName))
	if err != nil {
		return err
	}

	var response struct {
		Branch Branch `json:"branch"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get branch: %v", err)
	}

	targetBranch := response.Branch
	reader := bufio.NewReader(os.Stdin)
	updateReq := ModifyBranchRequest{}
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		if strings.HasPrefix(arg, "--name=") {
			updateReq.BranchNameNew = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying branch '%s' in repository '%s' (press Enter to keep current value):\n", branchName, repoName)

		fmt.Printf("Branch Name [%s]: ", targetBranch.BranchName)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.BranchNameNew = newName
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the branch
	updateURL, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s", repoName, branchName))
	if err != nil {
		return err
	}

	var updateResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Branch  Branch `json:"branch"`
		Status  string `json:"status"`
	}
	if err := client.Put(updateURL, updateReq, &updateResponse); err != nil {
		return fmt.Errorf("failed to update branch: %v", err)
	}

	fmt.Printf("Successfully updated branch '%s' in repository '%s'\n", branchName, repoName)
	return nil
}

// DeleteBranch deletes a branch
func DeleteBranch(repoBranchStr string, args []string) error {
	repoName, branchName, err := parseRepoBranch(repoBranchStr)
	if err != nil {
		return err
	}

	if repoName == "" || branchName == "" {
		return fmt.Errorf("repository name and branch name are required")
	}

	// Check for force flag
	force := false
	for _, arg := range args {
		if arg == "--force" || arg == "-f" {
			force = true
			break
		}
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	// Confirm deletion unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("Are you sure you want to delete branch '%s' in repository '%s'? This action cannot be undone. (y/N): ", branchName, repoName)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			return nil
		}
	}

	// Delete the branch
	deleteURL, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/branches/%s", repoName, branchName))
	if err != nil {
		return err
	}

	if err := client.Delete(deleteURL); err != nil {
		return fmt.Errorf("failed to delete branch: %v", err)
	}

	fmt.Printf("Successfully deleted branch '%s' in repository '%s'\n", branchName, repoName)
	return nil
}
