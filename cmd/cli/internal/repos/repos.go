package repos

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
)

type Repo struct {
	TenantID        string `json:"tenant_id"`
	WorkspaceID     string `json:"workspace_id"`
	RepoID          string `json:"repo_id"`
	RepoName        string `json:"repo_name"`
	RepoDescription string `json:"repo_description"`
	OwnerID         string `json:"owner_id"`
}

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

type FullRepo struct {
	TenantID        string   `json:"tenant_id"`
	WorkspaceID     string   `json:"workspace_id"`
	RepoID          string   `json:"repo_id"`
	RepoName        string   `json:"repo_name"`
	RepoDescription string   `json:"repo_description"`
	OwnerID         string   `json:"owner_id"`
	Branches        []Branch `json:"branches"`
}

type AddRepoRequest struct {
	RepoName        string `json:"repo_name"`
	RepoDescription string `json:"repo_description"`
}

type ModifyRepoRequest struct {
	RepoNameNew     string `json:"repo_name_new"`
	RepoDescription string `json:"repo_description"`
}

type CloneRepoRequest struct {
	CloneRepoName        string `json:"clone_repo_name"`
	CloneRepoDescription string `json:"clone_repo_description"`
}

type DeleteRepoRequest struct {
	Force bool `json:"force"`
}

// ListRepos lists all repositories using profile-based authentication
func ListRepos() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/repos")
	if err != nil {
		return err
	}

	var reposResponse struct {
		Repos []Repo `json:"repos"`
	}
	if err := client.Get(url, &reposResponse); err != nil {
		return fmt.Errorf("failed to list repos: %v", err)
	}

	if len(reposResponse.Repos) == 0 {
		fmt.Println("No repositories found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Println()
	fmt.Fprintln(w, "Name\tDescription\tOwner ID")
	fmt.Fprintln(w, "----\t-----------\t--------")
	for _, repo := range reposResponse.Repos {
		fmt.Fprintf(w, "%s\t%s\t%s\n",
			repo.RepoName,
			repo.RepoDescription,
			repo.OwnerID)
	}
	_ = w.Flush()
	fmt.Println()
	return nil
}

// ShowRepo displays details of a specific repository
func ShowRepo(repoName string) error {
	repoName = strings.TrimSpace(repoName)
	if repoName == "" {
		return fmt.Errorf("repository name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s", repoName))
	if err != nil {
		return err
	}

	var repoResponse struct {
		Repo FullRepo `json:"repo"`
	}
	if err := client.Get(url, &repoResponse); err != nil {
		return fmt.Errorf("failed to get repository details: %v", err)
	}

	repo := repoResponse.Repo
	fmt.Println()
	fmt.Printf("Repository Details for '%s'\n", repo.RepoName)
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("ID:          %s\n", repo.RepoID)
	fmt.Printf("Name:        %s\n", repo.RepoName)
	fmt.Printf("Description: %s\n", repo.RepoDescription)
	fmt.Printf("Owner ID:    %s\n", repo.OwnerID)
	fmt.Printf("Tenant ID:   %s\n", repo.TenantID)
	fmt.Printf("Workspace ID: %s\n", repo.WorkspaceID)

	if len(repo.Branches) > 0 {
		fmt.Println("\nBranches:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "Name\tConnected to DB\tDatabase ID\tStatus")
		fmt.Fprintln(w, "----\t---------------\t-----------\t------")
		for _, branch := range repo.Branches {
			connected := "No"
			if branch.ConnectedToDatabase {
				connected = "Yes"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				branch.BranchName,
				connected,
				branch.DatabaseID,
				branch.Status)
		}
		_ = w.Flush()
	}

	fmt.Println()
	return nil
}

// AddRepo creates a new repository
func AddRepo(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get repository name
	var repoName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		repoName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Repository Name: ")
		repoName, _ = reader.ReadString('\n')
		repoName = strings.TrimSpace(repoName)
	}

	if repoName == "" {
		return fmt.Errorf("repository name is required")
	}

	// Get repository description
	var repoDescription string
	if len(args) > 1 && strings.HasPrefix(args[1], "--description=") {
		repoDescription = strings.TrimPrefix(args[1], "--description=")
	} else {
		fmt.Print("Repository Description: ")
		repoDescription, _ = reader.ReadString('\n')
		repoDescription = strings.TrimSpace(repoDescription)
	}

	if repoDescription == "" {
		return fmt.Errorf("repository description is required")
	}

	// Create the repository request
	createReq := AddRepoRequest{
		RepoName:        repoName,
		RepoDescription: repoDescription,
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, "/repos")
	if err != nil {
		return err
	}

	var createResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Repo    Repo   `json:"repo"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, createReq, &createResponse); err != nil {
		return fmt.Errorf("failed to create repository: %v", err)
	}

	fmt.Printf("Successfully created repository '%s' (ID: %s)\n", createResponse.Repo.RepoName, createResponse.Repo.RepoID)
	return nil
}

// ModifyRepo modifies an existing repository
func ModifyRepo(repoName string, args []string) error {
	repoName = strings.TrimSpace(repoName)
	if repoName == "" {
		return fmt.Errorf("repository name is required")
	}

	// First get the repository to show current values
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s", repoName))
	if err != nil {
		return err
	}

	var response struct {
		Repo FullRepo `json:"repo"`
	}
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get repository: %v", err)
	}

	targetRepo := response.Repo
	reader := bufio.NewReader(os.Stdin)
	updateReq := ModifyRepoRequest{}
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		if strings.HasPrefix(arg, "--name=") {
			updateReq.RepoNameNew = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--description=") {
			updateReq.RepoDescription = strings.TrimPrefix(arg, "--description=")
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying repository '%s' (press Enter to keep current value):\n", repoName)

		fmt.Printf("Name [%s]: ", targetRepo.RepoName)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.RepoNameNew = newName
			hasChanges = true
		}

		fmt.Printf("Description [%s]: ", targetRepo.RepoDescription)
		newDescription, _ := reader.ReadString('\n')
		newDescription = strings.TrimSpace(newDescription)
		if newDescription != "" {
			updateReq.RepoDescription = newDescription
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the repository
	updateURL, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s", repoName))
	if err != nil {
		return err
	}

	var updateResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Repo    Repo   `json:"repo"`
		Status  string `json:"status"`
	}
	if err := client.Put(updateURL, updateReq, &updateResponse); err != nil {
		return fmt.Errorf("failed to update repository: %v", err)
	}

	fmt.Printf("Successfully updated repository '%s'\n", updateResponse.Repo.RepoName)
	return nil
}

// CloneRepo creates a clone of an existing repository
func CloneRepo(repoName string, args []string) error {
	repoName = strings.TrimSpace(repoName)
	if repoName == "" {
		return fmt.Errorf("repository name is required")
	}

	reader := bufio.NewReader(os.Stdin)

	// Get clone repository name
	var cloneRepoName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--clone-name=") {
		cloneRepoName = strings.TrimPrefix(args[0], "--clone-name=")
	} else {
		fmt.Print("Clone Repository Name: ")
		cloneRepoName, _ = reader.ReadString('\n')
		cloneRepoName = strings.TrimSpace(cloneRepoName)
	}

	if cloneRepoName == "" {
		return fmt.Errorf("clone repository name is required")
	}

	// Get clone repository description
	var cloneRepoDescription string
	if len(args) > 1 && strings.HasPrefix(args[1], "--clone-description=") {
		cloneRepoDescription = strings.TrimPrefix(args[1], "--clone-description=")
	} else {
		fmt.Print("Clone Repository Description (optional): ")
		cloneRepoDescription, _ = reader.ReadString('\n')
		cloneRepoDescription = strings.TrimSpace(cloneRepoDescription)
	}

	// Create the clone request
	cloneReq := CloneRepoRequest{
		CloneRepoName:        cloneRepoName,
		CloneRepoDescription: cloneRepoDescription,
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s/clone", repoName))
	if err != nil {
		return err
	}

	var cloneResponse struct {
		Message string `json:"message"`
		Success bool   `json:"success"`
		Repo    Repo   `json:"repo"`
		Status  string `json:"status"`
	}
	if err := client.Post(url, cloneReq, &cloneResponse); err != nil {
		return fmt.Errorf("failed to clone repository: %v", err)
	}

	fmt.Printf("Successfully cloned repository '%s' to '%s' (ID: %s)\n", repoName, cloneRepoName, cloneResponse.Repo.RepoID)
	return nil
}

// DeleteRepo deletes a repository
func DeleteRepo(repoName string, args []string) error {
	repoName = strings.TrimSpace(repoName)
	if repoName == "" {
		return fmt.Errorf("repository name is required")
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
		fmt.Printf("Are you sure you want to delete repository '%s'? This action cannot be undone. (y/N): ", repoName)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			return nil
		}
	}

	// Delete the repository
	deleteURL, err := common.BuildWorkspaceAPIURL(profileInfo, fmt.Sprintf("/repos/%s", repoName))
	if err != nil {
		return err
	}

	if err := client.Delete(deleteURL); err != nil {
		return fmt.Errorf("failed to delete repository: %v", err)
	}

	fmt.Printf("Successfully deleted repository '%s'\n", repoName)
	return nil
}
