package workspaces

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
)

type Workspace struct {
	ID                string `json:"workspace_id"`
	Name              string `json:"workspace_name"`
	Description       string `json:"workspace_description"`
	OwnerID           string `json:"owner_id"`
	InstanceCount     int    `json:"instance_count"`
	DatabaseCount     int    `json:"database_count"`
	RepositoryCount   int    `json:"repository_count"`
	MappingCount      int    `json:"mapping_count"`
	RelationshipCount int    `json:"relationship_count"`
}

// Response wraps the API response for listing workspaces
type Response struct {
	Workspaces []Workspace `json:"workspaces"`
}

// WorkspaceResponse wraps the API response for a single workspace
type WorkspaceResponse struct {
	Workspace Workspace `json:"workspace"`
}

// CreateWorkspaceResponse wraps the API response for creating a workspace
type CreateWorkspaceResponse struct {
	Message   string    `json:"message"`
	Success   bool      `json:"success"`
	Workspace Workspace `json:"workspace"`
	Status    string    `json:"status"`
}

// UpdateWorkspaceResponse wraps the API response for updating a workspace
type UpdateWorkspaceResponse struct {
	Message   string    `json:"message"`
	Success   bool      `json:"success"`
	Workspace Workspace `json:"workspace"`
	Status    string    `json:"status"`
}

type CreateWorkspaceRequest struct {
	Name        string `json:"workspace_name"`
	Description string `json:"workspace_description,omitempty"`
	OwnerID     string `json:"owner_id,omitempty"`
}

type UpdateWorkspaceRequest struct {
	Name        string `json:"workspace_name,omitempty"`
	NameNew     string `json:"workspace_name_new,omitempty"`
	Description string `json:"workspace_description,omitempty"`
}

// ShowWorkspaces lists all workspaces
func ListWorkspaces() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces", tenantURL)

	var response Response
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get workspaces: %v", err)
	}

	workspaces := response.Workspaces

	if len(workspaces) == 0 {
		fmt.Println("No workspaces found")
		return nil
	}

	// Sort workspaces by name
	sort.Slice(workspaces, func(i, j int) bool {
		return workspaces[i].Name < workspaces[j].Name
	})

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()

	// Print header
	fmt.Fprintln(w, "Name\tDescription\tInstances\tDatabases\tRepositories\tMappings\tRelationships")
	fmt.Fprintln(w, "----\t-----------\t---------\t---------\t------------\t--------\t-------------")

	// Print each workspace
	for _, workspace := range workspaces {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%d\n",
			workspace.Name,
			workspace.Description,
			workspace.InstanceCount,
			workspace.DatabaseCount,
			workspace.RepositoryCount,
			workspace.MappingCount,
			workspace.RelationshipCount)
	}

	_ = w.Flush()
	fmt.Println()
	return nil
}

// ShowWorkspace displays details of a specific workspace
func ShowWorkspace(workspaceName string) error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s", tenantURL, workspaceName)

	var response WorkspaceResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get workspace: %v", err)
	}

	workspace := response.Workspace

	fmt.Println()
	fmt.Printf("Workspace Name: %s\n", workspace.Name)
	fmt.Printf("Description: %s\n", workspace.Description)
	fmt.Printf("Instances: %d\n", workspace.InstanceCount)
	fmt.Printf("Databases: %d\n", workspace.DatabaseCount)
	fmt.Printf("Repositories: %d\n", workspace.RepositoryCount)
	fmt.Printf("Mappings: %d\n", workspace.MappingCount)
	fmt.Printf("Relationships: %d\n", workspace.RelationshipCount)
	fmt.Println()
	return nil
}

// AddWorkspace creates a new workspace
func AddWorkspace(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get workspace name
	var workspaceName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		workspaceName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Workspace Name: ")
		workspaceName, _ = reader.ReadString('\n')
		workspaceName = strings.TrimSpace(workspaceName)
	}

	if workspaceName == "" {
		return fmt.Errorf("workspace name is required")
	}

	// Get optional fields
	var description string

	fmt.Print("Description (optional): ")
	description, _ = reader.ReadString('\n')
	description = strings.TrimSpace(description)

	// Create the workspace
	createReq := CreateWorkspaceRequest{
		Name:        workspaceName,
		Description: description,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces", tenantURL)

	var createResponse CreateWorkspaceResponse
	if err := client.Post(url, createReq, &createResponse, true); err != nil {
		return fmt.Errorf("failed to create workspace: %v", err)
	}

	fmt.Printf("Successfully created workspace '%s' (ID: %s)\n", createResponse.Workspace.Name, createResponse.Workspace.ID)
	return nil
}

// ModifyWorkspace updates an existing workspace
func ModifyWorkspace(workspaceName string, args []string) error {
	// First find the workspace to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s", tenantURL, workspaceName)

	fmt.Println()

	var response WorkspaceResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get workspace: %v", err)
	}

	targetWorkspace := response.Workspace

	reader := bufio.NewReader(os.Stdin)
	updateReq := UpdateWorkspaceRequest{}
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		if strings.HasPrefix(arg, "--name=") {
			updateReq.Name = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--description=") {
			updateReq.Description = strings.TrimPrefix(arg, "--description=")
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying workspace '%s' (press Enter to keep current value):\n", workspaceName)

		fmt.Printf("New Name [%s]: ", targetWorkspace.Name)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.NameNew = newName
			hasChanges = true
		}

		fmt.Printf("Description [%s]: ", targetWorkspace.Description)
		newDescription, _ := reader.ReadString('\n')
		newDescription = strings.TrimSpace(newDescription)
		if newDescription != "" {
			updateReq.Description = newDescription
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the workspace
	updateURL := fmt.Sprintf("%s/api/v1/workspaces/%s", tenantURL, workspaceName)

	var updateResponse UpdateWorkspaceResponse
	if err := client.Put(updateURL, updateReq, &updateResponse, true); err != nil {
		return fmt.Errorf("failed to update workspace: %v", err)
	}

	fmt.Printf("Successfully updated workspace '%s'\n", updateResponse.Workspace.Name)
	fmt.Println()
	return nil
}

// DeleteWorkspace removes an existing workspace
func DeleteWorkspace(workspaceName string, args []string) error {
	// Check for force flag
	force := false
	for _, arg := range args {
		if arg == "--force" || arg == "-f" {
			force = true
			break
		}
	}

	// First find the workspace to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Confirm deletion unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println()
		fmt.Printf("Are you sure you want to delete workspace '%s'? This action cannot be undone. (y/N): ", workspaceName)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			fmt.Println()
			return nil
		}
	}

	// Delete the workspace
	deleteURL := fmt.Sprintf("%s/api/v1/workspaces/%s", tenantURL, workspaceName)

	if err := client.Delete(deleteURL, true); err != nil {
		return fmt.Errorf("failed to delete workspace: %v", err)
	}

	fmt.Printf("Successfully deleted workspace '%s'\n", workspaceName)
	fmt.Println()
	return nil
}
