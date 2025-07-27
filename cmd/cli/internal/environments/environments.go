package environments

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
)

type Environment struct {
	ID                      string `json:"environment_id"`
	EnvironmentName         string `json:"environment_name"`
	EnvironmentDescription  string `json:"environment_description"`
	EnvironmentIsProduction bool   `json:"environment_is_production"`
	EnvironmentCriticality  int32  `json:"environment_criticality"`
	EnvironmentPriority     int32  `json:"environment_priority"`
	Status                  string `json:"status"`
	InstanceCount           int32  `json:"instance_count"`
	DatabaseCount           int32  `json:"database_count"`
	RepositoryCount         int32  `json:"repository_count"`
	MappingCount            int32  `json:"mapping_count"`
	RelationshipCount       int32  `json:"relationship_count"`
	Created                 string `json:"created"`
	Updated                 string `json:"updated"`
}

// EnvironmentsResponse wraps the API response for listing environments
type EnvironmentsResponse struct {
	Environments []Environment `json:"environments"`
}

// EnvironmentResponse wraps the API response for a single environment
type EnvironmentResponse struct {
	Environment Environment `json:"environment"`
}

// CreateEnvironmentResponse wraps the API response for creating an environment
type CreateEnvironmentResponse struct {
	Message     string      `json:"message"`
	Success     bool        `json:"success"`
	Environment Environment `json:"environment"`
	Status      string      `json:"status"`
}

// UpdateEnvironmentResponse wraps the API response for updating an environment
type UpdateEnvironmentResponse struct {
	Message     string      `json:"message"`
	Success     bool        `json:"success"`
	Environment Environment `json:"environment"`
	Status      string      `json:"status"`
}

type CreateEnvironmentRequest struct {
	Name        string `json:"environment_name"`
	Description string `json:"environment_description,omitempty"`
	Production  bool   `json:"environment_is_production,omitempty"`
	Criticality int    `json:"environment_criticality,omitempty"`
	Priority    int    `json:"environment_priority,omitempty"`
}

type UpdateEnvironmentRequest struct {
	Name        string `json:"environment_name,omitempty"`
	NameNew     string `json:"environment_name_new,omitempty"`
	Description string `json:"environment_description,omitempty"`
	Production  bool   `json:"environment_is_production,omitempty"`
	Criticality int    `json:"environment_criticality,omitempty"`
	Priority    int    `json:"environment_priority,omitempty"`
}

// ListEnvironments lists all environments
func ListEnvironments() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/environments", tenantURL, workspaceName)

	var response EnvironmentsResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get environments: %v", err)
	}

	environments := response.Environments

	if len(environments) == 0 {
		fmt.Println("No environments found")
		return nil
	}

	// Sort environments by name
	sort.Slice(environments, func(i, j int) bool {
		return environments[i].EnvironmentName < environments[j].EnvironmentName
	})

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()

	// Print header
	fmt.Fprintln(w, "Name\tDescription\tProduction\tCriticality\tPriority\tStatus\tInstances\tDatabases\tRepositories\tMappings\tRelationships")
	fmt.Fprintln(w, "----\t-----------\t----------\t-----------\t--------\t------\t---------\t---------\t------------\t--------\t-------------")

	// Print each environment
	for _, environment := range environments {
		fmt.Fprintf(w, "%s\t%s\t%t\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d\n",
			environment.EnvironmentName,
			environment.EnvironmentDescription,
			environment.EnvironmentIsProduction,
			environment.EnvironmentCriticality,
			environment.EnvironmentPriority,
			environment.Status,
			environment.InstanceCount,
			environment.DatabaseCount,
			environment.RepositoryCount,
			environment.MappingCount,
			environment.RelationshipCount)
	}

	w.Flush()
	fmt.Println()
	return nil
}

// ShowEnvironment displays details of a specific environment
func ShowEnvironment(environmentName string) error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/environments/%s", tenantURL, workspaceName, environmentName)

	var response EnvironmentResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get environment: %v", err)
	}

	environment := response.Environment

	fmt.Println()
	fmt.Printf("Environment Name: %s\n", environment.EnvironmentName)
	fmt.Printf("Description: %s\n", environment.EnvironmentDescription)
	fmt.Printf("Production: %t\n", environment.EnvironmentIsProduction)
	fmt.Printf("Criticality: %d\n", environment.EnvironmentCriticality)
	fmt.Printf("Priority: %d\n", environment.EnvironmentPriority)
	fmt.Printf("Status: %s\n", environment.Status)
	fmt.Printf("Instances: %d\n", environment.InstanceCount)
	fmt.Printf("Databases: %d\n", environment.DatabaseCount)
	fmt.Printf("Repositories: %d\n", environment.RepositoryCount)
	fmt.Printf("Mappings: %d\n", environment.MappingCount)
	fmt.Printf("Relationships: %d\n", environment.RelationshipCount)
	fmt.Println()
	return nil
}

// AddEnvironment creates a new environment
func AddEnvironment(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get environment name
	var environmentName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		environmentName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Environment Name: ")
		environmentName, _ = reader.ReadString('\n')
		environmentName = strings.TrimSpace(environmentName)
	}

	if environmentName == "" {
		return fmt.Errorf("environment name is required")
	}

	// Get environment production
	var environmentProduction string
	if len(args) > 1 && strings.HasPrefix(args[1], "--production=") {
		environmentProduction = strings.TrimPrefix(args[1], "--production=")
	} else {
		fmt.Print("Environment Production (true/false): ")
		environmentProduction, _ = reader.ReadString('\n')
		environmentProduction = strings.TrimSpace(environmentProduction)
	}

	if environmentProduction == "" {
		return fmt.Errorf("environment production is required")
	}

	// Validate environment production
	if environmentProduction != "true" && environmentProduction != "false" {
		return fmt.Errorf("invalid environment production. Must be one of: true, false")
	}

	// Get optional fields
	var description, criticality, priority string

	fmt.Print("Description (optional): ")
	description, _ = reader.ReadString('\n')
	description = strings.TrimSpace(description)

	fmt.Print("Criticality (optional): ")
	criticality, _ = reader.ReadString('\n')
	criticality = strings.TrimSpace(criticality)

	// Validate criticality
	criticalityInt, err := strconv.Atoi(criticality)
	if err != nil {
		return fmt.Errorf("invalid criticality. Must be an integer")
	}

	fmt.Print("Priority (optional): ")
	priority, _ = reader.ReadString('\n')
	priority = strings.TrimSpace(priority)

	// Validate priority
	priorityInt, err := strconv.Atoi(priority)
	if err != nil {
		return fmt.Errorf("invalid priority. Must be an integer")
	}

	// Create the environment
	createReq := CreateEnvironmentRequest{
		Name:        environmentName,
		Description: description,
		Production:  environmentProduction == "true",
		Criticality: criticalityInt,
		Priority:    priorityInt,
	}

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/environments", tenantURL, workspaceName)

	var createResponse CreateEnvironmentResponse
	if err := client.Post(url, createReq, &createResponse, true); err != nil {
		return fmt.Errorf("failed to create environment: %v", err)
	}

	fmt.Printf("Successfully created environment '%s' (ID: %s)\n", createResponse.Environment.EnvironmentName, createResponse.Environment.ID)
	return nil
}

// ModifyEnvironment updates an existing environment
func ModifyEnvironment(environmentName string, args []string) error {
	// First find the environment to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces/%s/environments/%s", tenantURL, workspaceName, environmentName)

	fmt.Println()

	var response EnvironmentResponse
	if err := client.Get(url, &response, true); err != nil {
		return fmt.Errorf("failed to get environment: %v", err)
	}

	targetEnvironment := response.Environment

	reader := bufio.NewReader(os.Stdin)
	updateReq := UpdateEnvironmentRequest{}
	hasChanges := false

	// Parse command line arguments or prompt for input
	for _, arg := range args {
		if strings.HasPrefix(arg, "--name=") {
			updateReq.Name = strings.TrimPrefix(arg, "--name=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--description=") {
			updateReq.Description = strings.TrimPrefix(arg, "--description=")
			hasChanges = true
		} else if strings.HasPrefix(arg, "--production=") {
			updateReq.Production = strings.TrimPrefix(arg, "--production=") == "true"
			hasChanges = true
		} else if strings.HasPrefix(arg, "--criticality=") {
			criticalityInt, err := strconv.Atoi(strings.TrimPrefix(arg, "--criticality="))
			if err != nil {
				return fmt.Errorf("invalid criticality. Must be an integer")
			}
			updateReq.Criticality = criticalityInt
			hasChanges = true
		} else if strings.HasPrefix(arg, "--priority=") {
			priorityInt, err := strconv.Atoi(strings.TrimPrefix(arg, "--priority="))
			if err != nil {
				return fmt.Errorf("invalid priority. Must be an integer")
			}
			updateReq.Priority = priorityInt
			hasChanges = true
		}
	}

	// If no arguments provided, prompt for input
	if !hasChanges {
		fmt.Printf("Modifying environment '%s' (press Enter to keep current value):\n", environmentName)

		fmt.Printf("New Name [%s]: ", targetEnvironment.EnvironmentName)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.NameNew = newName
			hasChanges = true
		}

		fmt.Printf("Description [%s]: ", targetEnvironment.EnvironmentDescription)
		newDescription, _ := reader.ReadString('\n')
		newDescription = strings.TrimSpace(newDescription)
		if newDescription != "" {
			updateReq.Description = newDescription
			hasChanges = true
		}

		fmt.Printf("Production [%t]: ", targetEnvironment.EnvironmentIsProduction)
		newProduction, _ := reader.ReadString('\n')
		newProduction = strings.TrimSpace(newProduction)
		if newProduction != "" {
			updateReq.Production = newProduction == "true"
			hasChanges = true
		}

		fmt.Printf("Criticality [%d]: ", targetEnvironment.EnvironmentCriticality)
		newCriticality, _ := reader.ReadString('\n')
		newCriticality = strings.TrimSpace(newCriticality)
		if newCriticality != "" {
			criticalityInt, err := strconv.Atoi(newCriticality)
			if err != nil {
				return fmt.Errorf("invalid criticality. Must be an integer")
			}
			updateReq.Criticality = criticalityInt
			hasChanges = true
		}

		fmt.Printf("Priority [%d]: ", targetEnvironment.EnvironmentPriority)
		newPriority, _ := reader.ReadString('\n')
		newPriority = strings.TrimSpace(newPriority)
		if newPriority != "" {
			priorityInt, err := strconv.Atoi(newPriority)
			if err != nil {
				return fmt.Errorf("invalid priority. Must be an integer")
			}
			updateReq.Priority = priorityInt
			hasChanges = true
		}
	}

	if !hasChanges {
		fmt.Println("No changes made")
		return nil
	}

	// Update the environment
	updateURL := fmt.Sprintf("%s/api/v1/workspaces/%s/environments/%s", tenantURL, workspaceName, environmentName)

	var updateResponse UpdateEnvironmentResponse
	if err := client.Put(updateURL, updateReq, &updateResponse, true); err != nil {
		return fmt.Errorf("failed to update environment: %v", err)
	}

	fmt.Printf("Successfully updated environment '%s'\n", updateResponse.Environment.EnvironmentName)
	fmt.Println()
	return nil
}

// DeleteEnvironment deletes an existing environment
func DeleteEnvironment(environmentName string, args []string) error {
	// Check for force flag
	force := false
	for _, arg := range args {
		if arg == "--force" || arg == "-f" {
			force = true
			break
		}
	}

	// First find the environment to get its details
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	workspaceName, err := config.GetWorkspaceWithError(username)
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Confirm deletion unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println()
		fmt.Printf("Are you sure you want to delete environment '%s'? This action cannot be undone. (y/N): ", environmentName)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			fmt.Println()
			return nil
		}
	}

	// Delete the environment
	deleteURL := fmt.Sprintf("%s/api/v1/workspaces/%s/environments/%s", tenantURL, workspaceName, environmentName)

	if err := client.Delete(deleteURL, true); err != nil {
		return fmt.Errorf("failed to delete environment: %v", err)
	}

	fmt.Printf("Successfully deleted environment '%s'\n", environmentName)
	fmt.Println()
	return nil
}
