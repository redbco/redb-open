package tenants

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
	"golang.org/x/term"
)

type Tenant struct {
	ID          string `json:"tenant_id"`
	Name        string `json:"tenant_name"`
	Description string `json:"tenant_description"`
	URL         string `json:"tenant_url"`
}

// Response wraps the API response for listing tenants
type Response struct {
	Tenants []Tenant `json:"tenants"`
}

// TenantResponse wraps the API response for a single tenant
type TenantResponse struct {
	Tenant Tenant `json:"tenant"`
}

// CreateTenantResponse wraps the API response for creating a tenant
type CreateTenantResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Tenant  Tenant `json:"tenant"`
	Status  string `json:"status"`
}

// UpdateTenantResponse wraps the API response for updating a tenant
type UpdateTenantResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Tenant  Tenant `json:"tenant"`
	Status  string `json:"status"`
}

type CreateTenantRequest struct {
	Name         string `json:"tenant_name"`
	Description  string `json:"tenant_description,omitempty"`
	URL          string `json:"tenant_url,omitempty"`
	UserEmail    string `json:"user_email,omitempty"`
	UserPassword string `json:"user_password,omitempty"`
}

type UpdateTenantRequest struct {
	Name        string `json:"tenant_name,omitempty"`
	Description string `json:"tenant_description,omitempty"`
}

// ListTenants lists all tenants
func ListTenants() error {
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/tenants", serviceURL)

	var response Response
	if err := client.Get(url, &response, false); err != nil {
		return fmt.Errorf("failed to get tenants: %v", err)
	}

	tenants := response.Tenants

	if len(tenants) == 0 {
		fmt.Println("No tenants found")
		return nil
	}

	// Sort tenants by name
	sort.Slice(tenants, func(i, j int) bool {
		return tenants[i].Name < tenants[j].Name
	})

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()

	// Print header
	fmt.Fprintln(w, "ID\tName\tDescription\tURL")
	fmt.Fprintln(w, "--\t----\t-----------\t---")

	// Print each tenant
	for _, tenant := range tenants {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			tenant.ID,
			tenant.Name,
			tenant.Description,
			tenant.URL)
	}

	_ = w.Flush()
	fmt.Println()
	return nil
}

// ShowTenant displays details of a specific tenant
func ShowTenant(tenantID string) error {
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/tenants/%s", serviceURL, tenantID)

	var response TenantResponse
	if err := client.Get(url, &response, false); err != nil {
		return fmt.Errorf("failed to get tenant: %v", err)
	}

	tenant := response.Tenant

	fmt.Println()
	fmt.Printf("Tenant ID: %s\n", tenant.ID)
	fmt.Printf("Name: %s\n", tenant.Name)
	fmt.Printf("Description: %s\n", tenant.Description)
	fmt.Printf("URL: %s\n", tenant.URL)
	fmt.Println()
	return nil
}

// AddTenant creates a new tenant
func AddTenant(args []string) error {
	reader := bufio.NewReader(os.Stdin)

	// Get tenant name
	var tenantName string
	if len(args) > 0 && strings.HasPrefix(args[0], "--name=") {
		tenantName = strings.TrimPrefix(args[0], "--name=")
	} else {
		fmt.Print("Tenant Name: ")
		tenantName, _ = reader.ReadString('\n')
		tenantName = strings.TrimSpace(tenantName)
	}

	if tenantName == "" {
		return fmt.Errorf("tenant name is required")
	}

	var tenantURL string
	fmt.Print("URL (optional): ")
	tenantURL, _ = reader.ReadString('\n')
	tenantURL = strings.TrimSpace(tenantURL)

	// Get optional fields
	var description string
	var userEmail string
	var userPassword string

	fmt.Print("Description (optional): ")
	description, _ = reader.ReadString('\n')
	description = strings.TrimSpace(description)

	fmt.Print("User Email: ")
	userEmail, _ = reader.ReadString('\n')
	userEmail = strings.TrimSpace(userEmail)

	if userEmail != "" {
		fmt.Print("User Password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
		userPassword = string(passwordBytes)
		fmt.Println()
		userPassword = strings.TrimSpace(userPassword)
	}

	if userEmail == "" {
		return fmt.Errorf("user email is required")
	}

	if userPassword == "" {
		return fmt.Errorf("user password is required")
	}

	// Create the tenant
	createReq := CreateTenantRequest{
		Name:         tenantName,
		Description:  description,
		UserEmail:    userEmail,
		UserPassword: userPassword,
		URL:          tenantURL,
	}

	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/tenants", serviceURL)

	var createResponse CreateTenantResponse
	if err := client.Post(url, createReq, &createResponse, false); err != nil {
		return fmt.Errorf("failed to create tenant: %v", err)
	}

	fmt.Printf("Successfully created tenant '%s' (ID: %s)\n", createResponse.Tenant.Name, createResponse.Tenant.ID)
	return nil
}

// ModifyTenant updates an existing tenant
func ModifyTenant(tenantID string, args []string) error {
	// First find the tenant to get its details
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/tenants/%s", serviceURL, tenantID)

	fmt.Println()

	var response TenantResponse
	if err := client.Get(url, &response, false); err != nil {
		return fmt.Errorf("failed to get tenant: %v", err)
	}

	targetTenant := response.Tenant

	reader := bufio.NewReader(os.Stdin)
	updateReq := UpdateTenantRequest{}
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
		fmt.Printf("Modifying tenant '%s' (press Enter to keep current value):\n", tenantID)

		fmt.Printf("New Name [%s]: ", targetTenant.Name)
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if newName != "" {
			updateReq.Name = newName
			hasChanges = true
		}

		fmt.Printf("Description [%s]: ", targetTenant.Description)
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

	// Update the tenant
	updateURL := fmt.Sprintf("%s/api/v1/tenants/%s", serviceURL, tenantID)

	var updateResponse UpdateTenantResponse
	if err := client.Put(updateURL, updateReq, &updateResponse, false); err != nil {
		return fmt.Errorf("failed to update tenant: %v", err)
	}

	fmt.Printf("Successfully updated tenant '%s'\n", updateResponse.Tenant.Name)
	fmt.Println()
	return nil
}

// DeleteTenant removes an existing tenant
func DeleteTenant(tenantID string, args []string) error {
	// Check for force flag
	force := false
	for _, arg := range args {
		if arg == "--force" || arg == "-f" {
			force = true
			break
		}
	}

	// First find the tenant to get its details
	serviceURL, err := config.GetServiceAPIURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()

	// Confirm deletion unless force flag is used
	if !force {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println()
		fmt.Printf("Are you sure you want to delete tenant '%s'? This action cannot be undone. (y/N): ", tenantID)
		confirmation, _ := reader.ReadString('\n')
		confirmation = strings.TrimSpace(strings.ToLower(confirmation))

		if confirmation != "y" && confirmation != "yes" {
			fmt.Println("Operation cancelled")
			fmt.Println()
			return nil
		}
	}

	// Delete the tenant
	deleteURL := fmt.Sprintf("%s/api/v1/tenants/%s", serviceURL, tenantID)

	if err := client.Delete(deleteURL, false); err != nil {
		return fmt.Errorf("failed to delete tenant: %v", err)
	}

	fmt.Printf("Successfully deleted tenant '%s'\n", tenantID)
	fmt.Println()
	return nil
}
