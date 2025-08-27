package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// SetupError represents a setup-specific error that should not show usage help
type SetupError struct {
	message string
}

func (e SetupError) Error() string {
	return e.message
}

// setupCmd represents the setup command
var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Create initial tenant, user, and workspace",
	Long: "Create the first tenant, user, and workspace for a new reDB installation. " +
		"This command is only available when no tenants exist in the system.",
	RunE: func(cmd *cobra.Command, args []string) error {
		err := performInitialSetup()
		// Check if it's a SetupError and suppress usage help
		var setupError SetupError
		if errors.As(err, &setupError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// InitialSetupRequest represents the request body for initial setup
type InitialSetupRequest struct {
	TenantName        string `json:"tenant_name"`
	TenantURL         string `json:"tenant_url"`
	TenantDescription string `json:"tenant_description"`
	UserEmail         string `json:"user_email"`
	UserPassword      string `json:"user_password"`
	WorkspaceName     string `json:"workspace_name"`
}

// InitialSetupResponse represents the response from initial setup
type InitialSetupResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Tenant  struct {
		TenantID          string `json:"tenant_id"`
		TenantName        string `json:"tenant_name"`
		TenantDescription string `json:"tenant_description"`
		TenantURL         string `json:"tenant_url"`
	} `json:"tenant"`
	Workspace struct {
		WorkspaceID          string `json:"workspace_id"`
		WorkspaceName        string `json:"workspace_name"`
		WorkspaceDescription string `json:"workspace_description"`
	} `json:"workspace"`
}

// readPassword reads a password from stdin without echoing characters
func readPassword(prompt string) (string, error) {
	fmt.Print(prompt)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // Add newline after password input
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytePassword)), nil
}

// getClientAPIURLWithPrompt gets the client API URL, prompting for it if not stored
func getClientAPIURLWithPrompt() string {
	// Try to get the client API URL from stored configuration
	clientAPIURL := config.GetGlobalAPIURLNoAuth()
	if clientAPIURL != "" {
		return clientAPIURL
	}

	// If not stored, prompt the user
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Client API URL (default: http://localhost:8080): ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input == "" {
		input = "http://localhost:8080"
	}

	// Ensure protocol is included
	if !strings.HasPrefix(input, "http://") && !strings.HasPrefix(input, "https://") {
		input = "http://" + input
	}

	return input
}

// toURLSafe converts a string to a URL-safe slug
func toURLSafe(input string) string {
	// Convert to lowercase
	input = strings.ToLower(input)

	// Replace spaces and common separators with hyphens
	input = strings.ReplaceAll(input, " ", "-")
	input = strings.ReplaceAll(input, "_", "-")
	input = strings.ReplaceAll(input, ".", "-")

	// Remove any characters that aren't alphanumeric or hyphens
	var result strings.Builder
	for _, r := range input {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			result.WriteRune(r)
		}
	}

	// Remove leading and trailing hyphens
	resultStr := strings.Trim(result.String(), "-")

	// Ensure it's not empty
	if resultStr == "" {
		resultStr = "tenant"
	}

	return resultStr
}

// performInitialSetup handles the initial setup process
func performInitialSetup() error {
	fmt.Println("reDB Initial Setup")
	fmt.Println("==================")
	fmt.Println("This will create the first tenant, user, and workspace for your reDB installation.")
	fmt.Println("This command is only available when no tenants exist in the system.")
	fmt.Println()

	// Get Client API URL - prompt if not stored
	clientAPIURL := getClientAPIURLWithPrompt()

	// Check if setup is already done by trying to list tenants
	client := httpclient.GetClient()
	var tenantsResponse struct {
		Tenants []interface{} `json:"tenants"`
	}

	err := client.Get(fmt.Sprintf("%s/api/v1/tenants", clientAPIURL), &tenantsResponse, false)
	if err == nil && len(tenantsResponse.Tenants) > 0 {
		return SetupError{message: "initial setup is not available: tenants already exist in the system"}
	}

	// Get input from user
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Tenant Name: ")
	tenantName, _ := reader.ReadString('\n')
	tenantName = strings.TrimSpace(tenantName)
	if tenantName == "" {
		return fmt.Errorf("tenant name is required")
	}

	// Generate default tenant URL from tenant name
	defaultTenantURL := toURLSafe(tenantName)
	fmt.Printf("Tenant URL (slug, default: '%s'): ", defaultTenantURL)
	tenantURL, _ := reader.ReadString('\n')
	tenantURL = strings.TrimSpace(tenantURL)
	if tenantURL == "" {
		tenantURL = defaultTenantURL
	}

	fmt.Print("Tenant Description (optional): ")
	tenantDescription, _ := reader.ReadString('\n')
	tenantDescription = strings.TrimSpace(tenantDescription)

	fmt.Print("Admin User Email: ")
	userEmail, _ := reader.ReadString('\n')
	userEmail = strings.TrimSpace(userEmail)
	if userEmail == "" {
		return fmt.Errorf("user email is required")
	}

	// Read password with confirmation
	var userPassword string
	for {
		password, err := readPassword("Admin User Password: ")
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
		if password == "" {
			fmt.Println("Password cannot be empty. Please try again.")
			continue
		}

		confirmPassword, err := readPassword("Confirm Admin User Password: ")
		if err != nil {
			return fmt.Errorf("failed to read password confirmation: %v", err)
		}

		if password == confirmPassword {
			userPassword = password
			break
		} else {
			fmt.Println("Passwords do not match. Please try again.")
		}
	}

	fmt.Print("Workspace Name (default: 'default'): ")
	workspaceName, _ := reader.ReadString('\n')
	workspaceName = strings.TrimSpace(workspaceName)
	if workspaceName == "" {
		workspaceName = "default"
	}

	// Create the setup request
	setupReq := InitialSetupRequest{
		TenantName:        tenantName,
		TenantURL:         tenantURL,
		TenantDescription: tenantDescription,
		UserEmail:         userEmail,
		UserPassword:      userPassword,
		WorkspaceName:     workspaceName,
	}

	// Make the API call
	var setupResp InitialSetupResponse
	url := fmt.Sprintf("%s/api/v1/setup", clientAPIURL)

	fmt.Println()
	fmt.Println("Creating initial setup...")

	if err := client.Post(url, setupReq, &setupResp, false); err != nil {
		return fmt.Errorf("initial setup failed: %v", err)
	}

	// Store the tenant URL and workspace in keyring for future use
	if err := config.StoreTenant(userEmail, tenantURL); err != nil {
		fmt.Printf("Warning: Failed to store tenant URL in keyring: %v\n", err)
	}
	if err := config.StoreWorkspace(userEmail, workspaceName); err != nil {
		fmt.Printf("Warning: Failed to store workspace in keyring: %v\n", err)
	}

	// Display success message
	fmt.Println()
	fmt.Println("✅ Initial setup completed successfully!")
	fmt.Println()
	fmt.Printf("Tenant: %s (%s)\n", setupResp.Tenant.TenantName, setupResp.Tenant.TenantURL)
	fmt.Printf("Admin User: %s\n", userEmail)
	fmt.Printf("Workspace: %s\n", setupResp.Workspace.WorkspaceName)
	fmt.Println()
	fmt.Println("You can now login using:")
	fmt.Printf("  redb-cli auth login --username=%s --tenant=%s\n", userEmail, tenantURL)
	fmt.Println()
	fmt.Println("Or login interactively:")
	fmt.Println("  redb-cli auth login")

	return nil
}

func init() {
	// Add setup command to root
	rootCmd.AddCommand(setupCmd)
}
