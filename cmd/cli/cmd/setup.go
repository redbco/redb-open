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
	Use:   "setup [--tenant-url=<tenant_url>]",
	Short: "Create initial user and workspace for single-tenant installation",
	Long: "Create the first user and workspace for a single-tenant reDB installation. " +
		"The tenant must already exist (created during initialization). " +
		"This command is only available when no users exist in the tenant. " +
		"Use --tenant-url to specify the tenant URL, or it will use the default tenant.",
	RunE: func(cmd *cobra.Command, args []string) error {
		tenantURL, _ := cmd.Flags().GetString("tenant-url")
		err := performInitialSetup(tenantURL)
		// Check if it's a SetupError and suppress usage help
		var setupError SetupError
		if errors.As(err, &setupError) {
			cmd.SilenceUsage = true
		}
		return err
	},
}

// InitialSetupRequest represents the request body for initial user setup
type InitialSetupRequest struct {
	UserEmail     string `json:"user_email"`
	UserPassword  string `json:"user_password"`
	WorkspaceName string `json:"workspace_name"`
}

// InitialSetupResponse represents the response from initial user setup
type InitialSetupResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	User    struct {
		UserID    string `json:"user_id"`
		UserName  string `json:"user_name"`
		UserEmail string `json:"user_email"`
	} `json:"user"`
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

// performInitialSetup handles the initial user setup process
func performInitialSetup(tenantURL string) error {
	fmt.Println("reDB Initial User Setup")
	fmt.Println("=======================")
	fmt.Println("This will create the first user and workspace for your single-tenant reDB installation.")
	fmt.Println("The tenant must already exist (created during initialization).")
	fmt.Println("This command is only available when no users exist in the tenant.")
	fmt.Println()

	// Get Client API URL - prompt if not stored
	clientAPIURL := getClientAPIURLWithPrompt()

	// If no tenant URL provided, use the default from config
	if tenantURL == "" {
		tenantURL = "default" // Default tenant URL from single-tenant mode
		fmt.Printf("Using default tenant URL: %s\n", tenantURL)
	} else {
		fmt.Printf("Using tenant URL: %s\n", tenantURL)
	}

	// Verify the tenant exists by trying to access it
	client := httpclient.GetClient()
	var tenantResponse struct {
		Tenant interface{} `json:"tenant"`
	}

	err := client.Get(fmt.Sprintf("%s/api/v1/tenants", clientAPIURL), &tenantResponse, false)
	if err != nil {
		return SetupError{message: fmt.Sprintf("failed to verify tenant exists: %v", err)}
	}

	// Get input from user
	reader := bufio.NewReader(os.Stdin)

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
		UserEmail:     userEmail,
		UserPassword:  userPassword,
		WorkspaceName: workspaceName,
	}

	// Make the API call to the new user setup endpoint
	var setupResp InitialSetupResponse
	url := fmt.Sprintf("%s/%s/api/v1/setup/user", clientAPIURL, tenantURL)

	fmt.Println()
	fmt.Println("Creating initial user and workspace...")

	if err := client.Post(url, setupReq, &setupResp, false); err != nil {
		return fmt.Errorf("initial user setup failed: %v", err)
	}

	// Note: With profile-based authentication, users should create profiles instead
	// This setup command creates the initial tenant/workspace but doesn't store credentials
	fmt.Println("\n📋 Next Steps:")
	fmt.Printf("1. Create a profile: redb-cli profiles create <name> --hostname %s --tenant-url %s\n", clientAPIURL, tenantURL)
	fmt.Printf("2. Login to the profile: redb-cli auth login --profile <name>\n")
	fmt.Printf("3. Select workspace: redb-cli select workspace %s\n", workspaceName)

	// Display success message
	fmt.Println()
	fmt.Println("✅ Initial user setup completed successfully!")
	fmt.Println()
	fmt.Printf("Tenant URL: %s\n", tenantURL)
	fmt.Printf("Admin User: %s (%s)\n", setupResp.User.UserEmail, setupResp.User.UserName)
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
	// Add flags
	setupCmd.Flags().String("tenant-url", "", "Tenant URL to create user in (optional, defaults to 'default')")
}
