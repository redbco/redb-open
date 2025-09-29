package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
	"github.com/redbco/redb-open/cmd/cli/internal/profile"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// profilesCmd represents the profiles command
var profilesCmd = &cobra.Command{
	Use:   "profiles",
	Short: "Manage connection profiles",
	Long:  "Manage connection profiles for different reDB systems",
}

// profilesListCmd lists all profiles
var profilesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all profiles",
	Long:  "List all connection profiles (without status checking)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return listProfiles()
	},
}

// profilesCreateCmd creates a new profile
var profilesCreateCmd = &cobra.Command{
	Use:   "create <name> [--tenant-url=<url>]",
	Short: "Create a new profile",
	Long:  "Create a new connection profile for a reDB system",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		tenantURL, _ := cmd.Flags().GetString("tenant-url")
		return createProfile(args[0], tenantURL)
	},
}

// profilesDeleteCmd deletes a profile
var profilesDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a profile",
	Long:  "Delete a connection profile",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return deleteProfile(args[0])
	},
}

// profilesShowCmd shows details of a profile
var profilesShowCmd = &cobra.Command{
	Use:   "show <name>",
	Short: "Show profile details",
	Long:  "Show detailed information about a connection profile",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return showProfile(args[0])
	},
}

// profilesStatusCmd checks the status of profiles
var profilesStatusCmd = &cobra.Command{
	Use:   "status [name]",
	Short: "Check profile status",
	Long:  "Check the real-time status of one or all profiles with live connectivity tests",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 {
			return checkProfileStatus(args[0])
		}
		return checkAllProfilesStatusWithList()
	},
}

// profilesActivateCmd activates a profile
var profilesActivateCmd = &cobra.Command{
	Use:   "activate <name>",
	Short: "Activate a profile",
	Long:  "Set a profile as the active profile for CLI operations",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return activateProfile(args[0])
	},
}

// profilesRefreshCmd refreshes tokens for a profile
var profilesRefreshCmd = &cobra.Command{
	Use:   "refresh <name>",
	Short: "Refresh tokens for a profile",
	Long:  "Refresh the access token for a profile using the refresh token",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return refreshProfile(args[0])
	},
}

func listProfiles() error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	profiles, err := pm.ListProfiles()
	if err != nil {
		return fmt.Errorf("failed to load profiles: %v", err)
	}

	if len(profiles) == 0 {
		fmt.Println("No profiles found. Use 'redb-cli profiles create <name>' to create one.")
		return nil
	}

	activeProfile, _ := pm.GetActiveProfile()

	fmt.Printf("%-15s %-25s %-15s %-20s %s\n", "NAME", "ENDPOINT", "TENANT", "USERNAME", "ACTIVE")
	fmt.Println(strings.Repeat("-", 85))

	for name, prof := range profiles {
		endpoint := fmt.Sprintf("%s:%d", prof.Hostname, prof.Port)

		activeStr := ""
		if name == activeProfile {
			activeStr = "✓"
		}

		fmt.Printf("%-15s %-25s %-15s %-20s %s\n",
			name, endpoint, prof.TenantURL, prof.Username, activeStr)
	}

	fmt.Println()
	fmt.Println("Use 'redb-cli profiles status' to check connectivity and node status.")

	return nil
}

func createProfile(name string, tenantURL string) error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)

	fmt.Printf("Creating profile: %s\n", name)
	fmt.Println()

	// Get hostname
	fmt.Print("Hostname (e.g., localhost, 192.168.1.100): ")
	hostname, _ := reader.ReadString('\n')
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return fmt.Errorf("hostname is required")
	}

	// Get port
	fmt.Print("Port (default: 8080): ")
	portStr, _ := reader.ReadString('\n')
	portStr = strings.TrimSpace(portStr)
	port := 8080
	if portStr != "" {
		if p, err := strconv.Atoi(portStr); err != nil {
			return fmt.Errorf("invalid port number: %v", err)
		} else {
			port = p
		}
	}

	// Set default tenant URL if not provided via flag
	if tenantURL == "" {
		tenantURL = "default"
	}

	// Create a temporary profile to check node status
	tempProfile := &profile.Profile{
		Name:      name,
		Hostname:  hostname,
		Port:      port,
		TenantURL: tenantURL,
	}

	// Check node status to guide the user
	fmt.Println()
	fmt.Print("Checking node status... ")

	checker := profile.NewStatusChecker()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodeStatus, description, statusErr := checker.CheckNodeStatus(ctx, tempProfile)

	// Try to detect node name from the endpoint
	nodeName := detectNodeName(tempProfile)
	if nodeName != "" {
		tempProfile.NodeName = nodeName
		fmt.Printf("(detected node: %s) ", nodeName)
	}

	if statusErr != nil {
		fmt.Printf("❌ Unable to connect to node\n")
		fmt.Printf("Error: %v\n", statusErr)
		fmt.Println()
		fmt.Println("The node may be down or the connection details may be incorrect.")
		fmt.Print("Do you want to create the profile anyway? (y/N): ")
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))
		if response != "y" && response != "yes" {
			fmt.Println("Profile creation cancelled.")
			return nil
		}

		// Get username for offline profile
		fmt.Print("Username (email): ")
		username, _ := reader.ReadString('\n')
		username = strings.TrimSpace(username)
		if username == "" {
			return fmt.Errorf("username is required")
		}
		tempProfile.Username = username
	} else {
		statusColor := profile.GetStatusColor(nodeStatus)
		fmt.Printf("%s✅ %s%s\n", statusColor, nodeStatus, profile.ResetColor())
		fmt.Printf("Description: %s\n", description)
		fmt.Println()

		// Handle different node states
		switch nodeStatus {
		case profile.NodeStatusNotInitialized:
			fmt.Println("🔧 This node has not been initialized yet.")
			fmt.Println("You'll need to run the initialization process on the node first.")
			fmt.Println("After initialization, you can create users with the setup command.")

			// Get username for future use
			fmt.Print("Username (email) for when the node is ready: ")
			username, _ := reader.ReadString('\n')
			username = strings.TrimSpace(username)
			if username == "" {
				return fmt.Errorf("username is required")
			}
			tempProfile.Username = username

		case profile.NodeStatusInitializedNoUsers:
			fmt.Println("🎯 Perfect! This node is ready for user setup.")
			fmt.Print("Would you like to create the first user now? (Y/n): ")
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(strings.ToLower(response))

			if response == "" || response == "y" || response == "yes" {
				// Integrated user setup flow
				username, workspace, err := performIntegratedUserSetup(tempProfile, reader)
				if err != nil {
					fmt.Printf("❌ User setup failed: %v\n", err)
					fmt.Println("Profile will be created without user setup.")

					// Get username for profile
					fmt.Print("Username (email): ")
					username, _ = reader.ReadString('\n')
					username = strings.TrimSpace(username)
					if username == "" {
						return fmt.Errorf("username is required")
					}
				} else {
					tempProfile.Workspace = workspace
				}
				tempProfile.Username = username
			} else {
				// Get username for profile
				fmt.Print("Username (email): ")
				username, _ := reader.ReadString('\n')
				username = strings.TrimSpace(username)
				if username == "" {
					return fmt.Errorf("username is required")
				}
				tempProfile.Username = username
			}

		case profile.NodeStatusReadyNoMesh, profile.NodeStatusReadyWithMesh:
			fmt.Println("✅ This node is ready and has users.")
			fmt.Print("Username (email): ")
			username, _ := reader.ReadString('\n')
			username = strings.TrimSpace(username)
			if username == "" {
				return fmt.Errorf("username is required")
			}
			tempProfile.Username = username

		default:
			// Fallback for unknown status
			fmt.Print("Username (email): ")
			username, _ := reader.ReadString('\n')
			username = strings.TrimSpace(username)
			if username == "" {
				return fmt.Errorf("username is required")
			}
			tempProfile.Username = username
		}
	}

	// Create the profile
	if err := pm.CreateProfile(tempProfile); err != nil {
		return fmt.Errorf("failed to create profile: %v", err)
	}

	// Update status in profile
	tempProfile.LastStatus = nodeStatus
	tempProfile.LastStatusTime = time.Now()
	if statusErr != nil {
		tempProfile.LastError = statusErr.Error()
	}
	pm.UpdateProfile(tempProfile)

	fmt.Println()
	fmt.Printf("✅ Profile '%s' created successfully!\n", name)
	fmt.Printf("Endpoint: %s\n", tempProfile.GetBaseURL())
	fmt.Printf("Tenant: %s\n", tenantURL)
	fmt.Printf("Username: %s\n", tempProfile.Username)
	if tempProfile.Workspace != "" {
		fmt.Printf("Workspace: %s\n", tempProfile.Workspace)
	}
	fmt.Println()

	// Provide next steps based on node status
	if statusErr != nil {
		fmt.Println("Next steps:")
		fmt.Println("1. Ensure the node is running and accessible")
		fmt.Printf("2. Use 'redb-cli profiles status %s' to check connectivity\n", name)
	} else {
		switch nodeStatus {
		case profile.NodeStatusNotInitialized:
			fmt.Println("Next steps:")
			fmt.Println("1. Initialize the node (run with --initialize flag)")
			fmt.Printf("2. Use 'redb-cli profiles status %s' to check progress\n", name)
		case profile.NodeStatusInitializedNoUsers:
			if tempProfile.Workspace == "" {
				fmt.Println("Next steps:")
				fmt.Printf("1. Use 'redb-cli auth login --profile %s' to create the first user\n", name)
			} else {
				fmt.Println("Next steps:")
				fmt.Printf("1. Use 'redb-cli auth login --profile %s' to login\n", name)
			}
		default:
			fmt.Println("Next steps:")
			fmt.Printf("1. Use 'redb-cli profiles activate %s' to make this the active profile\n", name)
			fmt.Printf("2. Use 'redb-cli auth login --profile %s' to login\n", name)
		}
	}

	return nil
}

// performIntegratedUserSetup handles user setup during profile creation
func performIntegratedUserSetup(prof *profile.Profile, reader *bufio.Reader) (string, string, error) {
	fmt.Println()
	fmt.Println("🚀 Setting up the first user...")
	fmt.Println()

	// Get user email
	fmt.Print("Admin User Email: ")
	userEmail, _ := reader.ReadString('\n')
	userEmail = strings.TrimSpace(userEmail)
	if userEmail == "" {
		return "", "", fmt.Errorf("user email is required")
	}

	// Get password with confirmation
	var userPassword string
	for {
		fmt.Print("Admin User Password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", "", fmt.Errorf("failed to read password: %v", err)
		}
		fmt.Println() // Add newline after password input
		password := strings.TrimSpace(string(passwordBytes))

		if password == "" {
			fmt.Println("Password cannot be empty. Please try again.")
			continue
		}

		fmt.Print("Confirm Admin User Password: ")
		confirmBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", "", fmt.Errorf("failed to read password confirmation: %v", err)
		}
		fmt.Println() // Add newline after password input
		confirmPassword := strings.TrimSpace(string(confirmBytes))

		if password == confirmPassword {
			userPassword = password
			break
		} else {
			fmt.Println("Passwords do not match. Please try again.")
		}
	}

	// Get workspace name
	fmt.Print("Workspace Name (default: 'default'): ")
	workspaceName, _ := reader.ReadString('\n')
	workspaceName = strings.TrimSpace(workspaceName)
	if workspaceName == "" {
		workspaceName = "default"
	}

	// Create the setup request
	setupReq := struct {
		UserEmail     string `json:"user_email"`
		UserPassword  string `json:"user_password"`
		WorkspaceName string `json:"workspace_name"`
	}{
		UserEmail:     userEmail,
		UserPassword:  userPassword,
		WorkspaceName: workspaceName,
	}

	// Make the API call
	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/setup/user", prof.GetTenantURL())

	fmt.Println()
	fmt.Print("Creating user and workspace... ")

	var setupResp struct {
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

	if err := client.Post(url, setupReq, &setupResp, false); err != nil {
		fmt.Printf("❌ Failed\n")
		return "", "", fmt.Errorf("user setup failed: %v", err)
	}

	fmt.Printf("✅ Success\n")
	fmt.Println()
	fmt.Printf("✅ User '%s' created successfully!\n", userEmail)
	fmt.Printf("✅ Workspace '%s' created successfully!\n", workspaceName)

	return userEmail, workspaceName, nil
}

// detectNodeName tries to detect the node name from the health endpoint
func detectNodeName(prof *profile.Profile) string {
	healthURL := fmt.Sprintf("%s/health", prof.GetBaseURL())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
	if err != nil {
		return ""
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return ""
	}

	var healthResp struct {
		Service  string `json:"service"`
		Status   string `json:"status"`
		NodeName string `json:"node_name,omitempty"`
		NodeID   string `json:"node_id,omitempty"`
		Hostname string `json:"hostname,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&healthResp); err != nil {
		return ""
	}

	// Try different fields that might contain a useful node identifier
	if healthResp.NodeName != "" {
		return healthResp.NodeName
	}
	if healthResp.NodeID != "" {
		return healthResp.NodeID
	}
	if healthResp.Hostname != "" {
		return healthResp.Hostname
	}

	// Fallback: generate a name based on hostname and service
	if healthResp.Service != "" {
		return fmt.Sprintf("%s-%s", healthResp.Service, prof.Hostname)
	}

	return ""
}

func deleteProfile(name string) error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	// Check if profile exists
	_, err = pm.GetProfile(name)
	if err != nil {
		return err
	}

	// Confirm deletion
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Are you sure you want to delete profile '%s'? (y/N): ", name)
	response, _ := reader.ReadString('\n')
	response = strings.TrimSpace(strings.ToLower(response))

	if response != "y" && response != "yes" {
		fmt.Println("Profile deletion cancelled.")
		return nil
	}

	if err := pm.DeleteProfile(name); err != nil {
		return fmt.Errorf("failed to delete profile: %v", err)
	}

	fmt.Printf("✅ Profile '%s' deleted successfully!\n", name)
	return nil
}

func showProfile(name string) error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	prof, err := pm.GetProfile(name)
	if err != nil {
		return err
	}

	activeProfile, _ := pm.GetActiveProfile()
	isActive := name == activeProfile

	fmt.Printf("Profile: %s\n", name)
	fmt.Println(strings.Repeat("=", len(name)+9))
	fmt.Printf("Hostname:     %s\n", prof.Hostname)
	fmt.Printf("Port:         %d\n", prof.Port)
	fmt.Printf("Endpoint:     %s\n", prof.GetBaseURL())
	if prof.NodeName != "" {
		fmt.Printf("Node Name:    %s\n", prof.NodeName)
	}
	fmt.Printf("Tenant URL:   %s\n", prof.TenantURL)
	fmt.Printf("Username:     %s\n", prof.Username)
	if prof.Workspace != "" {
		fmt.Printf("Workspace:    %s\n", prof.Workspace)
	}
	fmt.Printf("Active:       %t\n", isActive)
	fmt.Printf("Created:      %s\n", prof.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("Updated:      %s\n", prof.UpdatedAt.Format("2006-01-02 15:04:05"))

	// Status information
	fmt.Println()
	fmt.Println("Status Information:")
	fmt.Println("------------------")
	if prof.LastStatus != "" {
		statusColor := profile.GetStatusColor(prof.LastStatus)
		fmt.Printf("Status:       %s%s%s\n", statusColor, prof.LastStatus, profile.ResetColor())
		fmt.Printf("Description:  %s\n", profile.GetStatusDescription(prof.LastStatus))
		if !prof.LastStatusTime.IsZero() {
			fmt.Printf("Last Checked: %s\n", prof.LastStatusTime.Format("2006-01-02 15:04:05"))
		}
		if prof.LastError != "" {
			fmt.Printf("Last Error:   %s\n", prof.LastError)
		}
	} else {
		fmt.Println("Status:       Not checked yet")
	}

	// Session information
	fmt.Println()
	fmt.Println("Session Information:")
	fmt.Println("-------------------")
	fmt.Printf("Login Status: %s\n", prof.GetLoginStatus())

	if prof.AccessToken != "" {
		if prof.SessionID != "" {
			fmt.Printf("Session ID:   %s\n", prof.SessionID)
		}

		// Show token expiry information
		if !prof.AccessTokenExpiry.IsZero() {
			fmt.Printf("Access Token: Expires %s", prof.AccessTokenExpiry.Format("2006-01-02 15:04:05 MST"))
			if prof.IsAccessTokenExpired() {
				fmt.Printf(" \033[31m(EXPIRED)\033[0m")
			} else if prof.AccessTokenExpiry.Before(time.Now().Add(time.Hour)) {
				fmt.Printf(" \033[33m(expires soon)\033[0m")
			}
			fmt.Println()
		}

		if !prof.RefreshTokenExpiry.IsZero() {
			fmt.Printf("Refresh Token: Expires %s", prof.RefreshTokenExpiry.Format("2006-01-02 15:04:05 MST"))
			if prof.IsRefreshTokenExpired() {
				fmt.Printf(" \033[31m(EXPIRED)\033[0m")
			} else if prof.RefreshTokenExpiry.Before(time.Now().Add(24 * time.Hour)) {
				fmt.Printf(" \033[33m(expires soon)\033[0m")
			}
			fmt.Println()
		}
	}

	return nil
}

func checkProfileStatus(name string) error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	fmt.Printf("Checking status of profile '%s'...\n", name)

	if err := pm.UpdateProfileStatus(name); err != nil {
		return fmt.Errorf("failed to check profile status: %v", err)
	}

	// Show updated status
	return showProfile(name)
}

func checkAllProfilesStatusWithList() error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	profiles, err := pm.ListProfiles()
	if err != nil {
		return fmt.Errorf("failed to load profiles: %v", err)
	}

	if len(profiles) == 0 {
		fmt.Println("No profiles found. Use 'redb-cli profiles create <name>' to create one.")
		return nil
	}

	activeProfile, _ := pm.GetActiveProfile()

	fmt.Println("Checking status of all profiles...")
	fmt.Println()

	// Header
	fmt.Printf("%-15s %-25s %-15s %-15s %-20s %-15s %s\n", "NAME", "ENDPOINT", "STATUS", "LOGIN", "TOKEN EXPIRY", "LAST CHECKED", "ACTIVE")
	fmt.Println(strings.Repeat("-", 130))

	// Check each profile and display in table format
	for name, prof := range profiles {
		endpoint := fmt.Sprintf("%s:%d", prof.Hostname, prof.Port)

		// Check real-time status
		statusChecker := profile.NewStatusChecker()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		status, _, err := statusChecker.CheckNodeStatus(ctx, prof)
		cancel()

		statusStr := "Unknown"
		statusColor := profile.ResetColor()

		if err != nil {
			statusStr = "Error"
			statusColor = profile.GetStatusColor(profile.NodeStatusUnreachable)
		} else {
			statusStr = string(status)
			statusColor = profile.GetStatusColor(status)
		}

		// Update profile with latest status
		prof.LastStatus = status
		prof.LastStatusTime = time.Now()
		if err != nil {
			prof.LastError = err.Error()
		} else {
			prof.LastError = ""
		}
		pm.UpdateProfile(prof)

		activeStr := ""
		if name == activeProfile {
			activeStr = "✓"
		}

		// Get login status and token expiry
		loginStatus := prof.GetLoginStatus()
		var tokenExpiry string
		if prof.IsLoggedIn() && !prof.RefreshTokenExpiry.IsZero() {
			// Show refresh token expiry since that's what matters for session validity
			if prof.RefreshTokenExpiry.Before(time.Now().Add(24 * time.Hour)) {
				// Show in red if expiring within 24 hours
				tokenExpiry = fmt.Sprintf("\033[31m%s\033[0m", prof.RefreshTokenExpiry.Format("Jan 2 15:04"))
			} else if prof.RefreshTokenExpiry.Before(time.Now().Add(7 * 24 * time.Hour)) {
				// Show in yellow if expiring within 7 days
				tokenExpiry = fmt.Sprintf("\033[33m%s\033[0m", prof.RefreshTokenExpiry.Format("Jan 2 15:04"))
			} else {
				// Show in green if not expiring soon
				tokenExpiry = fmt.Sprintf("\033[32m%s\033[0m", prof.RefreshTokenExpiry.Format("Jan 2 15:04"))
			}
		} else {
			tokenExpiry = "N/A"
		}

		lastChecked := prof.LastStatusTime.Format("15:04:05")

		fmt.Printf("%-15s %-25s %s%-15s%s %-15s %-20s %-15s %s\n",
			name, endpoint, statusColor, statusStr, profile.ResetColor(),
			loginStatus, tokenExpiry, lastChecked, activeStr)
	}

	fmt.Println()
	fmt.Println("Use 'redb-cli profiles status <name>' for detailed information about a specific profile.")

	return nil
}

func refreshProfile(name string) error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	fmt.Printf("Refreshing tokens for profile '%s'...\n", name)

	if err := pm.RefreshTokens(name); err != nil {
		return fmt.Errorf("failed to refresh tokens: %v", err)
	}

	// Get updated profile to show new expiry times
	prof, err := pm.GetProfile(name)
	if err != nil {
		return fmt.Errorf("failed to get updated profile: %v", err)
	}

	fmt.Printf("✅ Tokens refreshed successfully!\n")
	fmt.Printf("New Access Token Expiry: %s\n", prof.AccessTokenExpiry.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("New Refresh Token Expiry: %s\n", prof.RefreshTokenExpiry.Format("2006-01-02 15:04:05 MST"))

	return nil
}

// Helper function to truncate strings for table display
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func activateProfile(name string) error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	// Check if profile exists
	_, err = pm.GetProfile(name)
	if err != nil {
		return err
	}

	if err := pm.SetActiveProfile(name); err != nil {
		return fmt.Errorf("failed to activate profile: %v", err)
	}

	fmt.Printf("✅ Profile '%s' is now active.\n", name)
	return nil
}

func init() {
	// Add profiles command to root
	rootCmd.AddCommand(profilesCmd)

	// Add flags to create command
	profilesCreateCmd.Flags().String("tenant-url", "", "Tenant URL (defaults to 'default' for single-tenant mode)")

	// Add subcommands
	profilesCmd.AddCommand(profilesListCmd)
	profilesCmd.AddCommand(profilesCreateCmd)
	profilesCmd.AddCommand(profilesDeleteCmd)
	profilesCmd.AddCommand(profilesShowCmd)
	profilesCmd.AddCommand(profilesStatusCmd)
	profilesCmd.AddCommand(profilesActivateCmd)
	profilesCmd.AddCommand(profilesRefreshCmd)
}
