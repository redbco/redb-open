package auth

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/redbco/redb-open/cmd/cli/internal/common"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
	"github.com/redbco/redb-open/cmd/cli/internal/profile"
	"golang.org/x/term"
)

// AuthError represents an auth-specific error that should not show usage help
type AuthError struct {
	message string
}

func (e AuthError) Error() string {
	return "❌ Error: " + e.message
}

type LoginRequest struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	ExpiryTimeHours string `json:"expiry_time_hours,omitempty"`
	SessionName     string `json:"session_name,omitempty"`
	UserAgent       string `json:"user_agent,omitempty"`
	IPAddress       string `json:"ip_address,omitempty"`
	Platform        string `json:"platform,omitempty"`
	Browser         string `json:"browser,omitempty"`
	OperatingSystem string `json:"operating_system,omitempty"`
	DeviceType      string `json:"device_type,omitempty"`
	Location        string `json:"location,omitempty"`
}

type LoginProfile struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Name     string `json:"name"`
}

type LoginResponse struct {
	Profile      LoginProfile `json:"profile"`
	AccessToken  string       `json:"access_token"`
	RefreshToken string       `json:"refresh_token"`
	SessionID    string       `json:"session_id"`
	Status       string       `json:"status"`
}

type LogoutRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type LogoutResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
}

type UserProfile struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	Name     string `json:"name"`
}

type ProfileResponse struct {
	Profile UserProfile `json:"profile"`
}

type SessionInfo struct {
	SessionID       string `json:"session_id"`
	SessionName     string `json:"session_name"`
	UserAgent       string `json:"user_agent"`
	IPAddress       string `json:"ip_address"`
	Platform        string `json:"platform"`
	Browser         string `json:"browser"`
	OperatingSystem string `json:"operating_system"`
	DeviceType      string `json:"device_type"`
	Location        string `json:"location"`
	LastActivity    string `json:"last_activity"`
	Created         string `json:"created"`
	Expires         string `json:"expires"`
	IsCurrent       bool   `json:"is_current"`
}

type SessionsResponse struct {
	Sessions []SessionInfo `json:"sessions"`
	Status   string        `json:"status"`
}

type LogoutSessionResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
}

type LogoutAllSessionsRequest struct {
	ExcludeCurrent bool `json:"exclude_current,omitempty"`
}

type LogoutAllSessionsResponse struct {
	SessionsLoggedOut int    `json:"sessions_logged_out"`
	Message           string `json:"message"`
	Success           bool   `json:"success"`
	Status            string `json:"status"`
}

type UpdateSessionNameRequest struct {
	SessionName string `json:"session_name"`
}

type UpdateSessionNameResponse struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
	Status  string `json:"status"`
}

type ChangePasswordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

// getSystemInfo collects system information for session metadata
func getSystemInfo() (platform, operatingSystem, deviceType string) {
	switch runtime.GOOS {
	case "windows":
		platform = "Windows"
		operatingSystem = "Windows"
		deviceType = "Desktop"
	case "darwin":
		platform = "macOS"
		operatingSystem = "macOS"
		deviceType = "Desktop"
	case "linux":
		platform = "Linux"
		operatingSystem = "Linux"
		deviceType = "Desktop"
	default:
		platform = runtime.GOOS
		operatingSystem = runtime.GOOS
		deviceType = "Unknown"
	}
	return
}

// LoginWithProfile authenticates the user using a profile or direct connection details
func LoginWithProfile(args []string, profileName string) error {
	if profileName != "" {
		return loginWithProfile(profileName, args)
	}
	// For backward compatibility, if no profile is specified, require the user to create one
	return fmt.Errorf("profile name is required. Use 'redb-cli profiles create <name>' to create a profile, then 'redb-cli auth login --profile <name>' to login")
}

// Login authenticates the user (DEPRECATED - use profiles instead)
func Login(args []string) error {
	return fmt.Errorf("legacy login is deprecated. Use 'redb-cli profiles create <name>' to create a profile, then 'redb-cli auth login --profile <name>' to login")
	/*
		reader := bufio.NewReader(os.Stdin)

		// Get username
		var username string
		if len(args) > 0 && strings.HasPrefix(args[0], "--username=") {
			username = strings.TrimPrefix(args[0], "--username=")
		} else {
			fmt.Print("Username (email): ")
			username, _ = reader.ReadString('\n')
			username = strings.TrimSpace(username)
		}

		if username == "" {
			return fmt.Errorf("username is required")
		}

		// Get password
		var password string
		if len(args) > 1 && strings.HasPrefix(args[1], "--password=") {
			password = strings.TrimPrefix(args[1], "--password=")
		} else {
			fmt.Print("Password: ")
			passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
			if err != nil {
				return fmt.Errorf("failed to read password: %v", err)
			}
			password = string(passwordBytes)
			fmt.Println() // Add newline after password input
		}

		if password == "" {
			return fmt.Errorf("password is required")
		}

		// Get hostname
		var hostname string
		if len(args) > 2 && strings.HasPrefix(args[2], "--hostname=") {
			hostname = strings.TrimPrefix(args[2], "--hostname=")
		} else {
			fmt.Print("Hostname (default: localhost:8080): ")
			hostname, _ = reader.ReadString('\n')
			hostname = strings.TrimSpace(hostname)
			if hostname == "" {
				hostname = "localhost:8080"
			}
		}

		// Check if tenant URL is already stored for this username
		var tenantURL string
		storedTenantURL, err := config.GetTenant(username)
		if err == nil && storedTenantURL != "" {
			// Use stored tenant URL
			tenantURL = storedTenantURL
			// fmt.Printf("Using stored tenant URL: %s\n", tenantURL)
		} else {
			// Get tenant URL from command line or prompt
			if len(args) > 3 && strings.HasPrefix(args[3], "--tenant=") {
				tenantURL = strings.TrimPrefix(args[3], "--tenant=")
			} else {
				fmt.Print("Tenant URL: ")
				tenantURL, _ = reader.ReadString('\n')
				tenantURL = strings.TrimSpace(tenantURL)
			}

			if tenantURL == "" {
				return fmt.Errorf("tenant URL is required")
			}
		}

		// Get optional session name
		sessionName := "reDB CLI"
		// var sessionName string
		// fmt.Print("Session Name (optional, default: reDB CLI): ")
		// sessionName, _ = reader.ReadString('\n')
		// sessionName = strings.TrimSpace(sessionName)
		// if sessionName == "" {
		// 	sessionName = "reDB CLI"
		// }

		// Get system information
		platform, operatingSystem, deviceType := getSystemInfo()

		// Prepare login request with session metadata
		loginReq := LoginRequest{
			Username:        username,
			Password:        password,
			SessionName:     sessionName,
			UserAgent:       fmt.Sprintf("redb-cli/%s (%s)", "1.0.0", operatingSystem),
			Platform:        platform,
			OperatingSystem: operatingSystem,
			DeviceType:      deviceType,
		}

		// Ensure protocol is included in hostname
		if !strings.HasPrefix(hostname, "http://") && !strings.HasPrefix(hostname, "https://") {
			hostname = "http://" + hostname
		}

		// Make login request with tenant URL
		client := httpclient.GetClient()
		url := fmt.Sprintf("%s/%s/api/v1/auth/login", hostname, tenantURL)

		var loginResp LoginResponse
		if err = client.Post(url, loginReq, &loginResp, false); err != nil {
			return fmt.Errorf("login failed: %v", err)
		}

		// Store credentials in keyring
		if err = config.StoreUsername(username); err != nil {
			return fmt.Errorf("failed to store username: %v", err)
		}

		if err = config.StoreToken(username, loginResp.AccessToken); err != nil {
			return fmt.Errorf("failed to store token: %v", err)
		}

		if err = config.StoreRefreshToken(username, loginResp.RefreshToken); err != nil {
			return fmt.Errorf("failed to store refresh token: %v", err)
		}

		if err = config.StoreSessionID(username, loginResp.SessionID); err != nil {
			return fmt.Errorf("failed to store session ID: %v", err)
		}

		if err = config.StoreHostname(username, hostname); err != nil {
			return fmt.Errorf("failed to store hostname: %v", err)
		}

		if err = config.StoreTenant(username, tenantURL); err != nil {
			return fmt.Errorf("failed to store tenant: %v", err)
		}

		fmt.Printf("Successfully logged in as %s\n", username)
		// fmt.Printf("Tenant: %s\n", tenantURL)
		fmt.Printf("Session: %s (ID: %s)\n", sessionName, loginResp.SessionID)

		// Check if workspace is already selected
		currentWorkspace, err := config.GetWorkspace(username)
		if err != nil || currentWorkspace == "" {
			// Prompt for workspace selection
			fmt.Print("\nSelect workspace (press Enter to skip): ")
			workspaceInput, _ := reader.ReadString('\n')
			workspaceInput = strings.TrimSpace(workspaceInput)

			if workspaceInput != "" {
				// Validate and store the workspace
				if err := SelectWorkspace(workspaceInput); err != nil {
					fmt.Printf("Warning: Failed to select workspace '%s': %v\n", workspaceInput, err)
					fmt.Println("You can select a workspace later using 'redb-cli select workspace <name>'")
				} else {
					fmt.Printf("Selected workspace: %s\n", workspaceInput)
				}
			} else {
				fmt.Println("No workspace selected. Use 'redb-cli select workspace <name>' to select one later.")
			}
		}

		return nil
	*/
}

// loginWithProfile handles login using a profile
func loginWithProfile(profileName string, args []string) error {
	// Import profile package
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	// Get the profile
	prof, err := pm.GetProfile(profileName)
	if err != nil {
		return fmt.Errorf("profile '%s' not found: %v", profileName, err)
	}

	reader := bufio.NewReader(os.Stdin)

	// Get password (required for login)
	var password string
	if len(args) > 0 && strings.HasPrefix(args[0], "--password=") {
		password = strings.TrimPrefix(args[0], "--password=")
	} else {
		fmt.Print("Password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return fmt.Errorf("failed to read password: %v", err)
		}
		password = string(passwordBytes)
		fmt.Println() // Add newline after password input
	}

	if password == "" {
		return fmt.Errorf("password is required")
	}

	// Get system information
	platform, operatingSystem, deviceType := getSystemInfo()

	// Prepare login request with session metadata
	loginReq := LoginRequest{
		Username:        prof.Username,
		Password:        password,
		SessionName:     fmt.Sprintf("reDB CLI (%s)", profileName),
		UserAgent:       fmt.Sprintf("redb-cli/%s (%s)", "1.0.0", operatingSystem),
		Platform:        platform,
		OperatingSystem: operatingSystem,
		DeviceType:      deviceType,
	}

	// Make login request with profile's tenant URL
	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/auth/login", prof.GetTenantURL())

	var loginResp LoginResponse
	if err = client.Post(url, loginReq, &loginResp, false); err != nil {
		return fmt.Errorf("login failed: %v", err)
	}

	// Update profile with session information
	prof.AccessToken = loginResp.AccessToken
	prof.RefreshToken = loginResp.RefreshToken
	prof.SessionID = loginResp.SessionID

	// Update token expiry information
	prof.UpdateTokenExpiry()

	// Save updated profile
	if err := pm.UpdateProfile(prof); err != nil {
		return fmt.Errorf("failed to save profile: %v", err)
	}

	// Set this profile as active
	if err := pm.SetActiveProfile(profileName); err != nil {
		return fmt.Errorf("failed to set active profile: %v", err)
	}

	fmt.Printf("Successfully logged in to profile '%s' as %s\n", profileName, prof.Username)
	fmt.Printf("Endpoint: %s\n", prof.GetBaseURL())
	fmt.Printf("Tenant: %s\n", prof.TenantURL)
	fmt.Printf("Session: %s (ID: %s)\n", loginReq.SessionName, loginResp.SessionID)

	// Check if workspace is set in profile
	if prof.Workspace == "" {
		// Prompt for workspace selection
		fmt.Print("\nSelect workspace (press Enter to skip): ")
		workspaceInput, _ := reader.ReadString('\n')
		workspaceInput = strings.TrimSpace(workspaceInput)

		if workspaceInput != "" {
			prof.Workspace = workspaceInput
			if err := pm.UpdateProfile(prof); err != nil {
				fmt.Printf("Warning: Failed to save workspace to profile: %v\n", err)
			} else {
				fmt.Printf("Workspace '%s' saved to profile.\n", workspaceInput)
			}
		}
	} else {
		fmt.Printf("Using workspace: %s\n", prof.Workspace)
	}

	return nil
}

// Logout logs out the current user from the active profile
func Logout() error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	activeProfileName, err := pm.GetActiveProfile()
	if err != nil {
		return AuthError{message: "no active profile found. Use 'redb-cli profiles list' to see available profiles"}
	}

	prof, err := pm.GetProfile(activeProfileName)
	if err != nil {
		return fmt.Errorf("failed to get active profile '%s': %v", activeProfileName, err)
	}

	if !prof.IsLoggedIn() {
		return AuthError{message: fmt.Sprintf("profile '%s' is not logged in", activeProfileName)}
	}

	// Call logout API endpoint with refresh token if available
	if prof.RefreshToken != "" {
		client := httpclient.GetClient()
		url := fmt.Sprintf("%s/api/v1/auth/logout", prof.GetTenantURL())

		logoutReq := LogoutRequest{
			RefreshToken: prof.RefreshToken,
		}

		var logoutResp LogoutResponse
		if err := client.Post(url, logoutReq, &logoutResp, false); err != nil {
			fmt.Printf("⚠️  Failed to logout from server: %v\n", err)
		} else {
			fmt.Println("Successfully logged out from server")
		}
	} else {
		fmt.Println("⚠️  No refresh token found, clearing local credentials only")
	}

	// Clear profile credentials
	prof.AccessToken = ""
	prof.RefreshToken = ""
	prof.SessionID = ""
	prof.AccessTokenExpiry = time.Time{}
	prof.RefreshTokenExpiry = time.Time{}

	if err := pm.UpdateProfile(prof); err != nil {
		return fmt.Errorf("failed to clear profile credentials: %v", err)
	}

	fmt.Printf("✅ Successfully logged out from profile: %s\n", activeProfileName)
	return nil
}

// Profile displays the current user's profile using the active profile
func Profile() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/auth/profile", profileInfo.TenantURL)

	var response ProfileResponse
	if err := client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get profile: %v", err)
	}

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()
	fmt.Fprintln(w, "Field\tValue")
	fmt.Fprintln(w, "-----\t-----")
	fmt.Fprintf(w, "Tenant ID\t%s\n", response.Profile.TenantID)
	fmt.Fprintf(w, "User ID\t%s\n", response.Profile.UserID)
	fmt.Fprintf(w, "Username\t%s\n", response.Profile.Username)
	fmt.Fprintf(w, "Email\t%s\n", response.Profile.Email)
	fmt.Fprintf(w, "Name\t%s\n", response.Profile.Name)

	_ = w.Flush()
	fmt.Println()
	return nil
}

// ListSessions lists all active sessions for the authenticated user using the active profile
func ListSessions() error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/auth/sessions", profileInfo.TenantURL)

	var sessionsResp SessionsResponse
	if err := client.Get(url, &sessionsResp); err != nil {
		return fmt.Errorf("failed to get sessions: %v", err)
	}

	if len(sessionsResp.Sessions) == 0 {
		fmt.Println("ℹ️  No active sessions found")
		return nil
	}

	// Create a tabwriter for formatted output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Println()
	fmt.Fprintln(w, "Session Name\tSession ID\tPlatform\tIP Address\tLast Activity\tCurrent")
	fmt.Fprintln(w, "------------\t----------\t--------\t----------\t-------------\t-------")

	for _, session := range sessionsResp.Sessions {
		current := ""
		if session.IsCurrent {
			current = "✓"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			session.SessionName,
			session.SessionID,
			session.Platform,
			session.IPAddress,
			session.LastActivity,
			current,
		)
	}

	_ = w.Flush()
	fmt.Println()
	return nil
}

// LogoutSession logs out a specific session by session ID
func LogoutSession(sessionID string) error {
	if sessionID == "" {
		return AuthError{message: "session ID is required"}
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/api/v1/auth/sessions/%s/logout", profileInfo.TenantURL, sessionID)

	var response LogoutSessionResponse
	if err := client.Post(url, nil, &response); err != nil {
		return AuthError{message: fmt.Sprintf("failed to logout session: %v", err)}
	}

	fmt.Printf("✅ Successfully logged out session: %s\n", sessionID)
	return nil
}

// LogoutAllSessions logs out all sessions for the authenticated user
func LogoutAllSessions(excludeCurrent bool) error {
	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/api/v1/auth/sessions/logout-all", profileInfo.TenantURL)

	logoutReq := LogoutAllSessionsRequest{
		ExcludeCurrent: excludeCurrent,
	}

	var response LogoutAllSessionsResponse
	if err := client.Post(url, logoutReq, &response); err != nil {
		return fmt.Errorf("failed to logout all sessions: %v", err)
	}

	fmt.Printf("Successfully logged out %d sessions\n", response.SessionsLoggedOut)
	return nil
}

// UpdateSessionName updates the name of a specific session
func UpdateSessionName(sessionID, newName string) error {
	if sessionID == "" {
		return fmt.Errorf("session ID is required")
	}
	if newName == "" {
		return fmt.Errorf("session name is required")
	}

	profileInfo, err := common.GetActiveProfileInfo()
	if err != nil {
		return err
	}

	client, err := common.GetProfileClient()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/api/v1/auth/sessions/%s/name", profileInfo.TenantURL, sessionID)

	updateReq := UpdateSessionNameRequest{
		SessionName: newName,
	}

	var response UpdateSessionNameResponse
	if err := client.Put(url, updateReq, &response); err != nil {
		return fmt.Errorf("failed to update session name: %v", err)
	}

	fmt.Printf("Successfully updated session name to: %s\n", newName)
	return nil
}

// ChangePassword changes the current user's password using the active profile
func ChangePassword(args []string) error {
	// Initialize profile manager
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	// Get active profile
	activeProfileName, err := pm.GetActiveProfile()
	if err != nil {
		return fmt.Errorf("no active profile found. Use 'redb-cli profiles list' to see available profiles or 'redb-cli profiles create <name>' to create one: %v", err)
	}

	prof, err := pm.GetProfile(activeProfileName)
	if err != nil {
		return fmt.Errorf("failed to get active profile '%s': %v", activeProfileName, err)
	}

	if !prof.IsLoggedIn() {
		return fmt.Errorf("profile '%s' is not logged in or session has expired. Use 'redb-cli auth login --profile %s' to login", prof.Name, prof.Name)
	}

	// Get current password
	var currentPassword string
	if len(args) > 0 && strings.HasPrefix(args[0], "--current-password=") {
		currentPassword = strings.TrimPrefix(args[0], "--current-password=")
	} else {
		fmt.Print("Current Password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return fmt.Errorf("failed to read current password: %v", err)
		}
		currentPassword = string(passwordBytes)
		fmt.Println()
	}

	if currentPassword == "" {
		return fmt.Errorf("current password is required")
	}

	// Get new password
	var newPassword string
	if len(args) > 1 && strings.HasPrefix(args[1], "--new-password=") {
		newPassword = strings.TrimPrefix(args[1], "--new-password=")
	} else {
		fmt.Print("New Password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return fmt.Errorf("failed to read new password: %v", err)
		}
		newPassword = string(passwordBytes)
		fmt.Println()
	}

	if newPassword == "" {
		return fmt.Errorf("new password is required")
	}

	// Confirm new password
	fmt.Print("Confirm New Password: ")
	confirmBytes, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return fmt.Errorf("failed to read password confirmation: %v", err)
	}
	confirmPassword := string(confirmBytes)
	fmt.Println()

	if newPassword != confirmPassword {
		return fmt.Errorf("new password and confirmation do not match")
	}

	// Prepare change password request
	changeReq := ChangePasswordRequest{
		OldPassword: currentPassword,
		NewPassword: newPassword,
	}

	// Get profile-aware HTTP client
	client, err := httpclient.GetProfileClient()
	if err != nil {
		return fmt.Errorf("failed to get profile client: %v", err)
	}

	url := fmt.Sprintf("%s/api/v1/auth/change-password", prof.GetTenantURL())

	if err := client.Post(url, changeReq, nil); err != nil {
		return fmt.Errorf("failed to change password: %v", err)
	}

	fmt.Printf("Password changed successfully for profile '%s'\n", prof.Name)
	return nil
}

// JWTClaims represents the standard JWT claims
type JWTClaims struct {
	Exp int64  `json:"exp"`
	Iat int64  `json:"iat"`
	Sub string `json:"sub"`
}

// parseJWTToken extracts expiration time from a JWT token
func parseJWTToken(token string) (*JWTClaims, error) {
	// Split the token into parts
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT token format")
	}

	// Decode the payload (second part)
	payload := parts[1]
	// Add padding if needed
	if len(payload)%4 != 0 {
		payload += strings.Repeat("=", 4-len(payload)%4)
	}

	// Replace URL-safe characters
	payload = strings.ReplaceAll(payload, "-", "+")
	payload = strings.ReplaceAll(payload, "_", "/")

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT payload: %v", err)
	}

	var claims JWTClaims
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, fmt.Errorf("failed to parse JWT claims: %v", err)
	}

	return &claims, nil
}

// Status shows the current authentication status using profiles
func Status() error {
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	activeProfileName, err := pm.GetActiveProfile()
	if err != nil {
		fmt.Println("Authentication Status: No active profile")
		fmt.Println("Use 'redb-cli profiles list' to see available profiles or 'redb-cli profiles create <name>' to create one")
		return nil
	}

	prof, err := pm.GetProfile(activeProfileName)
	if err != nil {
		fmt.Printf("Authentication Status: Error loading profile '%s': %v\n", activeProfileName, err)
		return nil
	}

	if !prof.IsLoggedIn() {
		fmt.Printf("Authentication Status: Not logged in (Profile: %s)\n", activeProfileName)
		fmt.Printf("Profile: %s\n", prof.Name)
		fmt.Printf("Username: %s\n", prof.Username)
		fmt.Printf("Tenant URL: %s\n", prof.GetTenantURL())
		fmt.Printf("Workspace: %s\n", prof.Workspace)
		fmt.Println("Use 'redb-cli auth login --profile " + prof.Name + "' to login")
		return nil
	}

	// Parse token expiration times
	var accessExpiry, refreshExpiry string
	accessExpired := prof.IsAccessTokenExpired()
	refreshExpired := prof.IsRefreshTokenExpired()

	if !prof.AccessTokenExpiry.IsZero() {
		accessExpiry = prof.AccessTokenExpiry.Format("2006-01-02 15:04:05 MST")
	} else {
		accessExpiry = "Unknown"
	}

	if !prof.RefreshTokenExpiry.IsZero() {
		refreshExpiry = prof.RefreshTokenExpiry.Format("2006-01-02 15:04:05 MST")
	} else {
		refreshExpiry = "Unknown"
	}

	// Display status
	fmt.Printf("Authentication Status: Logged in (Profile: %s)\n", prof.Name)
	fmt.Println("----------------------------------------")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Profile:\t%s\n", prof.Name)
	fmt.Fprintf(w, "Username:\t%s\n", prof.Username)
	fmt.Fprintf(w, "Tenant URL:\t%s\n", prof.GetTenantURL())
	fmt.Fprintf(w, "Selected Workspace:\t%s\n", prof.Workspace)

	// Show token status
	accessStatus := "Valid"
	if accessExpired {
		accessStatus = "EXPIRED"
	}
	refreshStatus := "Valid"
	if refreshExpired {
		refreshStatus = "EXPIRED"
	}

	fmt.Fprintf(w, "Access Token:\t%s\n", accessStatus)
	fmt.Fprintf(w, "Access Token Expires:\t%s\n", accessExpiry)
	fmt.Fprintf(w, "Refresh Token:\t%s\n", refreshStatus)
	fmt.Fprintf(w, "Refresh Token Expires:\t%s\n", refreshExpiry)
	_ = w.Flush()

	// Show warnings if tokens are expired
	if accessExpired {
		fmt.Println("\n⚠️  Warning: Access token has expired. You may need to login again.")
	}
	if refreshExpired {
		fmt.Println("\n⚠️  Warning: Refresh token has expired. You need to login again.")
	}

	return nil
}

// SelectWorkspace selects the active workspace for the current profile
func SelectWorkspace(workspaceName string) error {
	if workspaceName == "" {
		return fmt.Errorf("workspace name is required")
	}

	// Initialize profile manager
	pm, err := profile.NewProfileManager()
	if err != nil {
		return fmt.Errorf("failed to initialize profile manager: %v", err)
	}

	// Get active profile
	activeProfileName, err := pm.GetActiveProfile()
	if err != nil {
		return fmt.Errorf("no active profile found. Use 'redb-cli profiles list' to see available profiles or 'redb-cli profiles create <name>' to create one: %v", err)
	}

	prof, err := pm.GetProfile(activeProfileName)
	if err != nil {
		return fmt.Errorf("failed to get active profile '%s': %v", activeProfileName, err)
	}

	if !prof.IsLoggedIn() {
		return fmt.Errorf("profile '%s' is not logged in or session has expired. Use 'redb-cli auth login --profile %s' to login", prof.Name, prof.Name)
	}

	// Get profile-aware HTTP client
	client, err := httpclient.GetProfileClient()
	if err != nil {
		return fmt.Errorf("failed to get profile client: %v", err)
	}

	// Get list of workspaces to validate the name
	url := fmt.Sprintf("%s/api/v1/workspaces", prof.GetTenantURL())

	var response struct {
		Workspaces []struct {
			ID   string `json:"workspace_id"`
			Name string `json:"workspace_name"`
		} `json:"workspaces"`
	}

	if err = client.Get(url, &response); err != nil {
		return fmt.Errorf("failed to get workspaces: %v", err)
	}

	// Find the workspace
	var selectedWorkspace *struct {
		ID   string `json:"workspace_id"`
		Name string `json:"workspace_name"`
	}

	for _, ws := range response.Workspaces {
		if ws.Name == workspaceName {
			selectedWorkspace = &ws
			break
		}
	}

	if selectedWorkspace == nil {
		return fmt.Errorf("workspace '%s' not found", workspaceName)
	}

	// Store the selected workspace in the profile
	prof.Workspace = workspaceName
	if err := pm.UpdateProfile(prof); err != nil {
		return fmt.Errorf("failed to save workspace selection to profile: %v", err)
	}

	fmt.Printf("Selected workspace: %s (ID: %s) for profile '%s'\n", workspaceName, selectedWorkspace.ID, prof.Name)
	return nil
}
