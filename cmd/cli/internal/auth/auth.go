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

	"github.com/redbco/redb-open/cmd/cli/internal/config"
	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
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

// Login authenticates the user
func Login(args []string) error {
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
}

// Logout logs out the current user
func Logout() error {
	username, err := config.GetUsername()
	if err != nil {
		return AuthError{message: "no user is currently logged in"}
	}

	// Get refresh token for logout
	refreshToken, err := config.GetRefreshToken(username)
	if err != nil {
		// If we can't get the refresh token, just clear local credentials
		fmt.Println("⚠️  Could not get refresh token for logout, clearing local credentials only")
	} else {
		// Call logout API endpoint with refresh token
		tenantURL, err := config.GetTenantURL()
		if err == nil {
			client := httpclient.GetClient()
			url := fmt.Sprintf("%s/api/v1/auth/logout", tenantURL)

			logoutReq := LogoutRequest{
				RefreshToken: refreshToken,
			}

			// Attempt to logout from server (ignore errors as we'll clear local credentials anyway)
			var logoutResp LogoutResponse
			if err := client.Post(url, logoutReq, &logoutResp, false); err != nil {
				// Log the error but don't fail the logout process
				fmt.Printf("⚠️  %v\n", err)
			}
		}
	}

	// Clear local credentials
	if err := config.ClearCredentials(username); err != nil {
		return AuthError{message: fmt.Sprintf("failed to clear credentials: %v", err)}
	}

	fmt.Printf("✅ Successfully logged out %s\n", username)
	return nil
}

// Profile displays the current user's profile
func Profile() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/auth/profile", tenantURL)

	var response ProfileResponse
	if err := client.Get(url, &response, true); err != nil {
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

// ListSessions lists all active sessions for the authenticated user
func ListSessions() error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/auth/sessions", tenantURL)

	var sessionsResp SessionsResponse
	if err := client.Get(url, &sessionsResp, true); err != nil {
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

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/auth/sessions/%s/logout", tenantURL, sessionID)

	var response LogoutSessionResponse
	if err := client.Post(url, nil, &response, true); err != nil {
		return AuthError{message: fmt.Sprintf("failed to logout session: %v", err)}
	}

	fmt.Printf("✅ Successfully logged out session: %s\n", sessionID)
	return nil
}

// LogoutAllSessions logs out all sessions for the authenticated user
func LogoutAllSessions(excludeCurrent bool) error {
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/auth/sessions/logout-all", tenantURL)

	logoutReq := LogoutAllSessionsRequest{
		ExcludeCurrent: excludeCurrent,
	}

	var response LogoutAllSessionsResponse
	if err := client.Post(url, logoutReq, &response, true); err != nil {
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

	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/auth/sessions/%s/name", tenantURL, sessionID)

	updateReq := UpdateSessionNameRequest{
		SessionName: newName,
	}

	var response UpdateSessionNameResponse
	if err := client.Put(url, updateReq, &response, true); err != nil {
		return fmt.Errorf("failed to update session name: %v", err)
	}

	fmt.Printf("Successfully updated session name to: %s\n", newName)
	return nil
}

// ChangePassword changes the current user's password
func ChangePassword(args []string) error {
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

	// Get tenant URL
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/auth/change-password", tenantURL)

	if err := client.Post(url, changeReq, nil, true); err != nil {
		return fmt.Errorf("failed to change password: %v", err)
	}

	fmt.Println("Password changed successfully")
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

// Status shows the current authentication status
func Status() error {
	// Try to get the current username
	username, err := config.GetUsername()
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Println("No user credentials found in keyring")
		return nil
	}

	// Get stored credentials
	accessToken, err := config.GetToken(username)
	if err != nil {
		fmt.Println("Authentication Status: Not logged in")
		fmt.Printf("Username found: %s\n", username)
		fmt.Println("Access token not found in keyring")
		return nil
	}

	refreshToken, err := config.GetRefreshToken(username)
	if err != nil {
		fmt.Println("Authentication Status: Partially logged in")
		fmt.Printf("Username: %s\n", username)
		fmt.Println("Refresh token not found in keyring")
		return nil
	}

	// Get tenant URL
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		fmt.Println("Authentication Status: Partially logged in")
		fmt.Printf("Username: %s\n", username)
		fmt.Println("Tenant URL not found in keyring")
		return nil
	}

	// Get workspace
	workspace, err := config.GetWorkspace(username)
	if err != nil {
		workspace = "Not selected"
	}

	// Parse token expiration times
	var accessExpiry, refreshExpiry string
	var accessExpiryTime, refreshExpiryTime time.Time

	if accessClaims, parseErr := parseJWTToken(accessToken); parseErr == nil {
		accessExpiryTime = time.Unix(accessClaims.Exp, 0)
		accessExpiry = accessExpiryTime.Format("2006-01-02 15:04:05 MST")
	} else {
		accessExpiry = "Unable to parse"
	}

	if refreshClaims, parseErr := parseJWTToken(refreshToken); parseErr == nil {
		refreshExpiryTime = time.Unix(refreshClaims.Exp, 0)
		refreshExpiry = refreshExpiryTime.Format("2006-01-02 15:04:05 MST")
	} else {
		refreshExpiry = "Unable to parse"
	}

	// Check if tokens are expired
	now := time.Now()
	accessExpired := accessExpiryTime.Before(now)
	refreshExpired := refreshExpiryTime.Before(now)

	// Display status
	fmt.Println("Authentication Status: Logged in")
	fmt.Println("----------------------------------------")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Username:\t%s\n", username)
	fmt.Fprintf(w, "Tenant URL:\t%s\n", tenantURL)
	fmt.Fprintf(w, "Selected Workspace:\t%s\n", workspace)

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

// SelectWorkspace selects the active workspace
func SelectWorkspace(workspaceName string) error {
	if workspaceName == "" {
		return fmt.Errorf("workspace name is required")
	}

	// Get tenant URL
	tenantURL, err := config.GetTenantURL()
	if err != nil {
		return err
	}

	// Get list of workspaces to validate the name
	client := httpclient.GetClient()
	url := fmt.Sprintf("%s/api/v1/workspaces", tenantURL)

	var response struct {
		Workspaces []struct {
			ID   string `json:"workspace_id"`
			Name string `json:"workspace_name"`
		} `json:"workspaces"`
	}

	if err = client.Get(url, &response, true); err != nil {
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

	// Store the selected workspace
	username, err := config.GetUsername()
	if err != nil {
		return fmt.Errorf("no user logged in: %v", err)
	}

	if err := config.StoreWorkspace(username, workspaceName); err != nil {
		return fmt.Errorf("failed to store workspace selection: %v", err)
	}

	fmt.Printf("Selected workspace: %s (ID: %s)\n", workspaceName, selectedWorkspace.ID)
	return nil
}
