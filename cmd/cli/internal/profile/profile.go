package profile

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/keyring"
)

const (
	ServiceName = "redb-cli-profiles"
	ProfilesKey = "profiles"
	ActiveKey   = "active_profile"
)

// NodeStatus represents the status of a reDB node
type NodeStatus string

const (
	NodeStatusUnreachable        NodeStatus = "unreachable"
	NodeStatusNotInitialized     NodeStatus = "not_initialized"
	NodeStatusInitializedNoUsers NodeStatus = "initialized_no_users"
	NodeStatusReadyNoMesh        NodeStatus = "ready_no_mesh"
	NodeStatusReadyWithMesh      NodeStatus = "ready_with_mesh"
)

// Profile represents a connection profile for a reDB system
type Profile struct {
	Name      string `json:"name"`
	Hostname  string `json:"hostname"`
	Port      int    `json:"port"`
	NodeName  string `json:"node_name,omitempty"`
	TenantURL string `json:"tenant_url"`
	Username  string `json:"username"`
	Workspace string `json:"workspace,omitempty"`

	// Session information
	AccessToken        string    `json:"-"` // Not stored in JSON, kept in keyring
	RefreshToken       string    `json:"-"` // Not stored in JSON, kept in keyring
	SessionID          string    `json:"-"` // Not stored in JSON, kept in keyring
	AccessTokenExpiry  time.Time `json:"access_token_expiry,omitempty"`
	RefreshTokenExpiry time.Time `json:"refresh_token_expiry,omitempty"`

	// Status information (cached)
	LastStatus     NodeStatus `json:"last_status,omitempty"`
	LastStatusTime time.Time  `json:"last_status_time,omitempty"`
	LastError      string     `json:"last_error,omitempty"`

	// Metadata
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ProfileManager manages CLI profiles
type ProfileManager struct {
	keyringManager *keyring.KeyringManager
	profilesFile   string
}

// NewProfileManager creates a new profile manager
func NewProfileManager() (*ProfileManager, error) {
	// Initialize keyring manager
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()
	keyringManager := keyring.NewKeyringManager(keyringPath, masterPassword)

	// Determine profiles file path
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get user home directory: %v", err)
	}

	profilesFile := filepath.Join(homeDir, ".redb", "profiles.json")

	return &ProfileManager{
		keyringManager: keyringManager,
		profilesFile:   profilesFile,
	}, nil
}

// GetBaseURL returns the base URL for the profile
func (p *Profile) GetBaseURL() string {
	protocol := "http"
	if p.Port == 443 || p.Port == 8443 {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%s:%d", protocol, p.Hostname, p.Port)
}

// GetTenantURL returns the tenant-specific URL for the profile
func (p *Profile) GetTenantURL() string {
	baseURL := p.GetBaseURL()
	if p.TenantURL != "" {
		return fmt.Sprintf("%s/%s", baseURL, p.TenantURL)
	}
	return baseURL
}

// GetStatusURL returns the status endpoint URL for the profile
func (p *Profile) GetStatusURL() string {
	return fmt.Sprintf("%s/api/v1/status", p.GetBaseURL())
}

// LoadProfiles loads all profiles from storage
func (pm *ProfileManager) LoadProfiles() (map[string]*Profile, error) {
	profiles := make(map[string]*Profile)

	// Create profiles directory if it doesn't exist
	profilesDir := filepath.Dir(pm.profilesFile)
	if err := os.MkdirAll(profilesDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create profiles directory: %v", err)
	}

	// Check if profiles file exists
	if _, err := os.Stat(pm.profilesFile); os.IsNotExist(err) {
		// File doesn't exist, return empty profiles
		return profiles, nil
	}

	// Read profiles file
	data, err := os.ReadFile(pm.profilesFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read profiles file: %v", err)
	}

	// Parse JSON
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, fmt.Errorf("failed to parse profiles file: %v", err)
	}

	// Load session tokens from keyring for each profile
	for name, profile := range profiles {
		profile.AccessToken, _ = pm.keyringManager.Get(ServiceName, fmt.Sprintf("%s:access_token", name))
		profile.RefreshToken, _ = pm.keyringManager.Get(ServiceName, fmt.Sprintf("%s:refresh_token", name))
		profile.SessionID, _ = pm.keyringManager.Get(ServiceName, fmt.Sprintf("%s:session_id", name))

		// Update token expiry information
		profile.UpdateTokenExpiry()
	}

	return profiles, nil
}

// SaveProfiles saves all profiles to storage
func (pm *ProfileManager) SaveProfiles(profiles map[string]*Profile) error {
	// Create profiles directory if it doesn't exist
	profilesDir := filepath.Dir(pm.profilesFile)
	if err := os.MkdirAll(profilesDir, 0o750); err != nil {
		return fmt.Errorf("failed to create profiles directory: %v", err)
	}

	// Save session tokens to keyring and clear from profile objects
	profilesCopy := make(map[string]*Profile)
	for name, profile := range profiles {
		// Create a copy without sensitive data
		profileCopy := *profile

		// Store tokens in keyring if they exist
		if profile.AccessToken != "" {
			pm.keyringManager.Set(ServiceName, fmt.Sprintf("%s:access_token", name), profile.AccessToken)
		}
		if profile.RefreshToken != "" {
			pm.keyringManager.Set(ServiceName, fmt.Sprintf("%s:refresh_token", name), profile.RefreshToken)
		}
		if profile.SessionID != "" {
			pm.keyringManager.Set(ServiceName, fmt.Sprintf("%s:session_id", name), profile.SessionID)
		}

		// Clear sensitive data from copy
		profileCopy.AccessToken = ""
		profileCopy.RefreshToken = ""
		profileCopy.SessionID = ""

		profilesCopy[name] = &profileCopy
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(profilesCopy, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal profiles: %v", err)
	}

	// Write to file
	if err := os.WriteFile(pm.profilesFile, data, 0o600); err != nil {
		return fmt.Errorf("failed to write profiles file: %v", err)
	}

	return nil
}

// GetActiveProfile returns the name of the active profile
func (pm *ProfileManager) GetActiveProfile() (string, error) {
	return pm.keyringManager.Get(ServiceName, ActiveKey)
}

// SetActiveProfile sets the active profile
func (pm *ProfileManager) SetActiveProfile(name string) error {
	return pm.keyringManager.Set(ServiceName, ActiveKey, name)
}

// CreateProfile creates a new profile
func (pm *ProfileManager) CreateProfile(profile *Profile) error {
	profiles, err := pm.LoadProfiles()
	if err != nil {
		return fmt.Errorf("failed to load profiles: %v", err)
	}

	// Check if profile already exists
	if _, exists := profiles[profile.Name]; exists {
		return fmt.Errorf("profile '%s' already exists", profile.Name)
	}

	// Set timestamps
	now := time.Now()
	profile.CreatedAt = now
	profile.UpdatedAt = now

	// Add to profiles
	profiles[profile.Name] = profile

	// Save profiles
	if err := pm.SaveProfiles(profiles); err != nil {
		return fmt.Errorf("failed to save profiles: %v", err)
	}

	return nil
}

// UpdateProfile updates an existing profile
func (pm *ProfileManager) UpdateProfile(profile *Profile) error {
	profiles, err := pm.LoadProfiles()
	if err != nil {
		return fmt.Errorf("failed to load profiles: %v", err)
	}

	// Check if profile exists
	existing, exists := profiles[profile.Name]
	if !exists {
		return fmt.Errorf("profile '%s' does not exist", profile.Name)
	}

	// Preserve creation time and update timestamp
	profile.CreatedAt = existing.CreatedAt
	profile.UpdatedAt = time.Now()

	// Update profile
	profiles[profile.Name] = profile

	// Save profiles
	if err := pm.SaveProfiles(profiles); err != nil {
		return fmt.Errorf("failed to save profiles: %v", err)
	}

	return nil
}

// DeleteProfile deletes a profile
func (pm *ProfileManager) DeleteProfile(name string) error {
	profiles, err := pm.LoadProfiles()
	if err != nil {
		return fmt.Errorf("failed to load profiles: %v", err)
	}

	// Check if profile exists
	if _, exists := profiles[name]; !exists {
		return fmt.Errorf("profile '%s' does not exist", name)
	}

	// Delete from profiles
	delete(profiles, name)

	// Clear tokens from keyring
	pm.keyringManager.Delete(ServiceName, fmt.Sprintf("%s:access_token", name))
	pm.keyringManager.Delete(ServiceName, fmt.Sprintf("%s:refresh_token", name))
	pm.keyringManager.Delete(ServiceName, fmt.Sprintf("%s:session_id", name))

	// If this was the active profile, clear it
	activeProfile, _ := pm.GetActiveProfile()
	if activeProfile == name {
		pm.keyringManager.Delete(ServiceName, ActiveKey)
	}

	// Save profiles
	if err := pm.SaveProfiles(profiles); err != nil {
		return fmt.Errorf("failed to save profiles: %v", err)
	}

	return nil
}

// GetProfile returns a specific profile
func (pm *ProfileManager) GetProfile(name string) (*Profile, error) {
	profiles, err := pm.LoadProfiles()
	if err != nil {
		return nil, fmt.Errorf("failed to load profiles: %v", err)
	}

	profile, exists := profiles[name]
	if !exists {
		return nil, fmt.Errorf("profile '%s' does not exist", name)
	}

	return profile, nil
}

// ListProfiles returns all profiles
func (pm *ProfileManager) ListProfiles() (map[string]*Profile, error) {
	return pm.LoadProfiles()
}

// JWTClaims represents the standard JWT claims
type JWTClaims struct {
	Exp int64  `json:"exp"`
	Iat int64  `json:"iat"`
	Sub string `json:"sub"`
}

// parseJWTToken extracts expiration time from a JWT token
func parseJWTToken(token string) (*JWTClaims, error) {
	if token == "" {
		return nil, fmt.Errorf("empty token")
	}

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

// UpdateTokenExpiry updates the token expiry information for a profile
func (p *Profile) UpdateTokenExpiry() {
	// Parse access token expiry
	if accessClaims, err := parseJWTToken(p.AccessToken); err == nil {
		p.AccessTokenExpiry = time.Unix(accessClaims.Exp, 0)
	} else {
		p.AccessTokenExpiry = time.Time{} // Zero time if parsing fails
	}

	// Parse refresh token expiry
	if refreshClaims, err := parseJWTToken(p.RefreshToken); err == nil {
		p.RefreshTokenExpiry = time.Unix(refreshClaims.Exp, 0)
	} else {
		p.RefreshTokenExpiry = time.Time{} // Zero time if parsing fails
	}
}

// IsAccessTokenExpired checks if the access token is expired
func (p *Profile) IsAccessTokenExpired() bool {
	if p.AccessTokenExpiry.IsZero() {
		return true // Consider expired if we can't parse the expiry
	}
	return time.Now().After(p.AccessTokenExpiry)
}

// IsRefreshTokenExpired checks if the refresh token is expired
func (p *Profile) IsRefreshTokenExpired() bool {
	if p.RefreshTokenExpiry.IsZero() {
		return true // Consider expired if we can't parse the expiry
	}
	return time.Now().After(p.RefreshTokenExpiry)
}

// IsLoggedIn checks if the profile has valid session credentials
func (p *Profile) IsLoggedIn() bool {
	return p.AccessToken != "" && p.RefreshToken != "" && p.SessionID != "" && !p.IsRefreshTokenExpired()
}

// GetLoginStatus returns a human-readable login status
func (p *Profile) GetLoginStatus() string {
	if p.AccessToken == "" || p.RefreshToken == "" || p.SessionID == "" {
		return "Not logged in"
	}

	if p.IsRefreshTokenExpired() {
		return "Session expired"
	}

	if p.IsAccessTokenExpired() {
		return "Token refresh needed"
	}

	return "Logged in"
}

// RefreshTokens refreshes the access token using the refresh token
func (pm *ProfileManager) RefreshTokens(profileName string) error {
	profile, err := pm.GetProfile(profileName)
	if err != nil {
		return fmt.Errorf("failed to get profile: %v", err)
	}

	if profile.RefreshToken == "" {
		return fmt.Errorf("no refresh token available")
	}

	if profile.IsRefreshTokenExpired() {
		return fmt.Errorf("refresh token has expired, please login again")
	}

	// Make refresh request to the client API
	refreshReq := RefreshTokenRequest{
		RefreshToken: profile.RefreshToken,
	}

	// Use a simple HTTP client for the refresh request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Prepare request body
	reqBody, err := json.Marshal(refreshReq)
	if err != nil {
		return fmt.Errorf("failed to marshal refresh request: %v", err)
	}

	// Make the refresh request
	url := fmt.Sprintf("%s/api/v1/auth/refresh", profile.GetTenantURL())
	req, err := http.NewRequest("POST", url, strings.NewReader(string(reqBody)))
	if err != nil {
		return fmt.Errorf("failed to create refresh request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+profile.RefreshToken)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make refresh request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("refresh request failed with status %d", resp.StatusCode)
	}

	// Parse response
	var refreshResp RefreshTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&refreshResp); err != nil {
		return fmt.Errorf("failed to decode refresh response: %v", err)
	}

	// Update profile with new tokens
	profile.AccessToken = refreshResp.AccessToken
	profile.RefreshToken = refreshResp.RefreshToken
	profile.UpdateTokenExpiry()

	// Save updated profile
	return pm.UpdateProfile(profile)
}

// RefreshTokenRequest represents a token refresh request
type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// RefreshTokenResponse represents a token refresh response
type RefreshTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	Status       string `json:"status"`
}
