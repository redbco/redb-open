package common

import (
	"fmt"
	"strings"

	"github.com/redbco/redb-open/cmd/cli/internal/httpclient"
)

// ProfileInfo contains the essential information from an active profile
type ProfileInfo struct {
	Name      string
	TenantURL string
	Username  string
	Workspace string
}

// GetActiveProfileInfo returns the active profile information needed for API calls
func GetActiveProfileInfo() (*ProfileInfo, error) {
	client, err := httpclient.GetProfileClient()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize profile client: %v", err)
	}

	prof, err := client.GetActiveProfile()
	if err != nil {
		return nil, fmt.Errorf("no active profile found. Use 'redb-cli profiles list' to see available profiles or 'redb-cli profiles create <name>' to create one: %v", err)
	}

	if !prof.IsLoggedIn() {
		return nil, fmt.Errorf("profile '%s' is not logged in or session has expired. Use 'redb-cli auth login --profile %s' to login", prof.Name, prof.Name)
	}

	return &ProfileInfo{
		Name:      prof.Name,
		TenantURL: prof.GetTenantURL(),
		Username:  prof.Username,
		Workspace: prof.Workspace,
	}, nil
}

// GetProfileClient returns a profile-aware HTTP client
func GetProfileClient() (*httpclient.ProfileHTTPClient, error) {
	return httpclient.GetProfileClient()
}

// ValidateWorkspace checks if the active profile has a workspace selected
func ValidateWorkspace(profileInfo *ProfileInfo) error {
	if profileInfo.Workspace == "" {
		return fmt.Errorf("no workspace selected for profile '%s'. Use 'redb-cli workspaces list' to see available workspaces", profileInfo.Name)
	}
	return nil
}

// BuildAPIURL constructs API URLs using the profile information
func BuildAPIURL(profileInfo *ProfileInfo, path string) string {
	return fmt.Sprintf("%s/api/v1%s", profileInfo.TenantURL, path)
}

// BuildWorkspaceAPIURL constructs workspace-specific API URLs
func BuildWorkspaceAPIURL(profileInfo *ProfileInfo, path string) (string, error) {
	if err := ValidateWorkspace(profileInfo); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/api/v1/workspaces/%s%s", profileInfo.TenantURL, profileInfo.Workspace, path), nil
}

// BuildGlobalAPIURL constructs global API URLs (without tenant prefix) using profile information
// This is used for endpoints like mesh management that are global rather than tenant-specific
func BuildGlobalAPIURL(profileInfo *ProfileInfo, path string) string {
	// Extract the base hostname from the tenant URL
	// TenantURL format is typically: http://hostname:port/tenant_name
	// We need to extract: http://hostname:port
	tenantURL := profileInfo.TenantURL

	// Find the last slash to separate base URL from tenant name
	lastSlash := strings.LastIndex(tenantURL, "/")
	if lastSlash == -1 {
		// If no slash found, assume the entire URL is the base
		return fmt.Sprintf("%s/api/v1%s", tenantURL, path)
	}

	// Extract base URL (everything before the last slash)
	baseURL := tenantURL[:lastSlash]
	return fmt.Sprintf("%s/api/v1%s", baseURL, path)
}
