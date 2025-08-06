package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/redbco/redb-open/pkg/keyring"
	"gopkg.in/yaml.v3"
)

const (
	ServiceName  = "redb-cli"
	TokenKey     = "access_token"
	RefreshKey   = "refresh_token"
	SessionKey   = "session_id"
	HostnameKey  = "hostname"
	WorkspaceKey = "workspace"
	TenantKey    = "tenant"
)

type Config struct {
	APIEndpoints APIEndpoints `yaml:"api_endpoints"`
	DefaultHost  string       `yaml:"default_host"`
	Timeout      int          `yaml:"timeout"`
}

type APIEndpoints struct {
	ClientAPI  string `yaml:"client_api"`
	ServiceAPI string `yaml:"service_api"`
	QueryAPI   string `yaml:"query_api"`
}

var (
	globalConfig   *Config
	keyringManager *keyring.KeyringManager
)

// Init initializes the configuration from the specified file
func Init(configFile string) error {
	// Initialize keyring manager
	keyringPath := keyring.GetDefaultKeyringPath()
	masterPassword := keyring.GetMasterPasswordFromEnv()
	keyringManager = keyring.NewKeyringManager(keyringPath, masterPassword)

	globalConfig = &Config{
		APIEndpoints: APIEndpoints{
			ClientAPI:  "/api/v1",
			ServiceAPI: "/service/v1",
			QueryAPI:   "/query/v1",
		},
		DefaultHost: "localhost:8080",
		Timeout:     30,
	}

	// Create config directory if it doesn't exist
	configDir := filepath.Dir(configFile)
	if err := os.MkdirAll(configDir, 0o750); err != nil {
		return fmt.Errorf("failed to create config directory: %v", err)
	}

	// Try to read existing config file
	if _, err := os.Stat(configFile); err == nil {
		//nolint:gosec // configFile is constructed internally and safe to read
		data, err := os.ReadFile(configFile)
		if err != nil {
			return fmt.Errorf("failed to read config file: %v", err)
		}

		if err := yaml.Unmarshal(data, globalConfig); err != nil {
			return fmt.Errorf("failed to parse config file: %v", err)
		}
	} else {
		// Create default config file
		data, err := yaml.Marshal(globalConfig)
		if err != nil {
			return fmt.Errorf("failed to marshal default config: %v", err)
		}

		if err := os.WriteFile(configFile, data, 0o600); err != nil {
			return fmt.Errorf("failed to write default config file: %v", err)
		}
	}

	return nil
}

// GetConfig returns the global configuration
func GetConfig() *Config {
	return globalConfig
}

// Keyring operations with fallback
func StoreToken(username, token string) error {
	return keyringManager.Set(ServiceName, fmt.Sprintf("%s:%s", username, TokenKey), token)
}

func GetToken(username string) (string, error) {
	return keyringManager.Get(ServiceName, fmt.Sprintf("%s:%s", username, TokenKey))
}

func StoreRefreshToken(username, token string) error {
	return keyringManager.Set(ServiceName, fmt.Sprintf("%s:%s", username, RefreshKey), token)
}

func GetRefreshToken(username string) (string, error) {
	return keyringManager.Get(ServiceName, fmt.Sprintf("%s:%s", username, RefreshKey))
}

func StoreSessionID(username, sessionID string) error {
	return keyringManager.Set(ServiceName, fmt.Sprintf("%s:%s", username, SessionKey), sessionID)
}

func GetSessionID(username string) (string, error) {
	return keyringManager.Get(ServiceName, fmt.Sprintf("%s:%s", username, SessionKey))
}

func StoreHostname(username, hostname string) error {
	return keyringManager.Set(ServiceName, fmt.Sprintf("%s:%s", username, HostnameKey), hostname)
}

func GetHostname(username string) (string, error) {
	return keyringManager.Get(ServiceName, fmt.Sprintf("%s:%s", username, HostnameKey))
}

func StoreWorkspace(username, workspace string) error {
	return keyringManager.Set(ServiceName, fmt.Sprintf("%s:%s", username, WorkspaceKey), workspace)
}

func GetWorkspace(username string) (string, error) {
	return keyringManager.Get(ServiceName, fmt.Sprintf("%s:%s", username, WorkspaceKey))
}

// GetWorkspaceWithError provides a specific error message when no workspace is selected
func GetWorkspaceWithError(username string) (string, error) {
	workspace, err := GetWorkspace(username)
	if err != nil {
		return "", fmt.Errorf("no workspace selected. Please select a workspace first using 'redb-cli select workspace <workspace-name>' or 'redb-cli workspaces list' to see available workspaces")
	}
	return workspace, nil
}

func StoreTenant(username, tenant string) error {
	return keyringManager.Set(ServiceName, fmt.Sprintf("%s:%s", username, TenantKey), tenant)
}

func GetTenant(username string) (string, error) {
	return keyringManager.Get(ServiceName, fmt.Sprintf("%s:%s", username, TenantKey))
}

func StoreUsername(username string) error {
	return keyringManager.Set(ServiceName, "current_user", username)
}

func GetUsername() (string, error) {
	return keyringManager.Get(ServiceName, "current_user")
}

func ClearCredentials(username string) error {
	// Clear all stored credentials for the user
	if err := keyringManager.Delete(ServiceName, fmt.Sprintf("%s:%s", username, TokenKey)); err != nil {
		return fmt.Errorf("failed to delete token: %v", err)
	}
	if err := keyringManager.Delete(ServiceName, fmt.Sprintf("%s:%s", username, RefreshKey)); err != nil {
		return fmt.Errorf("failed to delete refresh token: %v", err)
	}
	if err := keyringManager.Delete(ServiceName, fmt.Sprintf("%s:%s", username, SessionKey)); err != nil {
		return fmt.Errorf("failed to delete session: %v", err)
	}
	if err := keyringManager.Delete(ServiceName, fmt.Sprintf("%s:%s", username, HostnameKey)); err != nil {
		return fmt.Errorf("failed to delete hostname: %v", err)
	}
	if err := keyringManager.Delete(ServiceName, fmt.Sprintf("%s:%s", username, WorkspaceKey)); err != nil {
		return fmt.Errorf("failed to delete workspace: %v", err)
	}
	if err := keyringManager.Delete(ServiceName, fmt.Sprintf("%s:%s", username, TenantKey)); err != nil {
		return fmt.Errorf("failed to delete tenant: %v", err)
	}
	if err := keyringManager.Delete(ServiceName, "current_user"); err != nil {
		return fmt.Errorf("failed to delete current user: %v", err)
	}
	return nil
}

// GetBaseURL constructs the base URL for API calls
func GetBaseURL() (string, error) {
	username, err := GetUsername()
	if err != nil {
		return "", fmt.Errorf("no user logged in: %v", err)
	}

	hostname, err := GetHostname(username)
	if err != nil {
		hostname = globalConfig.DefaultHost
	}

	// Ensure protocol is included
	if hostname[:7] != "http://" && hostname[:8] != "https://" {
		hostname = "http://" + hostname
	}

	return hostname, nil
}

// GetTenantURL constructs the tenant-specific base URL
func GetTenantURL() (string, error) {
	baseURL, err := GetBaseURL()
	if err != nil {
		return "", err
	}

	username, err := GetUsername()
	if err != nil {
		return "", fmt.Errorf("no user logged in: %v", err)
	}

	tenant, err := GetTenant(username)
	if err != nil {
		return baseURL, nil // Return base URL if no tenant is set
	}

	return fmt.Sprintf("%s/%s", baseURL, tenant), nil
}

// GetServiceAPIURL constructs the service API base URL (port 8081)
func GetServiceAPIURL() (string, error) {
	username, err := GetUsername()
	if err != nil {
		return "", fmt.Errorf("no user logged in: %v", err)
	}

	hostname, err := GetHostname(username)
	if err != nil {
		hostname = globalConfig.DefaultHost
	}

	// Ensure protocol is included
	if hostname[:7] != "http://" && hostname[:8] != "https://" {
		hostname = "http://" + hostname
	}

	// Replace port 8080 with 8081 for service API
	if strings.Contains(hostname, ":8080") {
		hostname = strings.Replace(hostname, ":8080", ":8081", 1)
	} else if !strings.Contains(hostname, ":") {
		// If no port specified, assume it's localhost and add port 8081
		hostname = strings.Replace(hostname, "localhost", "localhost:8081", 1)
		hostname = strings.Replace(hostname, "127.0.0.1", "127.0.0.1:8081", 1)
	}

	return hostname, nil
}
