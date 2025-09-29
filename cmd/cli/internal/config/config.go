package config

import (
	"fmt"
	"os"
	"path/filepath"

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
	ClientAPI string `yaml:"client_api"`
	// ServiceAPI and QueryAPI have been consolidated into ClientAPI
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
			ClientAPI: "/api/v1",
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

// GetGlobalAPIURLNoAuth constructs the global API base URL without requiring authentication
// This is used for endpoints like setup and tenant listing that don't require login
func GetGlobalAPIURLNoAuth() string {
	hostname := globalConfig.DefaultHost

	// Ensure protocol is included
	if hostname[:7] != "http://" && hostname[:8] != "https://" {
		hostname = "http://" + hostname
	}

	return hostname
}
