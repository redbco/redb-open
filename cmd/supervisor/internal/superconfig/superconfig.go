package superconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Supervisor    SupervisorConfig         `yaml:"supervisor"`
	Database      DatabaseConfig           `yaml:"database"`
	Services      map[string]ServiceConfig `yaml:"services"`
	Logging       LoggingConfig            `yaml:"logging"`
	License       LicenseConfig            `yaml:"license"`
	Global        GlobalConfig             `yaml:"global"`
	Keyring       KeyringConfig            `yaml:"keyring"`
	InstanceGroup InstanceGroupConfig      `yaml:"instance_group"`
}

type SupervisorConfig struct {
	Port                int           `yaml:"port"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	HeartbeatTimeout    time.Duration `yaml:"heartbeat_timeout"`
	ShutdownTimeout     time.Duration `yaml:"shutdown_timeout"`
}

type ServiceConfig struct {
	Enabled      bool              `yaml:"enabled"`
	Required     bool              `yaml:"required"`
	Executable   string            `yaml:"executable"`
	Args         []string          `yaml:"args"`
	Environment  map[string]string `yaml:"environment"`
	Dependencies []string          `yaml:"dependencies"`
	Config       map[string]string `yaml:"config"`
	ExternalPort int               `yaml:"external_port"`
}

type DatabaseConfig struct {
	Name string `yaml:"name"`
}

type LoggingConfig struct {
	Level         string `yaml:"level"`
	RetentionDays int    `yaml:"retention_days"`
	MaxSizeMB     int64  `yaml:"max_size_mb"`
}

type LicenseConfig struct {
	Distribution string `yaml:"distribution"`
}

type GlobalConfig struct {
	MultiTenancy MultiTenancyConfig `yaml:"multi_tenancy"`
}

type MultiTenancyConfig struct {
	Mode              string `yaml:"mode"`                // "single-tenant" or "multi-tenant"
	DefaultTenantID   string `yaml:"default_tenant_id"`   // Used in single-tenant mode
	DefaultTenantName string `yaml:"default_tenant_name"` // Used in single-tenant mode
	DefaultTenantURL  string `yaml:"default_tenant_url"`  // Used in single-tenant mode
}

type KeyringConfig struct {
	Backend     string `yaml:"backend"`      // "system" or "file"
	Path        string `yaml:"path"`         // Path for file-based keyring
	MasterKey   string `yaml:"master_key"`   // Master key for encryption (use env var in production)
	ServiceName string `yaml:"service_name"` // Service name prefix for system keyring
}

type InstanceGroupConfig struct {
	GroupID    string `yaml:"group_id"`    // Unique identifier for this instance group
	PortOffset int    `yaml:"port_offset"` // Port offset to avoid conflicts
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if config.Supervisor.Port == 0 {
		config.Supervisor.Port = 50000
	}
	if config.Supervisor.HealthCheckInterval == 0 {
		config.Supervisor.HealthCheckInterval = 10 * time.Second
	}
	if config.Supervisor.HeartbeatTimeout == 0 {
		config.Supervisor.HeartbeatTimeout = 30 * time.Second
	}
	if config.Supervisor.ShutdownTimeout == 0 {
		config.Supervisor.ShutdownTimeout = 60 * time.Second
	}

	// Set defaults for new configuration sections
	if config.License.Distribution == "" {
		config.License.Distribution = "open-source"
	}

	if config.Global.MultiTenancy.Mode == "" {
		config.Global.MultiTenancy.Mode = "single-tenant"
	}
	if config.Global.MultiTenancy.DefaultTenantID == "" {
		config.Global.MultiTenancy.DefaultTenantID = "default-tenant"
	}
	if config.Global.MultiTenancy.DefaultTenantName == "" {
		config.Global.MultiTenancy.DefaultTenantName = "Default Tenant"
	}
	if config.Global.MultiTenancy.DefaultTenantURL == "" {
		config.Global.MultiTenancy.DefaultTenantURL = "default"
	}

	if config.Keyring.Backend == "" {
		config.Keyring.Backend = "auto" // auto-detect system keyring, fallback to file
	}
	if config.Keyring.ServiceName == "" {
		config.Keyring.ServiceName = "redb"
	}

	if config.InstanceGroup.GroupID == "" {
		config.InstanceGroup.GroupID = "default"
	}
	if config.InstanceGroup.PortOffset == 0 {
		config.InstanceGroup.PortOffset = 0 // Default no offset
	}

	// Validate required configuration
	if config.Database.Name == "" {
		return nil, fmt.Errorf("database.name is required in configuration file")
	}

	return &config, nil
}

func (c *Config) GetServiceStartupOrder() []string {
	// Build dependency graph and return topologically sorted order
	visited := make(map[string]bool)
	visiting := make(map[string]bool) // Track services currently being visited (for cycle detection)
	order := []string{}

	var visit func(string) bool
	visit = func(service string) bool {
		if visited[service] {
			return true // Already processed
		}
		if visiting[service] {
			// Circular dependency detected
			fmt.Printf("Warning: Circular dependency detected involving service '%s'\n", service)
			return false
		}

		visiting[service] = true

		if svcConfig, exists := c.Services[service]; exists {
			// Only process enabled services
			if svcConfig.Enabled {
				for _, dep := range svcConfig.Dependencies {
					if !visit(dep) {
						fmt.Printf("Warning: Failed to resolve dependency '%s' for service '%s'\n", dep, service)
					}
				}
			}
		}

		visiting[service] = false
		visited[service] = true

		// Only add enabled services to the startup order
		if svcConfig, exists := c.Services[service]; exists && svcConfig.Enabled {
			order = append(order, service)
		}

		return true
	}

	// Visit all enabled services
	for service, config := range c.Services {
		if config.Enabled {
			visit(service)
		}
	}

	return order
}

// IsSingleTenant returns true if the system is configured for single-tenant mode
func (c *Config) IsSingleTenant() bool {
	return c.Global.MultiTenancy.Mode == "single-tenant"
}

// GetDefaultTenantID returns the default tenant ID for single-tenant mode
func (c *Config) GetDefaultTenantID() string {
	if c.IsSingleTenant() {
		return c.Global.MultiTenancy.DefaultTenantID
	}
	return ""
}

// GetKeyringPath returns the keyring path with instance group isolation
func (c *Config) GetKeyringPath() string {
	if c.Keyring.Path != "" {
		// If a custom path is specified, use it with group ID suffix
		if c.InstanceGroup.GroupID != "default" {
			return fmt.Sprintf("%s-%s", c.Keyring.Path, c.InstanceGroup.GroupID)
		}
		return c.Keyring.Path
	}

	// Use default path with group ID isolation
	homeDir, err := os.UserHomeDir()
	if err != nil {
		if c.InstanceGroup.GroupID != "default" {
			return fmt.Sprintf("/tmp/redb-keyring-%s.json", c.InstanceGroup.GroupID)
		}
		return "/tmp/redb-keyring.json"
	}

	if c.InstanceGroup.GroupID != "default" {
		return filepath.Join(homeDir, ".local", "share", "redb", fmt.Sprintf("keyring-%s.json", c.InstanceGroup.GroupID))
	}
	return filepath.Join(homeDir, ".local", "share", "redb", "keyring.json")
}

// GetKeyringServiceName returns the keyring service name with instance group isolation
func (c *Config) GetKeyringServiceName(service string) string {
	if c.InstanceGroup.GroupID != "default" {
		return fmt.Sprintf("%s-%s-%s", c.Keyring.ServiceName, c.InstanceGroup.GroupID, service)
	}
	return fmt.Sprintf("%s-%s", c.Keyring.ServiceName, service)
}

// ApplyPortOffset applies the port offset to a base port for multi-instance support
func (c *Config) ApplyPortOffset(basePort int) int {
	return basePort + c.InstanceGroup.PortOffset
}
