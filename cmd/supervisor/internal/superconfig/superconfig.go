package superconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	RestAPIPort  int               `yaml:"rest_api_port"` // REST API port for services that provide HTTP endpoints
}

type DatabaseConfig struct {
	Name string `yaml:"name"`
	User string `yaml:"user"` // Database username for this instance
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

	// Set database defaults
	if config.Database.User == "" {
		config.Database.User = "redb" // Default database user
	}

	// Apply port offset to supervisor port for multi-instance support
	// This must be done after all defaults are set, but we need to avoid double-applying
	// We'll apply the offset directly here since ApplyPortOffset would add it again
	basePort := 50000 // The original default port before any offset
	config.Supervisor.Port = basePort + config.InstanceGroup.PortOffset

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

// Interface implementations for configprovider interfaces

// GetKeyringBackend implements KeyringConfigProvider
func (c *Config) GetKeyringBackend() string {
	return c.Keyring.Backend
}

// GetKeyringMasterKey implements KeyringConfigProvider
func (c *Config) GetKeyringMasterKey() string {
	return c.Keyring.MasterKey
}

// GetKeyringBaseServiceName implements KeyringConfigProvider (base service name)
func (c *Config) GetKeyringBaseServiceName() string {
	return c.Keyring.ServiceName
}

// GetInstanceGroupID implements InstanceConfigProvider
func (c *Config) GetInstanceGroupID() string {
	return c.InstanceGroup.GroupID
}

// GetPortOffset implements InstanceConfigProvider
func (c *Config) GetPortOffset() int {
	return c.InstanceGroup.PortOffset
}

// Note: GetKeyringServiceName(service string) is already implemented above
// and serves as the ServiceNameProvider interface implementation

// GetServiceGRPCAddress implements GRPCServiceProvider
func (c *Config) GetServiceGRPCAddress(serviceName string) string {
	basePort := c.GetServiceBaseGRPCPort(serviceName)
	if basePort == 0 {
		return "" // Service not found or not configured
	}

	// Apply port offset for multi-instance support
	actualPort := c.ApplyPortOffset(basePort)
	return fmt.Sprintf("localhost:%d", actualPort)
}

// GetServiceBaseGRPCPort implements GRPCServiceProvider
func (c *Config) GetServiceBaseGRPCPort(serviceName string) int {
	// Define base gRPC ports for all services
	servicePorts := map[string]int{
		"supervisor":     50000,
		"security":       50051,
		"unifiedmodel":   50052,
		"webhook":        50053,
		"transformation": 50054,
		"core":           50055,
		"mesh":           50056,
		"anchor":         50057,
		"integration":    50058,
		"clientapi":      50059,
		"mcpserver":      50060,
	}

	// Check if service is configured in services section with custom port
	if serviceConfig, exists := c.Services[serviceName]; exists {
		// Try to extract port from args (e.g., "--port=50055")
		for _, arg := range serviceConfig.Args {
			if strings.HasPrefix(arg, "--port=") {
				portStr := strings.TrimPrefix(arg, "--port=")
				if port, err := strconv.Atoi(portStr); err == nil {
					return port
				}
			}
		}
	}

	// Return default port for the service
	if port, exists := servicePorts[serviceName]; exists {
		return port
	}

	return 0 // Service not found
}

// GetDatabaseName implements DatabaseConfigProvider
func (c *Config) GetDatabaseName() string {
	return c.Database.Name
}

// GetDatabaseUser implements DatabaseConfigProvider
func (c *Config) GetDatabaseUser() string {
	return c.Database.User
}

// GetServiceConfig implements ServiceConfigProvider
func (c *Config) GetServiceConfig(serviceName string) map[string]string {
	if serviceConfig, exists := c.Services[serviceName]; exists {
		return serviceConfig.Config
	}
	return nil
}

// GetServiceExternalPort implements ServiceConfigProvider
func (c *Config) GetServiceExternalPort(serviceName string) int {
	if serviceConfig, exists := c.Services[serviceName]; exists {
		// First check if there's a direct external_port field
		if serviceConfig.ExternalPort > 0 {
			return serviceConfig.ExternalPort
		}

		// Then check in the config map
		if serviceConfig.Config != nil {
			// Try different possible config keys
			portKeys := []string{
				fmt.Sprintf("services.%s.external_port", serviceName),
				"external_port",
			}
			for _, key := range portKeys {
				if portStr, ok := serviceConfig.Config[key]; ok {
					if port, err := strconv.Atoi(portStr); err == nil {
						return port
					}
				}
			}
		}
	}
	return 0
}
