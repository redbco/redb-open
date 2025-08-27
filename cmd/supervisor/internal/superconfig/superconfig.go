package superconfig

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Supervisor SupervisorConfig         `yaml:"supervisor"`
	Database   DatabaseConfig           `yaml:"database"`
	Services   map[string]ServiceConfig `yaml:"services"`
	Logging    LoggingConfig            `yaml:"logging"`
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

	// Validate required configuration
	if config.Database.Name == "" {
		return nil, fmt.Errorf("database.name is required in configuration file")
	}

	return &config, nil
}

func (c *Config) GetServiceStartupOrder() []string {
	// Build dependency graph and return topologically sorted order
	visited := make(map[string]bool)
	order := []string{}

	var visit func(string)
	visit = func(service string) {
		if visited[service] {
			return
		}
		visited[service] = true

		if svcConfig, exists := c.Services[service]; exists {
			for _, dep := range svcConfig.Dependencies {
				visit(dep)
			}
		}

		order = append(order, service)
	}

	// Visit all services
	for service := range c.Services {
		visit(service)
	}

	return order
}
