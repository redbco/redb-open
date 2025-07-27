package config

import (
	"os"

	"gopkg.in/yaml.v3"

	"github.com/redbco/redb-open/services/unifiedmodel/internal/classifier/scoring"
)

// Config represents the application configuration
type Config struct {
	Server  ServerConfig         `yaml:"server"`
	Weights scoring.WeightMatrix `yaml:"weights,omitempty"`
	Logging LoggingConfig        `yaml:"logging"`
}

// ServerConfig represents server configuration
type ServerConfig struct {
	Port     int    `yaml:"port"`
	Timezone string `yaml:"timezone"`
}

// LoggingConfig represents logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Load loads configuration from file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// Default returns default configuration
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Port:     8080,
			Timezone: "Europe/Dublin",
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}
}
