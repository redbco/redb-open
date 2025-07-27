package config

import (
	"sync"
)

// Config manages service configuration
type Config struct {
	mu     sync.RWMutex
	values map[string]string

	// Define which keys require restart when changed
	restartKeys []string
}

// New creates a new configuration manager
func New() *Config {
	return &Config{
		values: make(map[string]string),
		restartKeys: []string{
			"database.url",
			"server.port",
			"server.host",
		},
	}
}

// Get retrieves a configuration value
func (c *Config) Get(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.values[key]
}

// GetAll returns a copy of all configuration values
func (c *Config) GetAll() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	copy := make(map[string]string)
	for k, v := range c.values {
		copy[k] = v
	}
	return copy
}

// Update updates configuration values
func (c *Config) Update(values map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range values {
		c.values[k] = v
	}
}

// RequiresRestart checks if any changed keys require a restart
func (c *Config) RequiresRestart(oldConfig map[string]string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, key := range c.restartKeys {
		if oldConfig[key] != c.values[key] {
			return true
		}
	}

	return false
}

// SetRestartKeys sets which configuration keys require restart when changed
func (c *Config) SetRestartKeys(keys []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.restartKeys = keys
}
