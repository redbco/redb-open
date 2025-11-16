package adapter

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/redbco/redb-open/pkg/streamcapabilities"
)

// NewConnectionConfig creates a new ConnectionConfig with default values.
func NewConnectionConfig(id string, platform streamcapabilities.StreamPlatform) *ConnectionConfig {
	return &ConnectionConfig{
		ID:               id,
		Platform:         platform,
		Configuration:    make(map[string]string),
		Authentication:   make(map[string]string),
		TLSEnabled:       false,
		TLSSkipVerify:    false,
		EnableAutoCommit: true,
		AutoOffsetReset:  "latest",
		Acks:             "all",
		Compression:      "none",
		MaxMessageSize:   1048576, // 1MB default
		ConnectTimeout:   30 * time.Second,
		RequestTimeout:   30 * time.Second,
		Metadata:         make(map[string]interface{}),
	}
}

// Validate checks if the connection configuration is valid.
func (c *ConnectionConfig) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("connection ID is required")
	}

	if c.Platform == "" {
		return fmt.Errorf("platform type is required")
	}

	// Platform-specific validation
	switch c.Platform {
	case "kafka", "redpanda":
		if len(c.Brokers) == 0 {
			return fmt.Errorf("at least one broker is required for %s", c.Platform)
		}
	case "kinesis", "sqs", "sns":
		if c.Region == "" {
			return fmt.Errorf("region is required for %s", c.Platform)
		}
	case "pubsub":
		if c.Project == "" {
			return fmt.Errorf("project is required for Pub/Sub")
		}
	case "eventhubs":
		if c.Namespace == "" {
			return fmt.Errorf("namespace is required for Event Hubs")
		}
	case "mqtt", "nats", "rabbitmq":
		if c.Endpoint == "" && len(c.Brokers) == 0 {
			return fmt.Errorf("endpoint or brokers are required for %s", c.Platform)
		}
	}

	return nil
}

// ToJSON serializes the connection config to JSON.
func (c *ConnectionConfig) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

// FromJSON deserializes a connection config from JSON.
func FromJSON(data []byte) (*ConnectionConfig, error) {
	var config ConnectionConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal connection config: %w", err)
	}
	return &config, nil
}

// Clone creates a deep copy of the connection config.
func (c *ConnectionConfig) Clone() *ConnectionConfig {
	clone := &ConnectionConfig{
		ID:               c.ID,
		Platform:         c.Platform,
		Brokers:          make([]string, len(c.Brokers)),
		Region:           c.Region,
		Project:          c.Project,
		Namespace:        c.Namespace,
		Endpoint:         c.Endpoint,
		Configuration:    make(map[string]string, len(c.Configuration)),
		Username:         c.Username,
		Password:         c.Password,
		SASLMechanism:    c.SASLMechanism,
		CertFile:         c.CertFile,
		KeyFile:          c.KeyFile,
		CAFile:           c.CAFile,
		TLSEnabled:       c.TLSEnabled,
		TLSSkipVerify:    c.TLSSkipVerify,
		Authentication:   make(map[string]string, len(c.Authentication)),
		GroupID:          c.GroupID,
		AutoOffsetReset:  c.AutoOffsetReset,
		EnableAutoCommit: c.EnableAutoCommit,
		Acks:             c.Acks,
		Compression:      c.Compression,
		MaxMessageSize:   c.MaxMessageSize,
		ConnectTimeout:   c.ConnectTimeout,
		RequestTimeout:   c.RequestTimeout,
		Metadata:         make(map[string]interface{}, len(c.Metadata)),
	}

	copy(clone.Brokers, c.Brokers)

	for k, v := range c.Configuration {
		clone.Configuration[k] = v
	}

	for k, v := range c.Authentication {
		clone.Authentication[k] = v
	}

	for k, v := range c.Metadata {
		clone.Metadata[k] = v
	}

	return clone
}

// GetBrokerString returns a comma-separated list of brokers.
func (c *ConnectionConfig) GetBrokerString() string {
	if len(c.Brokers) == 0 {
		return ""
	}

	result := c.Brokers[0]
	for i := 1; i < len(c.Brokers); i++ {
		result += "," + c.Brokers[i]
	}
	return result
}
