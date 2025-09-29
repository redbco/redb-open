// Package grpcconfig provides utilities for dynamic gRPC service address resolution
// with multi-instance support.
package grpcconfig

import (
	"fmt"
	"os"
	"strconv"

	"github.com/redbco/redb-open/pkg/config"
	"github.com/redbco/redb-open/pkg/configprovider"
)

// GetServiceAddress returns the gRPC address for a service with multi-instance support.
// It first tries to get the address from the configuration provider interface,
// then falls back to environment variables, and finally to hardcoded defaults.
func GetServiceAddress(cfg *config.Config, serviceName string) string {
	// Try to use the configuration provider interface if available
	if grpcProvider, ok := interface{}(cfg).(configprovider.GRPCServiceProvider); ok {
		if addr := grpcProvider.GetServiceGRPCAddress(serviceName); addr != "" {
			return addr
		}
	}

	// Fallback to legacy config keys
	configKey := fmt.Sprintf("services.%s.grpc_address", serviceName)
	if addr := cfg.Get(configKey); addr != "" {
		return addr
	}

	// Fallback to environment variables (for backward compatibility)
	envKey := fmt.Sprintf("GRPC_%s_ADDRESS", serviceName)
	if addr := os.Getenv(envKey); addr != "" {
		return addr
	}

	// Calculate address with port offset using config values directly
	basePort := getServiceBasePort(serviceName)
	if basePort > 0 {
		// Get port offset from config
		portOffset := getPortOffsetFromConfig(cfg)
		actualPort := basePort + portOffset
		return fmt.Sprintf("localhost:%d", actualPort)
	}

	// Final fallback to hardcoded defaults (should not be reached with proper config)
	return getDefaultServiceAddress(serviceName)
}

// getDefaultServiceAddress returns hardcoded default addresses as last resort
func getDefaultServiceAddress(serviceName string) string {
	defaults := map[string]string{
		"supervisor":     "localhost:50000",
		"security":       "localhost:50051",
		"unifiedmodel":   "localhost:50052",
		"webhook":        "localhost:50053",
		"transformation": "localhost:50054",
		"core":           "localhost:50055",
		"mesh":           "localhost:50056",
		"anchor":         "localhost:50057",
		"integration":    "localhost:50058",
		"clientapi":      "localhost:50059",
		"mcpserver":      "localhost:50060",
	}

	if addr, exists := defaults[serviceName]; exists {
		return addr
	}

	return "localhost:50000" // Ultimate fallback
}

// GetServicePort extracts the port number from a service address
func GetServicePort(address string) int {
	if address == "" {
		return 0
	}

	// Handle "host:port" format
	parts := []string{}
	if colonIndex := len(address) - 1; colonIndex > 0 {
		for i := len(address) - 1; i >= 0; i-- {
			if address[i] == ':' {
				parts = []string{address[:i], address[i+1:]}
				break
			}
		}
	}

	if len(parts) == 2 {
		if port, err := strconv.Atoi(parts[1]); err == nil {
			return port
		}
	}

	return 0
}

// ValidateServiceAddress checks if a service address is valid
func ValidateServiceAddress(address string) error {
	if address == "" {
		return fmt.Errorf("service address cannot be empty")
	}

	port := GetServicePort(address)
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid port in service address: %s", address)
	}

	return nil
}

// getServiceBasePort returns the base port for a service (before offset)
func getServiceBasePort(serviceName string) int {
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

	if port, exists := servicePorts[serviceName]; exists {
		return port
	}
	return 0
}

// getPortOffsetFromConfig extracts the port offset from the config
func getPortOffsetFromConfig(cfg *config.Config) int {
	// Try to get port offset from instance_group.port_offset
	if offsetStr := cfg.Get("instance_group.port_offset"); offsetStr != "" {
		if offset, err := strconv.Atoi(offsetStr); err == nil {
			return offset
		}
	}
	return 0 // Default no offset
}
