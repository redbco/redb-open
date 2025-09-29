// Package configprovider defines interfaces for configuration providers
// to avoid import cycles between internal packages while maintaining type safety.
package configprovider

// KeyringConfigProvider provides keyring configuration settings
type KeyringConfigProvider interface {
	// GetKeyringBackend returns the keyring backend type ("auto", "system", "file")
	GetKeyringBackend() string

	// GetKeyringPath returns the keyring file path (for file-based keyring)
	GetKeyringPath() string

	// GetKeyringMasterKey returns the master key for keyring encryption
	GetKeyringMasterKey() string

	// GetKeyringBaseServiceName returns the base service name for keyring entries
	GetKeyringBaseServiceName() string
}

// InstanceConfigProvider provides instance group configuration settings
type InstanceConfigProvider interface {
	// GetInstanceGroupID returns the unique identifier for this instance group
	GetInstanceGroupID() string

	// GetPortOffset returns the port offset for multi-instance support
	GetPortOffset() int
}

// ServiceNameProvider provides instance-aware service naming
type ServiceNameProvider interface {
	// GetKeyringServiceName returns the keyring service name with instance isolation
	// for the given service component (e.g., "database", "node", "security")
	GetKeyringServiceName(service string) string
}

// GRPCServiceProvider provides gRPC service address configuration
type GRPCServiceProvider interface {
	// GetServiceGRPCAddress returns the gRPC address for a given service
	// with port offset applied for multi-instance support
	GetServiceGRPCAddress(serviceName string) string

	// GetServiceBaseGRPCPort returns the base gRPC port for a service (before offset)
	GetServiceBaseGRPCPort(serviceName string) int
}

// DatabaseConfigProvider provides database configuration
type DatabaseConfigProvider interface {
	// GetDatabaseName returns the database name for this instance
	GetDatabaseName() string

	// GetDatabaseUser returns the database username for this instance
	GetDatabaseUser() string
}

// MultiInstanceConfigProvider combines all configuration interfaces needed
// for multi-instance initialization and service communication
type MultiInstanceConfigProvider interface {
	KeyringConfigProvider
	InstanceConfigProvider
	ServiceNameProvider
	GRPCServiceProvider
	DatabaseConfigProvider
}
