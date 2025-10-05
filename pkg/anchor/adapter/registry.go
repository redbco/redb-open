package adapter

import (
	"context"
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Registry manages the registration and retrieval of database adapters.
type Registry struct {
	adapters map[dbcapabilities.DatabaseType]DatabaseAdapter
	mu       sync.RWMutex
}

// NewRegistry creates a new adapter registry.
func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[dbcapabilities.DatabaseType]DatabaseAdapter),
	}
}

// Register registers a database adapter.
// If an adapter for the same database type is already registered, it will be replaced.
func (r *Registry) Register(adapter DatabaseAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()

	dbType := adapter.Type()
	r.adapters[dbType] = adapter
}

// Get retrieves a registered adapter by database type.
// Returns ErrAdapterNotFound if the adapter is not registered.
func (r *Registry) Get(dbType dbcapabilities.DatabaseType) (DatabaseAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	adapter, exists := r.adapters[dbType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrAdapterNotFound, dbType)
	}

	return adapter, nil
}

// GetByName retrieves a registered adapter by database name or alias.
// Returns ErrAdapterNotFound if the adapter is not registered.
func (r *Registry) GetByName(name string) (DatabaseAdapter, error) {
	dbType, ok := dbcapabilities.ParseID(name)
	if !ok {
		return nil, fmt.Errorf("%w: unknown database type '%s'", ErrAdapterNotFound, name)
	}

	return r.Get(dbType)
}

// IsRegistered checks if an adapter is registered for the given database type.
func (r *Registry) IsRegistered(dbType dbcapabilities.DatabaseType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.adapters[dbType]
	return exists
}

// ListRegistered returns a list of all registered database types.
func (r *Registry) ListRegistered() []dbcapabilities.DatabaseType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]dbcapabilities.DatabaseType, 0, len(r.adapters))
	for dbType := range r.adapters {
		types = append(types, dbType)
	}

	return types
}

// Unregister removes an adapter from the registry.
func (r *Registry) Unregister(dbType dbcapabilities.DatabaseType) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.adapters, dbType)
}

// Clear removes all adapters from the registry.
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.adapters = make(map[dbcapabilities.DatabaseType]DatabaseAdapter)
}

// Connect creates a new database connection using the registered adapter.
func (r *Registry) Connect(ctx context.Context, config ConnectionConfig) (Connection, error) {
	dbType, ok := dbcapabilities.ParseID(config.ConnectionType)
	if !ok {
		return nil, NewConfigurationError(
			dbcapabilities.DatabaseType(config.ConnectionType),
			"connectionType",
			fmt.Sprintf("unknown database type: %s", config.ConnectionType),
		)
	}

	adapter, err := r.Get(dbType)
	if err != nil {
		return nil, err
	}

	conn, err := adapter.Connect(ctx, config)
	if err != nil {
		return nil, WrapError(dbType, "connect", err)
	}

	return conn, nil
}

// ConnectInstance creates a new instance connection using the registered adapter.
func (r *Registry) ConnectInstance(ctx context.Context, config InstanceConfig) (InstanceConnection, error) {
	dbType, ok := dbcapabilities.ParseID(config.ConnectionType)
	if !ok {
		return nil, NewConfigurationError(
			dbcapabilities.DatabaseType(config.ConnectionType),
			"connectionType",
			fmt.Sprintf("unknown database type: %s", config.ConnectionType),
		)
	}

	adapter, err := r.Get(dbType)
	if err != nil {
		return nil, err
	}

	conn, err := adapter.ConnectInstance(ctx, config)
	if err != nil {
		return nil, WrapError(dbType, "connect_instance", err)
	}

	return conn, nil
}

// GetCapabilities returns the capabilities for a database type.
func (r *Registry) GetCapabilities(dbType dbcapabilities.DatabaseType) (dbcapabilities.Capability, error) {
	adapter, err := r.Get(dbType)
	if err != nil {
		return dbcapabilities.Capability{}, err
	}

	return adapter.Capabilities(), nil
}

// GetCapabilitiesByName returns the capabilities for a database by name or alias.
func (r *Registry) GetCapabilitiesByName(name string) (dbcapabilities.Capability, error) {
	adapter, err := r.GetByName(name)
	if err != nil {
		return dbcapabilities.Capability{}, err
	}

	return adapter.Capabilities(), nil
}

// globalRegistry is the default global adapter registry.
var globalRegistry = NewRegistry()

// Register registers an adapter in the global registry.
func Register(adapter DatabaseAdapter) {
	globalRegistry.Register(adapter)
}

// Get retrieves an adapter from the global registry.
func Get(dbType dbcapabilities.DatabaseType) (DatabaseAdapter, error) {
	return globalRegistry.Get(dbType)
}

// GetByName retrieves an adapter from the global registry by name.
func GetByName(name string) (DatabaseAdapter, error) {
	return globalRegistry.GetByName(name)
}

// IsRegistered checks if an adapter is registered in the global registry.
func IsRegistered(dbType dbcapabilities.DatabaseType) bool {
	return globalRegistry.IsRegistered(dbType)
}

// ListRegistered returns all registered database types from the global registry.
func ListRegistered() []dbcapabilities.DatabaseType {
	return globalRegistry.ListRegistered()
}

// GlobalRegistry returns the global adapter registry.
func GlobalRegistry() *Registry {
	return globalRegistry
}
