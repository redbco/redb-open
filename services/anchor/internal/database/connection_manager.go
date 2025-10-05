package database

import (
	"context"
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/logger"
)

// ConnectionManager manages adapter-based database connections
// This is a simplified replacement for DatabaseManager that only handles
// connection lifecycle - all business logic is delegated to adapters.
type ConnectionManager struct {
	connections map[string]adapter.Connection         // Database connections
	instances   map[string]adapter.InstanceConnection // Instance connections
	registry    *adapter.Registry                     // Adapter registry
	mu          sync.RWMutex                          // Protects maps
	logger      *logger.Logger                        // Logger
}

// NewConnectionManager creates a new ConnectionManager instance
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]adapter.Connection),
		instances:   make(map[string]adapter.InstanceConnection),
		registry:    adapter.GlobalRegistry(),
	}
}

// SetLogger sets the logger for the connection manager
func (cm *ConnectionManager) SetLogger(logger *logger.Logger) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.logger = logger
}

// GetLogger returns the logger
func (cm *ConnectionManager) GetLogger() *logger.Logger {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.logger
}

// safeLog safely logs a message if logger is available
func (cm *ConnectionManager) safeLog(level string, format string, args ...interface{}) {
	if cm.logger != nil {
		switch level {
		case "info":
			cm.logger.Info(format, args...)
		case "error":
			cm.logger.Error(format, args...)
		case "warn":
			cm.logger.Warn(format, args...)
		case "debug":
			cm.logger.Debug(format, args...)
		}
	}
}

// Connect establishes a database connection using the appropriate adapter
func (cm *ConnectionManager) Connect(ctx context.Context, cfg adapter.ConnectionConfig) error {
	dbType := dbcapabilities.DatabaseType(cfg.ConnectionType)

	cm.safeLog("info", "Connecting to database %s (type: %s)", cfg.DatabaseID, dbType)

	// Get the appropriate adapter
	adp, err := cm.registry.Get(dbType)
	if err != nil {
		cm.safeLog("error", "No adapter found for database type %s: %v", dbType, err)
		return fmt.Errorf("no adapter found for %s: %w", cfg.ConnectionType, err)
	}

	// Establish connection via adapter (cfg is already adapter.ConnectionConfig)
	conn, err := adp.Connect(ctx, cfg)
	if err != nil {
		cm.safeLog("error", "Failed to connect to database %s: %v", cfg.DatabaseID, err)
		return fmt.Errorf("adapter connection failed: %w", err)
	}

	// Store the connection
	cm.mu.Lock()
	cm.connections[cfg.DatabaseID] = conn
	cm.mu.Unlock()

	cm.safeLog("info", "Successfully connected to database %s", cfg.DatabaseID)
	return nil
}

// ConnectInstance establishes a connection to a database instance (without specific database)
func (cm *ConnectionManager) ConnectInstance(ctx context.Context, cfg adapter.InstanceConfig) error {
	dbType := dbcapabilities.DatabaseType(cfg.ConnectionType)

	cm.safeLog("info", "Connecting to instance %s (type: %s)", cfg.InstanceID, dbType)

	// Get the appropriate adapter
	adp, err := cm.registry.Get(dbType)
	if err != nil {
		cm.safeLog("error", "No adapter found for database type %s: %v", dbType, err)
		return fmt.Errorf("no adapter found for %s: %w", cfg.ConnectionType, err)
	}

	// Establish connection via adapter (cfg is already adapter.InstanceConfig)
	instance, err := adp.ConnectInstance(ctx, cfg)
	if err != nil {
		cm.safeLog("error", "Failed to connect to instance %s: %v", cfg.InstanceID, err)
		return fmt.Errorf("adapter instance connection failed: %w", err)
	}

	// Store the instance connection
	cm.mu.Lock()
	cm.instances[cfg.InstanceID] = instance
	cm.mu.Unlock()

	cm.safeLog("info", "Successfully connected to instance %s", cfg.InstanceID)
	return nil
}

// GetConnection retrieves a database connection by ID
func (cm *ConnectionManager) GetConnection(id string) (adapter.Connection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	conn, exists := cm.connections[id]
	if !exists {
		return nil, fmt.Errorf("connection not found: %s", id)
	}
	return conn, nil
}

// GetInstance retrieves an instance connection by ID
func (cm *ConnectionManager) GetInstance(id string) (adapter.InstanceConnection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	instance, exists := cm.instances[id]
	if !exists {
		return nil, fmt.Errorf("instance connection not found: %s", id)
	}
	return instance, nil
}

// Disconnect closes and removes a database connection
func (cm *ConnectionManager) Disconnect(ctx context.Context, id string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conn, exists := cm.connections[id]
	if !exists {
		return fmt.Errorf("connection not found: %s", id)
	}

	cm.safeLog("info", "Disconnecting database %s", id)

	if err := conn.Close(); err != nil {
		cm.safeLog("error", "Error closing connection %s: %v", id, err)
		return err
	}

	delete(cm.connections, id)
	cm.safeLog("info", "Successfully disconnected database %s", id)
	return nil
}

// DisconnectInstance closes and removes an instance connection
func (cm *ConnectionManager) DisconnectInstance(ctx context.Context, id string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	instance, exists := cm.instances[id]
	if !exists {
		return fmt.Errorf("instance connection not found: %s", id)
	}

	cm.safeLog("info", "Disconnecting instance %s", id)

	if err := instance.Close(); err != nil {
		cm.safeLog("error", "Error closing instance %s: %v", id, err)
		return err
	}

	delete(cm.instances, id)
	cm.safeLog("info", "Successfully disconnected instance %s", id)
	return nil
}

// ListConnections returns a list of all active database connection IDs
func (cm *ConnectionManager) ListConnections() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	ids := make([]string, 0, len(cm.connections))
	for id := range cm.connections {
		ids = append(ids, id)
	}
	return ids
}

// ListInstances returns a list of all active instance connection IDs
func (cm *ConnectionManager) ListInstances() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	ids := make([]string, 0, len(cm.instances))
	for id := range cm.instances {
		ids = append(ids, id)
	}
	return ids
}

// GetConnectionInfo returns connection information for a database
func (cm *ConnectionManager) GetConnectionInfo(id string) (map[string]interface{}, error) {
	conn, err := cm.GetConnection(id)
	if err != nil {
		return nil, err
	}

	config := conn.Config()
	return map[string]interface{}{
		"database_id":     config.DatabaseID,
		"type":            string(conn.Type()),
		"host":            config.Host,
		"port":            config.Port,
		"database_name":   config.DatabaseName,
		"is_connected":    conn.IsConnected(),
		"connection_type": config.ConnectionType,
	}, nil
}

// CheckHealth pings a database connection to verify it's healthy
func (cm *ConnectionManager) CheckHealth(ctx context.Context, id string) error {
	conn, err := cm.GetConnection(id)
	if err != nil {
		return err
	}

	if !conn.IsConnected() {
		return fmt.Errorf("database %s is disconnected", id)
	}

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// CheckInstanceHealth pings an instance connection to verify it's healthy
func (cm *ConnectionManager) CheckInstanceHealth(ctx context.Context, id string) error {
	instance, err := cm.GetInstance(id)
	if err != nil {
		return err
	}

	if !instance.IsConnected() {
		return fmt.Errorf("instance %s is disconnected", id)
	}

	if err := instance.Ping(ctx); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// DisconnectAll closes all connections
func (cm *ConnectionManager) DisconnectAll(ctx context.Context) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.safeLog("info", "Disconnecting all connections")

	var errors []error

	// Disconnect all database connections
	for id, conn := range cm.connections {
		if err := conn.Close(); err != nil {
			cm.safeLog("error", "Error closing connection %s: %v", id, err)
			errors = append(errors, fmt.Errorf("failed to close %s: %w", id, err))
		}
	}
	cm.connections = make(map[string]adapter.Connection)

	// Disconnect all instance connections
	for id, instance := range cm.instances {
		if err := instance.Close(); err != nil {
			cm.safeLog("error", "Error closing instance %s: %v", id, err)
			errors = append(errors, fmt.Errorf("failed to close instance %s: %w", id, err))
		}
	}
	cm.instances = make(map[string]adapter.InstanceConnection)

	if len(errors) > 0 {
		return fmt.Errorf("errors during disconnect: %v", errors)
	}

	cm.safeLog("info", "All connections disconnected")
	return nil
}
