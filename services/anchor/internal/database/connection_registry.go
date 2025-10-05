package database

import (
	"context"
	"fmt"
	"sync"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/logger"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// ConnectionRegistry provides connection lifecycle management for watchers.
// It wraps ConnectionManager and provides additional features like listing connections,
// status tracking, and metadata collection.
type ConnectionRegistry struct {
	connMgr *ConnectionManager

	// Track connection info for watchers
	databases          map[string]*dbclient.DatabaseClient
	instances          map[string]*dbclient.InstanceClient
	replicationClients map[string]*dbclient.ReplicationClient
	mu                 sync.RWMutex

	logger *logger.Logger
}

// NewConnectionRegistry creates a new connection registry.
func NewConnectionRegistry(connMgr *ConnectionManager) *ConnectionRegistry {
	return &ConnectionRegistry{
		connMgr:            connMgr,
		databases:          make(map[string]*dbclient.DatabaseClient),
		instances:          make(map[string]*dbclient.InstanceClient),
		replicationClients: make(map[string]*dbclient.ReplicationClient),
	}
}

// SetLogger sets the logger for the registry.
func (r *ConnectionRegistry) SetLogger(logger *logger.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger = logger
}

// ConnectDatabase establishes a database connection and tracks it.
func (r *ConnectionRegistry) ConnectDatabase(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
	// Convert to adapter config
	adapterConfig := adapter.ConnectionConfig{
		DatabaseID:            config.DatabaseID,
		WorkspaceID:           config.WorkspaceID,
		TenantID:              config.TenantID,
		EnvironmentID:         &config.EnvironmentID,
		InstanceID:            config.InstanceID,
		Name:                  config.Name,
		Description:           config.Description,
		DatabaseVendor:        config.DatabaseVendor,
		ConnectionType:        config.ConnectionType,
		Host:                  config.Host,
		Port:                  config.Port,
		Username:              config.Username,
		Password:              config.Password,
		DatabaseName:          config.DatabaseName,
		Enabled:               config.Enabled,
		SSL:                   config.SSL,
		SSLMode:               config.SSLMode,
		SSLRejectUnauthorized: config.SSLRejectUnauthorized,
		SSLCert:               &config.SSLCert,
		SSLKey:                &config.SSLKey,
		SSLRootCert:           &config.SSLRootCert,
		Role:                  config.Role,
		ConnectedToNodeID:     config.ConnectedToNodeID,
		OwnerID:               config.OwnerID,
	}

	// Connect via ConnectionManager
	ctx := context.Background()
	if err := r.connMgr.Connect(ctx, adapterConfig); err != nil {
		return nil, err
	}

	// Get the adapter connection
	conn, err := r.connMgr.GetConnection(config.DatabaseID)
	if err != nil {
		return nil, err
	}

	// Create a legacy-compatible client wrapper
	client := &dbclient.DatabaseClient{
		DB:                conn.Raw(),
		DatabaseType:      config.ConnectionType,
		DatabaseID:        config.DatabaseID,
		Config:            config,
		IsConnected:       1,
		AdapterConnection: conn, // Store adapter connection
	}

	// Track it
	r.mu.Lock()
	r.databases[config.DatabaseID] = client
	r.mu.Unlock()

	return client, nil
}

// ConnectInstance establishes an instance connection and tracks it.
func (r *ConnectionRegistry) ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
	// Convert to adapter config
	adapterConfig := adapter.InstanceConfig{
		InstanceID:            config.InstanceID,
		WorkspaceID:           config.WorkspaceID,
		TenantID:              config.TenantID,
		EnvironmentID:         &config.EnvironmentID,
		Name:                  config.Name,
		Description:           config.Description,
		DatabaseVendor:        config.DatabaseVendor,
		ConnectionType:        config.ConnectionType,
		Host:                  config.Host,
		Port:                  config.Port,
		Username:              config.Username,
		Password:              config.Password,
		DatabaseName:          config.DatabaseName,
		Enabled:               config.Enabled,
		SSL:                   config.SSL,
		SSLMode:               config.SSLMode,
		SSLRejectUnauthorized: config.SSLRejectUnauthorized,
		SSLCert:               &config.SSLCert,
		SSLKey:                &config.SSLKey,
		SSLRootCert:           &config.SSLRootCert,
		Role:                  config.Role,
		ConnectedToNodeID:     config.ConnectedToNodeID,
		OwnerID:               config.OwnerID,
		UniqueIdentifier:      config.UniqueIdentifier,
		Version:               config.Version,
	}

	// Connect via ConnectionManager
	ctx := context.Background()
	if err := r.connMgr.ConnectInstance(ctx, adapterConfig); err != nil {
		return nil, err
	}

	// Get the adapter instance connection
	instance, err := r.connMgr.GetInstance(config.InstanceID)
	if err != nil {
		return nil, err
	}

	// Create a legacy-compatible client wrapper
	client := &dbclient.InstanceClient{
		DB:                instance.Raw(),
		InstanceType:      config.ConnectionType,
		InstanceID:        config.InstanceID,
		Config:            config,
		IsConnected:       1,
		AdapterConnection: instance, // Store adapter connection
	}

	// Track it
	r.mu.Lock()
	r.instances[config.InstanceID] = client
	r.mu.Unlock()

	return client, nil
}

// GetDatabaseClient retrieves a database client.
func (r *ConnectionRegistry) GetDatabaseClient(id string) (*dbclient.DatabaseClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	client, exists := r.databases[id]
	if !exists {
		return nil, fmt.Errorf("database client not found: %s", id)
	}
	return client, nil
}

// GetInstanceClient retrieves an instance client.
func (r *ConnectionRegistry) GetInstanceClient(id string) (*dbclient.InstanceClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	client, exists := r.instances[id]
	if !exists {
		return nil, fmt.Errorf("instance client not found: %s", id)
	}
	return client, nil
}

// DisconnectDatabase disconnects a database.
func (r *ConnectionRegistry) DisconnectDatabase(id string) error {
	ctx := context.Background()

	// Disconnect via ConnectionManager
	err := r.connMgr.Disconnect(ctx, id)
	if err != nil {
		return err
	}

	// Remove from tracking
	r.mu.Lock()
	delete(r.databases, id)
	r.mu.Unlock()

	return nil
}

// DisconnectInstance disconnects an instance.
func (r *ConnectionRegistry) DisconnectInstance(id string) error {
	ctx := context.Background()

	// Disconnect via ConnectionManager
	err := r.connMgr.DisconnectInstance(ctx, id)
	if err != nil {
		return err
	}

	// Remove from tracking
	r.mu.Lock()
	delete(r.instances, id)
	r.mu.Unlock()

	return nil
}

// GetAllDatabaseClientIDs returns all tracked database IDs.
func (r *ConnectionRegistry) GetAllDatabaseClientIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.databases))
	for id := range r.databases {
		ids = append(ids, id)
	}
	return ids
}

// GetAllInstanceClientIDs returns all tracked instance IDs.
func (r *ConnectionRegistry) GetAllInstanceClientIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.instances))
	for id := range r.instances {
		ids = append(ids, id)
	}
	return ids
}

// CheckDatabaseHealth checks if a database connection is healthy.
func (r *ConnectionRegistry) CheckDatabaseHealth(ctx context.Context, id string) error {
	return r.connMgr.CheckHealth(ctx, id)
}

// CheckInstanceHealth checks if an instance connection is healthy.
func (r *ConnectionRegistry) CheckInstanceHealth(ctx context.Context, id string) error {
	return r.connMgr.CheckInstanceHealth(ctx, id)
}

// GetActiveReplicationClients returns all active replication clients
func (r *ConnectionRegistry) GetActiveReplicationClients() ([]*dbclient.ReplicationClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var activeClients []*dbclient.ReplicationClient
	for _, client := range r.replicationClients {
		if client != nil {
			activeClients = append(activeClients, client)
		}
	}

	return activeClients, nil
}

// AddReplicationClient tracks a replication client
func (r *ConnectionRegistry) AddReplicationClient(client *dbclient.ReplicationClient) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if client != nil {
		key := fmt.Sprintf("%s:%s", client.DatabaseID, client.ReplicationID)
		r.replicationClients[key] = client
	}
}

// RemoveReplicationClient removes a replication client from tracking
func (r *ConnectionRegistry) RemoveReplicationClient(databaseID, replicationID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := fmt.Sprintf("%s:%s", databaseID, replicationID)
	delete(r.replicationClients, key)
}

// GetReplicationClient retrieves a replication client by ID
func (r *ConnectionRegistry) GetReplicationClient(replicationID string) (*dbclient.ReplicationClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Find the client with matching replication ID
	for _, client := range r.replicationClients {
		if client.ReplicationID == replicationID {
			return client, nil
		}
	}

	return nil, fmt.Errorf("replication client not found: %s", replicationID)
}

// DisconnectReplication disconnects a replication client
func (r *ConnectionRegistry) DisconnectReplication(replicationID string) error {
	client, err := r.GetReplicationClient(replicationID)
	if err != nil {
		return err
	}

	// Stop the replication (implementation depends on client)
	// For now, just remove from tracking
	r.RemoveReplicationClient(client.DatabaseID, replicationID)

	return nil
}
