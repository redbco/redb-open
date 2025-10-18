package mongodb

import (
	"context"
	"sync/atomic"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Adapter implements the adapter.DatabaseAdapter interface for MongoDB.
type Adapter struct{}

// NewAdapter creates a new MongoDB adapter.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.MongoDB
}

// Capabilities returns the capabilities metadata for MongoDB.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.MongoDB)
}

// Connect establishes a connection to a MongoDB database.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Convert adapter config to legacy config
	legacyConfig := dbclient.DatabaseConfig{
		DatabaseID:            config.DatabaseID,
		WorkspaceID:           config.WorkspaceID,
		TenantID:              config.TenantID,
		EnvironmentID:         adapter.GetString(config.EnvironmentID),
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
		SSLCert:               adapter.GetString(config.SSLCert),
		SSLKey:                adapter.GetString(config.SSLKey),
		SSLRootCert:           adapter.GetString(config.SSLRootCert),
		Role:                  config.Role,
		ConnectedToNodeID:     config.ConnectedToNodeID,
		OwnerID:               config.OwnerID,
	}

	// Use existing Connect function
	client, err := Connect(legacyConfig)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.MongoDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Extract the *mongo.Database from the client
	db, ok := client.DB.(*mongo.Database)
	if !ok {
		return nil, adapter.NewConfigurationError(
			dbcapabilities.MongoDB,
			"connection",
			"invalid mongodb connection type",
		)
	}

	conn := &Connection{
		id:        config.DatabaseID,
		db:        db,
		config:    config,
		adapter:   a,
		connected: 1, // Mark as connected
	}

	return conn, nil
}

// ConnectInstance establishes a connection to a MongoDB instance.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Convert adapter config to legacy config
	legacyConfig := dbclient.InstanceConfig{
		InstanceID:            config.InstanceID,
		WorkspaceID:           config.WorkspaceID,
		TenantID:              config.TenantID,
		EnvironmentID:         adapter.GetString(config.EnvironmentID),
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
		SSLCert:               adapter.GetString(config.SSLCert),
		SSLKey:                adapter.GetString(config.SSLKey),
		SSLRootCert:           adapter.GetString(config.SSLRootCert),
		Role:                  config.Role,
		ConnectedToNodeID:     config.ConnectedToNodeID,
		OwnerID:               config.OwnerID,
		UniqueIdentifier:      config.UniqueIdentifier,
		Version:               config.Version,
	}

	// Use existing ConnectInstance function
	client, err := ConnectInstance(legacyConfig)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.MongoDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Extract the *mongo.Client from the client
	mongoClient, ok := client.DB.(*mongo.Client)
	if !ok {
		return nil, adapter.NewConfigurationError(
			dbcapabilities.MongoDB,
			"connection",
			"invalid mongodb connection type",
		)
	}

	conn := &InstanceConnection{
		id:        config.InstanceID,
		client:    mongoClient,
		config:    config,
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// Connection implements adapter.Connection for MongoDB.
type Connection struct {
	id        string
	db        *mongo.Database
	config    adapter.ConnectionConfig
	adapter   *Adapter
	connected int32
}

// ID returns the connection identifier.
func (c *Connection) ID() string {
	return c.id
}

// Type returns the database type.
func (c *Connection) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.MongoDB
}

// IsConnected returns whether the connection is active.
func (c *Connection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// Ping checks if the connection is alive.
func (c *Connection) Ping(ctx context.Context) error {
	return c.db.Client().Ping(ctx, nil)
}

// Close closes the connection.
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	return c.db.Client().Disconnect(context.Background())
}

// SchemaOperations returns the schema operator for MongoDB.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator for MongoDB.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator for MongoDB.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator for MongoDB.
func (c *Connection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{conn: c}
}

// Raw returns the underlying *mongo.Database.
func (c *Connection) Raw() interface{} {
	return c.db
}

// Config returns the connection configuration.
func (c *Connection) Config() adapter.ConnectionConfig {
	return c.config
}

// Adapter returns the database adapter.
func (c *Connection) Adapter() adapter.DatabaseAdapter {
	return c.adapter
}

// InstanceConnection implements adapter.InstanceConnection for MongoDB.
type InstanceConnection struct {
	id        string
	client    *mongo.Client
	config    adapter.InstanceConfig
	adapter   *Adapter
	connected int32
}

// ID returns the instance connection identifier.
func (i *InstanceConnection) ID() string {
	return i.id
}

// Type returns the database type.
func (i *InstanceConnection) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.MongoDB
}

// IsConnected returns whether the connection is active.
func (i *InstanceConnection) IsConnected() bool {
	return atomic.LoadInt32(&i.connected) == 1
}

// Ping checks if the connection is alive.
func (i *InstanceConnection) Ping(ctx context.Context) error {
	return i.client.Ping(ctx, nil)
}

// Close closes the connection.
func (i *InstanceConnection) Close() error {
	atomic.StoreInt32(&i.connected, 0)
	return i.client.Disconnect(context.Background())
}

// ListDatabases lists all databases in the instance.
func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	dbs, err := i.client.ListDatabaseNames(ctx, map[string]interface{}{})
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MongoDB, "list_databases", err)
	}
	return dbs, nil
}

// CreateDatabase creates a new database in the instance.
func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return CreateDatabase(ctx, i.client, name, options)
}

// DropDatabase drops a database from the instance.
func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return DropDatabase(ctx, i.client, name, options)
}

// MetadataOperations returns the metadata operator for the instance.
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}

// Raw returns the underlying *mongo.Client.
func (i *InstanceConnection) Raw() interface{} {
	return i.client
}

// Config returns the instance configuration.
func (i *InstanceConnection) Config() adapter.InstanceConfig {
	return i.config
}

// Adapter returns the database adapter.
func (i *InstanceConnection) Adapter() adapter.DatabaseAdapter {
	return i.adapter
}
