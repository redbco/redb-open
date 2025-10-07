package mysql

import (
	"context"
	"database/sql"
	"sync/atomic"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Adapter implements the adapter.DatabaseAdapter interface for MySQL.
type Adapter struct{}

// NewAdapter creates a new MySQL adapter.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.MySQL
}

// Capabilities returns the capabilities metadata for MySQL.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.MySQL)
}

// Connect establishes a connection to a MySQL database.
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
			dbcapabilities.MySQL,
			config.Host,
			config.Port,
			err,
		)
	}

	// Extract the *sql.DB from the client
	db, ok := client.DB.(*sql.DB)
	if !ok {
		return nil, adapter.NewConfigurationError(
			dbcapabilities.MySQL,
			"connection",
			"invalid mysql connection type",
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

// ConnectInstance establishes a connection to a MySQL instance.
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
			dbcapabilities.MySQL,
			config.Host,
			config.Port,
			err,
		)
	}

	// Extract the *sql.DB from the client
	db, ok := client.DB.(*sql.DB)
	if !ok {
		return nil, adapter.NewConfigurationError(
			dbcapabilities.MySQL,
			"connection",
			"invalid mysql connection type",
		)
	}

	conn := &InstanceConnection{
		id:        config.InstanceID,
		db:        db,
		config:    config,
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// Connection implements adapter.Connection for MySQL.
type Connection struct {
	id        string
	db        *sql.DB
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
	return dbcapabilities.MySQL
}

// IsConnected returns whether the connection is active.
func (c *Connection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// Ping checks if the connection is alive.
func (c *Connection) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// Close closes the connection.
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	return c.db.Close()
}

// SchemaOperations returns the schema operator for MySQL.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator for MySQL.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator for MySQL.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator for MySQL.
func (c *Connection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{conn: c}
}

// Raw returns the underlying *sql.DB.
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

// InstanceConnection implements adapter.InstanceConnection for MySQL.
type InstanceConnection struct {
	id        string
	db        *sql.DB
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
	return dbcapabilities.MySQL
}

// IsConnected returns whether the connection is active.
func (i *InstanceConnection) IsConnected() bool {
	return atomic.LoadInt32(&i.connected) == 1
}

// Ping checks if the connection is alive.
func (i *InstanceConnection) Ping(ctx context.Context) error {
	return i.db.PingContext(ctx)
}

// Close closes the connection.
func (i *InstanceConnection) Close() error {
	atomic.StoreInt32(&i.connected, 0)
	return i.db.Close()
}

// ListDatabases lists all databases in the instance.
func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	query := "SHOW DATABASES"

	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.MySQL, "list_databases", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.MySQL, "list_databases", err)
		}
		databases = append(databases, dbName)
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new database in the instance.
func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return CreateDatabase(ctx, i.db, name, options)
}

// DropDatabase drops a database from the instance.
func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return DropDatabase(ctx, i.db, name, options)
}

// MetadataOperations returns the metadata operator for the instance.
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}

// Raw returns the underlying *sql.DB.
func (i *InstanceConnection) Raw() interface{} {
	return i.db
}

// Config returns the instance configuration.
func (i *InstanceConnection) Config() adapter.InstanceConfig {
	return i.config
}

// Adapter returns the database adapter.
func (i *InstanceConnection) Adapter() adapter.DatabaseAdapter {
	return i.adapter
}
