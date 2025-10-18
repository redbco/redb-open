package cockroach

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Adapter implements the adapter.DatabaseAdapter interface for CockroachDB.
type Adapter struct{}

// NewAdapter creates a new CockroachDB adapter.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.CockroachDB
}

// Capabilities returns the capabilities metadata for CockroachDB.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.CockroachDB)
}

// Connect establishes a connection to a CockroachDB database.
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
			dbcapabilities.CockroachDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Extract the pool from the client
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, adapter.NewConfigurationError(
			dbcapabilities.CockroachDB,
			"connection",
			"invalid cockroachdb connection type",
		)
	}

	conn := &Connection{
		id:        config.DatabaseID,
		pool:      pool,
		config:    config,
		adapter:   a,
		connected: 1, // Mark as connected
	}

	return conn, nil
}

// ConnectInstance establishes a connection to a CockroachDB instance.
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
			dbcapabilities.CockroachDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Extract the pool from the client
	pool, ok := client.DB.(*pgxpool.Pool)
	if !ok {
		return nil, adapter.NewConfigurationError(
			dbcapabilities.CockroachDB,
			"connection",
			"invalid cockroachdb connection type",
		)
	}

	conn := &InstanceConnection{
		id:        config.InstanceID,
		pool:      pool,
		config:    config,
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// Connection implements adapter.Connection for CockroachDB.
type Connection struct {
	id        string
	pool      *pgxpool.Pool
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
	return dbcapabilities.CockroachDB
}

// IsConnected returns whether the connection is active.
func (c *Connection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// Ping checks if the connection is alive.
func (c *Connection) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

// Close closes the connection.
func (c *Connection) Close() error {
	atomic.StoreInt32(&c.connected, 0)
	c.pool.Close()
	return nil
}

// SchemaOperations returns the schema operator for CockroachDB.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator for CockroachDB.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator for CockroachDB.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator for CockroachDB.
func (c *Connection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{conn: c}
}

// Raw returns the underlying pgxpool.Pool.
func (c *Connection) Raw() interface{} {
	return c.pool
}

// Config returns the connection configuration.
func (c *Connection) Config() adapter.ConnectionConfig {
	return c.config
}

// Adapter returns the database adapter.
func (c *Connection) Adapter() adapter.DatabaseAdapter {
	return c.adapter
}

// InstanceConnection implements adapter.InstanceConnection for CockroachDB.
type InstanceConnection struct {
	id        string
	pool      *pgxpool.Pool
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
	return dbcapabilities.CockroachDB
}

// IsConnected returns whether the connection is active.
func (i *InstanceConnection) IsConnected() bool {
	return atomic.LoadInt32(&i.connected) == 1
}

// Ping checks if the connection is alive.
func (i *InstanceConnection) Ping(ctx context.Context) error {
	return i.pool.Ping(ctx)
}

// Close closes the connection.
func (i *InstanceConnection) Close() error {
	atomic.StoreInt32(&i.connected, 0)
	i.pool.Close()
	return nil
}

// ListDatabases lists all databases in the instance.
func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	query := `SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname`

	rows, err := i.pool.Query(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.CockroachDB, "list_databases", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.CockroachDB, "list_databases", err)
		}
		databases = append(databases, dbName)
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new database in the instance.
func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Use existing CreateDatabase function
	return CreateDatabase(ctx, i.pool, name, options)
}

// DropDatabase drops a database from the instance.
func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Use existing DropDatabase function
	return DropDatabase(ctx, i.pool, name, options)
}

// MetadataOperations returns the metadata operator for the instance.
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}

// Raw returns the underlying pgxpool.Pool.
func (i *InstanceConnection) Raw() interface{} {
	return i.pool
}

// Config returns the instance configuration.
func (i *InstanceConnection) Config() adapter.InstanceConfig {
	return i.config
}

// Adapter returns the database adapter.
func (i *InstanceConnection) Adapter() adapter.DatabaseAdapter {
	return i.adapter
}
