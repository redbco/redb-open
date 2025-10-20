package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Adapter implements adapter.DatabaseAdapter for TimescaleDB.
type Adapter struct{}

// NewAdapter creates a new TimescaleDB adapter instance.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.TimescaleDB
}

// Capabilities returns the capability metadata.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.TimescaleDB)
}

// Connect establishes a connection to TimescaleDB.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Build connection string
	connStr := buildConnectionString(config)

	// Open database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.TimescaleDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.TimescaleDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Verify TimescaleDB extension is installed
	var installed bool
	err = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')").Scan(&installed)
	if err != nil || !installed {
		db.Close()
		return nil, fmt.Errorf("TimescaleDB extension not installed in database")
	}

	conn := &Connection{
		id:        config.DatabaseID,
		db:        db,
		config:    config,
		adapter:   a,
		connected: 1,
	}

	return conn, nil
}

// ConnectInstance establishes an instance-level connection to TimescaleDB.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Build connection string
	connStr := buildInstanceConnectionString(config)

	// Open database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.TimescaleDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.TimescaleDB,
			config.Host,
			config.Port,
			err,
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

// Connection implements adapter.Connection for TimescaleDB.
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
	return dbcapabilities.TimescaleDB
}

// IsConnected returns whether the connection is active.
func (c *Connection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// Ping tests the connection.
func (c *Connection) Ping(ctx context.Context) error {
	if !c.IsConnected() {
		return adapter.ErrConnectionClosed
	}
	return c.db.PingContext(ctx)
}

// Close closes the connection.
func (c *Connection) Close() error {
	if !atomic.CompareAndSwapInt32(&c.connected, 1, 0) {
		return adapter.ErrConnectionClosed
	}
	return c.db.Close()
}

// SchemaOperations returns the schema operator.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator.
func (c *Connection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{conn: c}
}

// Raw returns the underlying database connection.
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

// InstanceConnection implements adapter.InstanceConnection for TimescaleDB.
type InstanceConnection struct {
	id        string
	db        *sql.DB
	config    adapter.InstanceConfig
	adapter   *Adapter
	connected int32
}

// ID returns the instance connection identifier.
func (ic *InstanceConnection) ID() string {
	return ic.id
}

// Type returns the database type.
func (ic *InstanceConnection) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.TimescaleDB
}

// IsConnected returns whether the connection is active.
func (ic *InstanceConnection) IsConnected() bool {
	return atomic.LoadInt32(&ic.connected) == 1
}

// Ping tests the connection.
func (ic *InstanceConnection) Ping(ctx context.Context) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}
	return ic.db.PingContext(ctx)
}

// Close closes the connection.
func (ic *InstanceConnection) Close() error {
	if !atomic.CompareAndSwapInt32(&ic.connected, 1, 0) {
		return adapter.ErrConnectionClosed
	}
	return ic.db.Close()
}

// ListDatabases lists all databases in the TimescaleDB instance.
func (ic *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	if !ic.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	query := `SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname`
	rows, err := ic.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}
		databases = append(databases, dbName)
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new database in the TimescaleDB instance.
func (ic *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	query := fmt.Sprintf("CREATE DATABASE %s", name)
	_, err := ic.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase deletes a database from the TimescaleDB instance.
func (ic *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	query := fmt.Sprintf("DROP DATABASE %s", name)
	_, err := ic.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}

// MetadataOperations returns the metadata operator.
func (ic *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{instanceConn: ic}
}

// Raw returns the underlying database connection.
func (ic *InstanceConnection) Raw() interface{} {
	return ic.db
}

// Config returns the instance configuration.
func (ic *InstanceConnection) Config() adapter.InstanceConfig {
	return ic.config
}

// Adapter returns the database adapter.
func (ic *InstanceConnection) Adapter() adapter.DatabaseAdapter {
	return ic.adapter
}

// buildConnectionString builds a PostgreSQL connection string from the configuration.
func buildConnectionString(config adapter.ConnectionConfig) string {
	sslMode := config.SSLMode
	if sslMode == "" {
		if config.SSL {
			sslMode = "require"
		} else {
			sslMode = "disable"
		}
	}

	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host,
		config.Port,
		config.Username,
		config.Password,
		config.DatabaseName,
		sslMode,
	)
}

// buildInstanceConnectionString builds a connection string for instance-level operations.
func buildInstanceConnectionString(config adapter.InstanceConfig) string {
	sslMode := config.SSLMode
	if sslMode == "" {
		if config.SSL {
			sslMode = "require"
		} else {
			sslMode = "disable"
		}
	}

	dbName := config.DatabaseName
	if dbName == "" {
		dbName = "postgres"
	}

	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host,
		config.Port,
		config.Username,
		config.Password,
		dbName,
		sslMode,
	)
}
