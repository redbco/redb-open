package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// Adapter implements adapter.DatabaseAdapter for TiDB.
type Adapter struct{}

// NewAdapter creates a new TiDB adapter instance.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.TiDB
}

// Capabilities returns the capability metadata.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.TiDB)
}

// Connect establishes a connection to TiDB.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Build DSN (Data Source Name) for MySQL driver
	dsn := buildDSN(config)

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.TiDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.TiDB,
			config.Host,
			config.Port,
			err,
		)
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

// ConnectInstance establishes an instance-level connection to TiDB.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Build DSN for instance connection
	dsn := buildInstanceDSN(config)

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.TiDB,
			config.Host,
			config.Port,
			err,
		)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.TiDB,
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

// Connection implements adapter.Connection for TiDB.
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
	return dbcapabilities.TiDB
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

// InstanceConnection implements adapter.InstanceConnection for TiDB.
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
	return dbcapabilities.TiDB
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

// ListDatabases lists all databases in the TiDB cluster.
func (ic *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	if !ic.IsConnected() {
		return nil, adapter.ErrConnectionClosed
	}

	query := "SHOW DATABASES"
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
		// Skip system databases
		if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
			databases = append(databases, dbName)
		}
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new database in the TiDB cluster.
func (ic *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	query := fmt.Sprintf("CREATE DATABASE `%s`", name)
	_, err := ic.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase deletes a database from the TiDB cluster.
func (ic *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if !ic.IsConnected() {
		return adapter.ErrConnectionClosed
	}

	query := fmt.Sprintf("DROP DATABASE `%s`", name)
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

// buildDSN builds a MySQL DSN from the connection configuration.
func buildDSN(config adapter.ConnectionConfig) string {
	// Format: user:password@tcp(host:port)/dbname?params
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.DatabaseName,
	)

	// Add common parameters
	dsn += "?parseTime=true&loc=Local"

	return dsn
}

// buildInstanceDSN builds a DSN for instance-level operations.
func buildInstanceDSN(config adapter.InstanceConfig) string {
	// Connect without specifying a database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
	)

	dsn += "?parseTime=true&loc=Local"

	return dsn
}
