//go:build enterprise
// +build enterprise

package hana

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/SAP/go-hdb/driver" // SAP HANA driver

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/encryption"
)

// Adapter implements the adapter.DatabaseAdapter interface for SAP HANA.
type Adapter struct{}

// NewAdapter creates a new SAP HANA adapter.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.HANA
}

// Capabilities returns the capabilities metadata for SAP HANA.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.HANA)
}

// Connect establishes a connection to a SAP HANA database.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.HANA,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build connection string for SAP HANA
	// Format: hdb://username:password@host:port?database=dbname
	connString := fmt.Sprintf("hdb://%s:%s@%s:%d?database=%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Create connection
	db, err := sql.Open("hdb", connString)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.HANA,
			config.Host,
			config.Port,
			fmt.Errorf("error connecting to database: %w", err),
		)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.HANA,
			config.Host,
			config.Port,
			fmt.Errorf("error pinging database: %w", err),
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

// ConnectInstance establishes a connection to a SAP HANA instance.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.HANA,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build connection string for SAP HANA instance (SYSTEMDB)
	dbName := config.DatabaseName
	if dbName == "" {
		dbName = "SYSTEMDB"
	}

	connString := fmt.Sprintf("hdb://%s:%s@%s:%d?database=%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		dbName)

	// Create connection
	db, err := sql.Open("hdb", connString)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.HANA,
			config.Host,
			config.Port,
			fmt.Errorf("error connecting to instance: %w", err),
		)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.HANA,
			config.Host,
			config.Port,
			fmt.Errorf("error pinging instance: %w", err),
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

// Connection implements adapter.Connection for SAP HANA.
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
	return dbcapabilities.HANA
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

// SchemaOperations returns the schema operator for SAP HANA.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator for SAP HANA.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator for SAP HANA.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator for SAP HANA.
func (c *Connection) MetadataOperations() adapter.MetadataOperator {
	return &MetadataOps{conn: c}
}

// Raw returns the underlying sql.DB.
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

// InstanceConnection implements adapter.InstanceConnection for SAP HANA.
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
	return dbcapabilities.HANA
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

// ListDatabases lists all tenant databases in the instance.
func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	// Query tenant databases from SYSTEMDB
	query := `
		SELECT DATABASE_NAME 
		FROM SYS.M_DATABASES 
		WHERE DATABASE_NAME != 'SYSTEMDB'
		ORDER BY DATABASE_NAME
	`

	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.HANA, "list_databases", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.HANA, "list_databases", err)
		}
		databases = append(databases, dbName)
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new tenant database.
func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Build CREATE DATABASE command
	commandBuilder := fmt.Sprintf("CREATE DATABASE %s", name)

	// Parse and apply options
	if len(options) > 0 {
		if mode, ok := options["mode"].(string); ok && mode != "" {
			commandBuilder += fmt.Sprintf(" SYSTEM USER PASSWORD %s", mode)
		}
	}

	// Create the tenant database
	_, err := i.db.ExecContext(ctx, commandBuilder)
	if err != nil {
		return adapter.WrapError(dbcapabilities.HANA, "create_database", err)
	}

	return nil
}

// DropDatabase drops a tenant database.
func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Build DROP DATABASE command
	commandBuilder := fmt.Sprintf("DROP DATABASE %s", name)

	// Check for CASCADE option
	if cascade, ok := options["cascade"].(bool); ok && cascade {
		commandBuilder += " CASCADE"
	}

	// Drop the tenant database
	_, err := i.db.ExecContext(ctx, commandBuilder)
	if err != nil {
		return adapter.WrapError(dbcapabilities.HANA, "drop_database", err)
	}

	return nil
}

// MetadataOperations returns the metadata operator for the instance.
func (i *InstanceConnection) MetadataOperations() adapter.MetadataOperator {
	return &InstanceMetadataOps{conn: i}
}

// Raw returns the underlying sql.DB.
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
