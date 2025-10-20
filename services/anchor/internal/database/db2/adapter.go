//go:build enterprise
// +build enterprise

package db2

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	_ "github.com/ibmdb/go_ibm_db" // IBM DB2 driver

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/encryption"
)

// Adapter implements the adapter.DatabaseAdapter interface for IBM DB2.
type Adapter struct{}

// NewAdapter creates a new IBM DB2 adapter.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.DB2
}

// Capabilities returns the capabilities metadata for IBM DB2.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.DB2)
}

// Connect establishes a connection to an IBM DB2 database.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.DB2,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build connection string for DB2
	// Format: HOSTNAME=host;DATABASE=dbname;PORT=port;UID=username;PWD=password;
	connString := fmt.Sprintf("HOSTNAME=%s;DATABASE=%s;PORT=%d;UID=%s;PWD=%s;",
		config.Host,
		config.DatabaseName,
		config.Port,
		config.Username,
		decryptedPassword)

	// Add SSL configuration if enabled
	if config.SSL {
		sslMode := a.getSslMode(config)
		connString += fmt.Sprintf("Security=%s;", sslMode)

		if config.SSLCert != nil && *config.SSLCert != "" && config.SSLKey != nil && *config.SSLKey != "" {
			connString += fmt.Sprintf("SSLClientKeystoredb=%s;SSLClientKeystash=%s;",
				*config.SSLCert, *config.SSLKey)
		}
		if config.SSLRootCert != nil && *config.SSLRootCert != "" {
			connString += fmt.Sprintf("SSLServerCertificate=%s;", *config.SSLRootCert)
		}
	}

	// Open database connection
	db, err := sql.Open("go_ibm_db", connString)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.DB2,
			config.Host,
			config.Port,
			fmt.Errorf("error connecting to database: %w", err),
		)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.DB2,
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
		connected: 1, // Mark as connected
	}

	return conn, nil
}

// ConnectInstance establishes a connection to an IBM DB2 instance.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.DB2,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build connection string for DB2 instance
	// Connect to the system database or a default database
	dbName := config.DatabaseName
	if dbName == "" {
		dbName = "BLUDB" // Default system database for DB2
	}

	connString := fmt.Sprintf("HOSTNAME=%s;DATABASE=%s;PORT=%d;UID=%s;PWD=%s;",
		config.Host,
		dbName,
		config.Port,
		config.Username,
		decryptedPassword)

	// Add SSL configuration if enabled
	if config.SSL {
		sslMode := a.getInstanceSslMode(config)
		connString += fmt.Sprintf("Security=%s;", sslMode)

		if config.SSLCert != nil && *config.SSLCert != "" && config.SSLKey != nil && *config.SSLKey != "" {
			connString += fmt.Sprintf("SSLClientKeystoredb=%s;SSLClientKeystash=%s;",
				*config.SSLCert, *config.SSLKey)
		}
		if config.SSLRootCert != nil && *config.SSLRootCert != "" {
			connString += fmt.Sprintf("SSLServerCertificate=%s;", *config.SSLRootCert)
		}
	}

	// Open database connection
	db, err := sql.Open("go_ibm_db", connString)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.DB2,
			config.Host,
			config.Port,
			fmt.Errorf("error connecting to instance: %w", err),
		)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.DB2,
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

// getSslMode returns the appropriate SSL mode for database connection
func (a *Adapter) getSslMode(config adapter.ConnectionConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "SSL"
	}
	return "SSL"
}

// getInstanceSslMode returns the appropriate SSL mode for instance connection
func (a *Adapter) getInstanceSslMode(config adapter.InstanceConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "SSL"
	}
	return "SSL"
}

// Connection implements adapter.Connection for IBM DB2.
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
	return dbcapabilities.DB2
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

// SchemaOperations returns the schema operator for IBM DB2.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator for IBM DB2.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator for IBM DB2.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator for IBM DB2.
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

// InstanceConnection implements adapter.InstanceConnection for IBM DB2.
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
	return dbcapabilities.DB2
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
	// DB2 uses a different approach - query catalog tables
	query := `
		SELECT DBNAME 
		FROM SYSCAT.TABLES 
		WHERE TABSCHEMA = 'SYSCAT' AND TABNAME = 'TABLES'
		GROUP BY DBNAME
		ORDER BY DBNAME
	`

	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.DB2, "list_databases", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.DB2, "list_databases", err)
		}
		databases = append(databases, dbName)
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new database in the instance.
func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Build the CREATE DATABASE command with options
	commandBuilder := fmt.Sprintf("CREATE DATABASE %s", name)

	// Parse and apply options
	if len(options) > 0 {
		if codepage, ok := options["codepage"].(string); ok && codepage != "" {
			commandBuilder += fmt.Sprintf(" USING CODESET %s", codepage)
		}

		if territory, ok := options["territory"].(string); ok && territory != "" {
			commandBuilder += fmt.Sprintf(" TERRITORY %s", territory)
		}

		if collate, ok := options["collate"].(string); ok && collate != "" {
			commandBuilder += fmt.Sprintf(" COLLATE USING %s", collate)
		}

		if pagesize, ok := options["pagesize"].(int); ok && pagesize > 0 {
			commandBuilder += fmt.Sprintf(" PAGESIZE %d", pagesize)
		}
	}

	// Create the database
	_, err := i.db.ExecContext(ctx, commandBuilder)
	if err != nil {
		return adapter.WrapError(dbcapabilities.DB2, "create_database", err)
	}

	return nil
}

// DropDatabase drops a database from the instance.
func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Build the DROP DATABASE command
	commandBuilder := fmt.Sprintf("DROP DATABASE %s", name)

	// Drop the database
	_, err := i.db.ExecContext(ctx, commandBuilder)
	if err != nil {
		return adapter.WrapError(dbcapabilities.DB2, "drop_database", err)
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
