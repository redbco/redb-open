//go:build enterprise
// +build enterprise

package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/godror/godror" // Oracle driver

	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/encryption"
)

// Adapter implements the adapter.DatabaseAdapter interface for Oracle Database.
type Adapter struct{}

// NewAdapter creates a new Oracle adapter.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.Oracle
}

// Capabilities returns the capabilities metadata for Oracle.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.Oracle)
}

// Connect establishes a connection to an Oracle database.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.Oracle,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build connection string for Oracle
	// Format: user/password@host:port/service_name
	connString := fmt.Sprintf("%s/%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Create connection
	db, err := sql.Open("godror", connString)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Oracle,
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
			dbcapabilities.Oracle,
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

// ConnectInstance establishes a connection to an Oracle instance.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.Oracle,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build connection string for Oracle instance
	dbName := config.DatabaseName
	if dbName == "" {
		dbName = "ORCL" // Default service name
	}

	connString := fmt.Sprintf("%s/%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		dbName)

	// Create connection
	db, err := sql.Open("godror", connString)
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.Oracle,
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
			dbcapabilities.Oracle,
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

// Connection implements adapter.Connection for Oracle.
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
	return dbcapabilities.Oracle
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

// SchemaOperations returns the schema operator for Oracle.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator for Oracle.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator for Oracle.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator for Oracle.
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

// InstanceConnection implements adapter.InstanceConnection for Oracle.
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
	return dbcapabilities.Oracle
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

// ListDatabases lists all databases (pluggable databases) in the instance.
func (i *InstanceConnection) ListDatabases(ctx context.Context) ([]string, error) {
	// In Oracle, query pluggable databases
	query := `
		SELECT PDB_NAME 
		FROM DBA_PDBS 
		WHERE PDB_NAME != 'PDB$SEED'
		ORDER BY PDB_NAME
	`

	rows, err := i.db.QueryContext(ctx, query)
	if err != nil {
		// If DBA_PDBS is not accessible, try V$PDBS
		query = `
			SELECT NAME 
			FROM V$PDBS 
			WHERE NAME != 'PDB$SEED'
			ORDER BY NAME
		`
		rows, err = i.db.QueryContext(ctx, query)
		if err != nil {
			return nil, adapter.WrapError(dbcapabilities.Oracle, "list_databases", err)
		}
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.Oracle, "list_databases", err)
		}
		databases = append(databases, dbName)
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new pluggable database.
func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Build CREATE PLUGGABLE DATABASE command
	commandBuilder := fmt.Sprintf("CREATE PLUGGABLE DATABASE %s", name)

	// Parse and apply options
	if len(options) > 0 {
		if adminUser, ok := options["admin_user"].(string); ok && adminUser != "" {
			if adminPwd, ok := options["admin_password"].(string); ok && adminPwd != "" {
				commandBuilder += fmt.Sprintf(" ADMIN USER %s IDENTIFIED BY %s", adminUser, adminPwd)
			}
		}

		if fileNameConvert, ok := options["file_name_convert"].(string); ok && fileNameConvert != "" {
			commandBuilder += fmt.Sprintf(" FILE_NAME_CONVERT = (%s)", fileNameConvert)
		}

		if storageMax, ok := options["storage_max"].(string); ok && storageMax != "" {
			commandBuilder += fmt.Sprintf(" STORAGE (MAXSIZE %s)", storageMax)
		}
	}

	// Create the pluggable database
	_, err := i.db.ExecContext(ctx, commandBuilder)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "create_database", err)
	}

	// Open the pluggable database
	_, err = i.db.ExecContext(ctx, fmt.Sprintf("ALTER PLUGGABLE DATABASE %s OPEN", name))
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "create_database", err)
	}

	return nil
}

// DropDatabase drops a pluggable database.
func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Close the pluggable database first
	_, err := i.db.ExecContext(ctx, fmt.Sprintf("ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE", name))
	if err != nil {
		// Continue even if close fails
	}

	// Build DROP PLUGGABLE DATABASE command
	commandBuilder := fmt.Sprintf("DROP PLUGGABLE DATABASE %s", name)

	// Check for INCLUDING DATAFILES option
	if includeDatafiles, ok := options["include_datafiles"].(bool); ok && includeDatafiles {
		commandBuilder += " INCLUDING DATAFILES"
	}

	// Drop the pluggable database
	_, err = i.db.ExecContext(ctx, commandBuilder)
	if err != nil {
		return adapter.WrapError(dbcapabilities.Oracle, "drop_database", err)
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
