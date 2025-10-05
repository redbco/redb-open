package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/anchor/adapter"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/encryption"
)

// Adapter implements the adapter.DatabaseAdapter interface for PostgreSQL.
type Adapter struct{}

// NewAdapter creates a new PostgreSQL adapter.
func NewAdapter() adapter.DatabaseAdapter {
	return &Adapter{}
}

// Type returns the database type identifier.
func (a *Adapter) Type() dbcapabilities.DatabaseType {
	return dbcapabilities.PostgreSQL
}

// Capabilities returns the capabilities metadata for PostgreSQL.
func (a *Adapter) Capabilities() dbcapabilities.Capability {
	return dbcapabilities.MustGet(dbcapabilities.PostgreSQL)
}

// Connect establishes a connection to a PostgreSQL database.
func (a *Adapter) Connect(ctx context.Context, config adapter.ConnectionConfig) (adapter.Connection, error) {
	// Build connection string
	var connString strings.Builder

	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.PostgreSQL,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build base connection string
	fmt.Fprintf(&connString, "postgres://%s:%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration
	if config.SSL {
		sslMode := a.getSslMode(config)
		fmt.Fprintf(&connString, "?sslmode=%s", sslMode)

		if config.SSLCert != nil && *config.SSLCert != "" && config.SSLKey != nil && *config.SSLKey != "" {
			fmt.Fprintf(&connString, "&sslcert=%s&sslkey=%s", *config.SSLCert, *config.SSLKey)
		}
		if config.SSLRootCert != nil && *config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "&sslrootcert=%s", *config.SSLRootCert)
		}
	} else {
		connString.WriteString("?sslmode=disable")
	}

	// Create connection pool
	pool, err := pgxpool.New(ctx, connString.String())
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.PostgreSQL,
			config.Host,
			config.Port,
			fmt.Errorf("error connecting to database: %w", err),
		)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.PostgreSQL,
			config.Host,
			config.Port,
			fmt.Errorf("error pinging database: %w", err),
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

// ConnectInstance establishes a connection to a PostgreSQL instance.
func (a *Adapter) ConnectInstance(ctx context.Context, config adapter.InstanceConfig) (adapter.InstanceConnection, error) {
	// Build connection string
	var connString strings.Builder

	// Decrypt password
	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, adapter.NewConnectionError(
				dbcapabilities.PostgreSQL,
				config.Host,
				config.Port,
				fmt.Errorf("error decrypting password: %w", err),
			)
		}
		decryptedPassword = dp
	}

	// Build base connection string
	fmt.Fprintf(&connString, "postgres://%s:%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration
	if config.SSL {
		sslMode := a.getInstanceSslMode(config)
		fmt.Fprintf(&connString, "?sslmode=%s", sslMode)

		if config.SSLCert != nil && *config.SSLCert != "" && config.SSLKey != nil && *config.SSLKey != "" {
			fmt.Fprintf(&connString, "&sslcert=%s&sslkey=%s", *config.SSLCert, *config.SSLKey)
		}
		if config.SSLRootCert != nil && *config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "&sslrootcert=%s", *config.SSLRootCert)
		}
	} else {
		connString.WriteString("?sslmode=disable")
	}

	// Create connection pool
	pool, err := pgxpool.New(ctx, connString.String())
	if err != nil {
		return nil, adapter.NewConnectionError(
			dbcapabilities.PostgreSQL,
			config.Host,
			config.Port,
			fmt.Errorf("error connecting to database: %w", err),
		)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, adapter.NewConnectionError(
			dbcapabilities.PostgreSQL,
			config.Host,
			config.Port,
			fmt.Errorf("error pinging database: %w", err),
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

// getSslMode returns the appropriate SSL mode for database connection
func (a *Adapter) getSslMode(config adapter.ConnectionConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "verify-ca"
	}
	return "verify-full"
}

// getInstanceSslMode returns the appropriate SSL mode for instance connection
func (a *Adapter) getInstanceSslMode(config adapter.InstanceConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "verify-ca"
	}
	return "verify-full"
}

// Connection implements adapter.Connection for PostgreSQL.
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
	return dbcapabilities.PostgreSQL
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

// SchemaOperations returns the schema operator for PostgreSQL.
func (c *Connection) SchemaOperations() adapter.SchemaOperator {
	return &SchemaOps{conn: c}
}

// DataOperations returns the data operator for PostgreSQL.
func (c *Connection) DataOperations() adapter.DataOperator {
	return &DataOps{conn: c}
}

// ReplicationOperations returns the replication operator for PostgreSQL.
func (c *Connection) ReplicationOperations() adapter.ReplicationOperator {
	return &ReplicationOps{conn: c}
}

// MetadataOperations returns the metadata operator for PostgreSQL.
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

// InstanceConnection implements adapter.InstanceConnection for PostgreSQL.
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
	return dbcapabilities.PostgreSQL
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
	query := `
		SELECT datname 
		FROM pg_database 
		WHERE datistemplate = false
		ORDER BY datname
	`

	rows, err := i.pool.Query(ctx, query)
	if err != nil {
		return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_databases", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, adapter.WrapError(dbcapabilities.PostgreSQL, "list_databases", err)
		}
		databases = append(databases, dbName)
	}

	return databases, rows.Err()
}

// CreateDatabase creates a new database in the instance.
func (i *InstanceConnection) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Build the CREATE DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE DATABASE %s", name))

	// Parse and apply options
	if len(options) > 0 {
		commandBuilder.WriteString(" WITH")

		var optionParts []string

		if owner, ok := options["owner"].(string); ok && owner != "" {
			optionParts = append(optionParts, fmt.Sprintf(" OWNER = %s", owner))
		}

		if template, ok := options["template"].(string); ok && template != "" {
			optionParts = append(optionParts, fmt.Sprintf(" TEMPLATE = %s", template))
		}

		if encoding, ok := options["encoding"].(string); ok && encoding != "" {
			optionParts = append(optionParts, fmt.Sprintf(" ENCODING = '%s'", encoding))
		}

		if lcCollate, ok := options["lc_collate"].(string); ok && lcCollate != "" {
			optionParts = append(optionParts, fmt.Sprintf(" LC_COLLATE = '%s'", lcCollate))
		}

		if lcCtype, ok := options["lc_ctype"].(string); ok && lcCtype != "" {
			optionParts = append(optionParts, fmt.Sprintf(" LC_CTYPE = '%s'", lcCtype))
		}

		if tablespace, ok := options["tablespace"].(string); ok && tablespace != "" {
			optionParts = append(optionParts, fmt.Sprintf(" TABLESPACE = %s", tablespace))
		}

		if connectionLimit, ok := options["connection_limit"]; ok {
			if limit, validInt := connectionLimit.(int); validInt {
				optionParts = append(optionParts, fmt.Sprintf(" CONNECTION LIMIT = %d", limit))
			}
		}

		commandBuilder.WriteString(strings.Join(optionParts, ""))
	}

	// Create the database
	_, err := i.pool.Exec(ctx, commandBuilder.String())
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "create_database", err)
	}

	return nil
}

// DropDatabase drops a database from the instance.
func (i *InstanceConnection) DropDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	// Build the DROP DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP DATABASE")

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		commandBuilder.WriteString(" IF EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", name))

	// Check for CASCADE/RESTRICT options
	if cascade, ok := options["cascade"].(bool); ok && cascade {
		commandBuilder.WriteString(" CASCADE")
	} else if restrict, ok := options["restrict"].(bool); ok && restrict {
		commandBuilder.WriteString(" RESTRICT")
	}

	// Drop the database
	_, err := i.pool.Exec(ctx, commandBuilder.String())
	if err != nil {
		return adapter.WrapError(dbcapabilities.PostgreSQL, "drop_database", err)
	}

	return nil
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
