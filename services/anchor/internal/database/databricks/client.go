package databricks

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/databricks/databricks-sql-go" // Databricks SQL driver
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// DatabricksClient wraps the SQL database connection for Databricks.
type DatabricksClient struct {
	db           *sql.DB
	databaseName string
}

// NewDatabricksClient creates a new Databricks client from a database connection config.
func NewDatabricksClient(ctx context.Context, cfg adapter.ConnectionConfig) (*DatabricksClient, error) {
	// Build DSN connection string for Databricks
	// Format: databricks://token:<token>@<host>:<port>/<database>?http_path=<http_path>
	connStr := fmt.Sprintf("databricks://token:%s@%s:%d/%s",
		cfg.Token,
		cfg.Host,
		getPort(cfg.Port),
		cfg.DatabaseName,
	)

	// Add http_path if provided in options
	if httpPath, ok := cfg.Options["http_path"].(string); ok && httpPath != "" {
		connStr += "?http_path=" + httpPath
	}

	// Open database connection
	db, err := sql.Open("databricks", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open Databricks connection: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping Databricks: %w", err)
	}

	return &DatabricksClient{
		db:           db,
		databaseName: cfg.DatabaseName,
	}, nil
}

// NewDatabricksClientFromInstance creates a new Databricks client from an instance config.
func NewDatabricksClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*DatabricksClient, error) {
	connCfg := adapter.ConnectionConfig{
		Host:         cfg.Host,
		Port:         cfg.Port,
		Token:        cfg.Token,
		DatabaseName: cfg.DatabaseName,
		Options:      cfg.Options,
	}

	return NewDatabricksClient(ctx, connCfg)
}

// getPort returns the port or default 443 for Databricks.
func getPort(port int) int {
	if port == 0 {
		return 443
	}
	return port
}

// Ping tests the Databricks connection.
func (c *DatabricksClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// Close closes the Databricks client.
func (c *DatabricksClient) Close() error {
	return c.db.Close()
}

// ListDatabases lists all databases (schemas) in Databricks.
func (c *DatabricksClient) ListDatabases(ctx context.Context) ([]string, error) {
	query := "SHOW DATABASES"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}
	defer rows.Close()

	databases := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %w", err)
		}
		databases = append(databases, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating databases: %w", err)
	}

	return databases, nil
}

// CreateDatabase creates a new database (schema) in Databricks.
func (c *DatabricksClient) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

// DropDatabase drops a database (schema) from Databricks.
func (c *DatabricksClient) DropDatabase(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", name)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	return nil
}

// DB returns the underlying database connection.
func (c *DatabricksClient) DB() *sql.DB {
	return c.db
}

// GetDatabaseName returns the current database name.
func (c *DatabricksClient) GetDatabaseName() string {
	return c.databaseName
}
