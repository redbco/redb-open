package redshift

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL driver (Redshift compatible)
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// RedshiftClient wraps the SQL database connection for Redshift.
type RedshiftClient struct {
	db           *sql.DB
	databaseName string
}

// NewRedshiftClient creates a new Redshift client from a database connection config.
func NewRedshiftClient(ctx context.Context, cfg adapter.ConnectionConfig) (*RedshiftClient, error) {
	// Build connection string (PostgreSQL format)
	connStr := buildConnectionString(cfg)

	// Open database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open Redshift connection: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping Redshift: %w", err)
	}

	return &RedshiftClient{
		db:           db,
		databaseName: cfg.DatabaseName,
	}, nil
}

// NewRedshiftClientFromInstance creates a new Redshift client from an instance config.
func NewRedshiftClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*RedshiftClient, error) {
	connCfg := adapter.ConnectionConfig{
		Host:         cfg.Host,
		Port:         cfg.Port,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DatabaseName: cfg.DatabaseName,
		SSL:          cfg.SSL,
		SSLMode:      cfg.SSLMode,
	}

	return NewRedshiftClient(ctx, connCfg)
}

// buildConnectionString builds a PostgreSQL-compatible connection string for Redshift.
func buildConnectionString(cfg adapter.ConnectionConfig) string {
	// Default port for Redshift
	port := cfg.Port
	if port == 0 {
		port = 5439
	}

	// Default SSL mode
	sslMode := cfg.SSLMode
	if sslMode == "" {
		if cfg.SSL {
			sslMode = "require"
		} else {
			sslMode = "disable"
		}
	}

	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, port, cfg.Username, cfg.Password, cfg.DatabaseName, sslMode)
}

// Ping tests the Redshift connection.
func (c *RedshiftClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// Close closes the Redshift client.
func (c *RedshiftClient) Close() error {
	return c.db.Close()
}

// ListDatabases lists all databases in the Redshift cluster.
func (c *RedshiftClient) ListDatabases(ctx context.Context) ([]string, error) {
	query := "SELECT datname FROM pg_database WHERE datistemplate = false"
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

// CreateDatabase creates a new database in Redshift.
func (c *RedshiftClient) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	query := fmt.Sprintf("CREATE DATABASE %s", name)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

// DropDatabase drops a database from Redshift.
func (c *RedshiftClient) DropDatabase(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP DATABASE %s", name)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	return nil
}

// DB returns the underlying database connection.
func (c *RedshiftClient) DB() *sql.DB {
	return c.db
}

// GetDatabaseName returns the current database name.
func (c *RedshiftClient) GetDatabaseName() string {
	return c.databaseName
}
