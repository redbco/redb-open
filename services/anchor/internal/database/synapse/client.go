package synapse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/microsoft/go-mssqldb" // SQL Server driver
	"github.com/redbco/redb-open/pkg/anchor/adapter"
)

// SynapseClient wraps the SQL database connection for Synapse.
type SynapseClient struct {
	db           *sql.DB
	databaseName string
}

// NewSynapseClient creates a new Synapse client from a database connection config.
func NewSynapseClient(ctx context.Context, cfg adapter.ConnectionConfig) (*SynapseClient, error) {
	// Build connection string (SQL Server format)
	connStr := buildConnectionString(cfg)

	// Open database connection
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open Synapse connection: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping Synapse: %w", err)
	}

	return &SynapseClient{
		db:           db,
		databaseName: cfg.DatabaseName,
	}, nil
}

// NewSynapseClientFromInstance creates a new Synapse client from an instance config.
func NewSynapseClientFromInstance(ctx context.Context, cfg adapter.InstanceConfig) (*SynapseClient, error) {
	connCfg := adapter.ConnectionConfig{
		Host:         cfg.Host,
		Port:         cfg.Port,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DatabaseName: cfg.DatabaseName,
		SSL:          cfg.SSL,
	}

	return NewSynapseClient(ctx, connCfg)
}

// buildConnectionString builds a SQL Server-compatible connection string for Synapse.
func buildConnectionString(cfg adapter.ConnectionConfig) string {
	// Default port for Synapse (SQL Server)
	port := cfg.Port
	if port == 0 {
		port = 1433
	}

	// Build connection string
	query := url.Values{}
	query.Add("database", cfg.DatabaseName)

	if cfg.SSL {
		query.Add("encrypt", "true")
		query.Add("TrustServerCertificate", "false")
	} else {
		query.Add("encrypt", "false")
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cfg.Username, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, port),
		RawQuery: query.Encode(),
	}

	return u.String()
}

// Ping tests the Synapse connection.
func (c *SynapseClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// Close closes the Synapse client.
func (c *SynapseClient) Close() error {
	return c.db.Close()
}

// ListDatabases lists all databases in the Synapse workspace.
func (c *SynapseClient) ListDatabases(ctx context.Context) ([]string, error) {
	query := "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')"
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

// CreateDatabase creates a new database in Synapse.
func (c *SynapseClient) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	query := fmt.Sprintf("CREATE DATABASE [%s]", name)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

// DropDatabase drops a database from Synapse.
func (c *SynapseClient) DropDatabase(ctx context.Context, name string) error {
	query := fmt.Sprintf("DROP DATABASE [%s]", name)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}
	return nil
}

// DB returns the underlying database connection.
func (c *SynapseClient) DB() *sql.DB {
	return c.db
}

// GetDatabaseName returns the current database name.
func (c *SynapseClient) GetDatabaseName() string {
	return c.databaseName
}
