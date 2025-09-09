package database

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redbco/redb-open/pkg/config"
)

var (
	instance *PostgreSQL
	once     sync.Once
)

// PostgreSQL represents a PostgreSQL database connection
type PostgreSQL struct {
	pool *pgxpool.Pool
}

type PostgreSQLConfig struct {
	User              string
	Password          string
	Host              string
	Port              int
	Database          string
	SSLMode           string
	MaxConnections    int32
	ConnectionTimeout time.Duration
}

// New creates a new PostgreSQL instance
func New(ctx context.Context, cfg PostgreSQLConfig) (*PostgreSQL, error) {
	// Validate required configuration
	if cfg.Database == "" {
		return nil, fmt.Errorf("database name is required - must be provided in config or REDB_DATABASE_NAME environment variable")
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("database host is required")
	}
	if cfg.User == "" {
		return nil, fmt.Errorf("database user is required")
	}

	// Use pgxpool.ParseConfig to handle special characters in passwords
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("failed to create connection config: %w", err)
	}

	// Set connection parameters individually to avoid URL parsing issues
	poolConfig.ConnConfig.Host = cfg.Host
	poolConfig.ConnConfig.Port = uint16(cfg.Port)
	poolConfig.ConnConfig.Database = cfg.Database
	poolConfig.ConnConfig.User = cfg.User
	poolConfig.ConnConfig.Password = cfg.Password
	poolConfig.ConnConfig.ConnectTimeout = cfg.ConnectionTimeout

	// Set SSL mode through TLS config
	switch cfg.SSLMode {
	case "disable":
		poolConfig.ConnConfig.TLSConfig = nil
	case "require", "prefer":
		// Use default TLS config for these modes
		// pgx will handle the TLS negotiation automatically
	default:
		// For other SSL modes, use default behavior
	}

	// Set pool configuration
	poolConfig.MaxConns = int32(cfg.MaxConnections)
	poolConfig.MaxConnIdleTime = cfg.ConnectionTimeout

	// Create the connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgreSQL{pool: pool}, nil
}

// FromGlobalConfig creates a PostgreSQL config from the global configuration
// If the node has been initialized, it will use keyring credentials
func FromGlobalConfig(cfg *config.Config) PostgreSQLConfig {
	// Try to get production credentials from keyring first
	var databaseName string
	if cfg != nil {
		databaseName = cfg.Get("database.name")
	}
	if prodConfig, err := FromProductionConfig(databaseName); err == nil {
		return prodConfig
	}

	// Use provided database name from config, or try environment variable
	dbName := databaseName
	if dbName == "" {
		// Try to get database name from environment variable
		dbName = os.Getenv("REDB_DATABASE_NAME")
	}

	if dbName == "" {
		// Return error configuration - caller should handle this
		return PostgreSQLConfig{
			User:              "",
			Password:          "",
			Host:              "",
			Port:              0,
			Database:          "",
			SSLMode:           "",
			MaxConnections:    0,
			ConnectionTimeout: 0,
		}
	}

	// Fallback to default configuration
	return PostgreSQLConfig{
		User:              "redb",
		Password:          "redb",
		Host:              "localhost",
		Port:              5432,
		Database:          dbName,
		SSLMode:           "disable",
		MaxConnections:    40,
		ConnectionTimeout: 5 * time.Second,
	}
}

// Pool returns the underlying connection pool
func (db *PostgreSQL) Pool() *pgxpool.Pool {
	return db.pool
}

// Close closes the database connection
func (db *PostgreSQL) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// Initialize creates and sets up the database instance
func Initialize(ctx context.Context, cfg PostgreSQLConfig) error {
	var err error
	once.Do(func() {
		instance, err = New(ctx, cfg)
	})
	return err
}

// GetInstance returns the singleton database instance
func GetInstance() *PostgreSQL {
	if instance == nil {
		panic("database not initialized")
	}
	return instance
}

// CreateDatabase creates the database if it doesn't exist
func CreateDatabase(ctx context.Context, cfg *config.Config, databaseName string) error {
	// Use provided database name, or try environment variable, or return error
	dbName := databaseName
	if dbName == "" {
		dbName = os.Getenv("REDB_DATABASE_NAME")
	}
	if dbName == "" {
		return fmt.Errorf("database name is required - must be provided in config or REDB_DATABASE_NAME environment variable")
	}

	// Connect to default postgres database using pgxpool.ParseConfig to handle special characters
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return fmt.Errorf("failed to create connection config: %w", err)
	}

	// Set connection parameters individually
	poolConfig.ConnConfig.Host = "localhost"
	poolConfig.ConnConfig.Port = 5432
	poolConfig.ConnConfig.Database = "postgres"
	poolConfig.ConnConfig.User = "postgres"
	poolConfig.ConnConfig.Password = "postgres"
	poolConfig.ConnConfig.ConnectTimeout = 30 * time.Second

	// Set SSL mode to disable through TLS config
	poolConfig.ConnConfig.TLSConfig = nil

	defaultPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to default database: %w", err)
	}
	defer defaultPool.Close()

	// Create the database
	_, err = defaultPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}
