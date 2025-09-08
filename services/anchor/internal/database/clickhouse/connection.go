package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// ClickhouseConn is a wrapper interface that combines the necessary methods
// from the Clickhouse driver.Conn interface
type ClickhouseConn interface {
	Query(ctx context.Context, query string, args ...interface{}) (chdriver.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) chdriver.Row
	Exec(ctx context.Context, query string, args ...interface{}) error
	PrepareBatch(ctx context.Context, query string) (chdriver.Batch, error)
	Close() error
}

// Connect establishes a connection to a Clickhouse database
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {

	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
		decryptedPassword = dp
	}

	// Build connection options
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.DatabaseName,
			Username: config.Username,
			Password: decryptedPassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 10,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	}

	// Configure TLS if SSL is enabled
	if config.SSL {
		tlsConfig := &tls.Config{}

		if config.SSLRootCert != "" {
			tlsConfig.RootCAs = x509.NewCertPool()
			tlsConfig.RootCAs.AppendCertsFromPEM([]byte(config.SSLRootCert))
		}

		if config.SSLCert != "" && config.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(config.SSLCert, config.SSLKey)
			if err != nil {
				return nil, fmt.Errorf("error loading TLS certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Set InsecureSkipVerify based on SSLRejectUnauthorized
		if config.SSLRejectUnauthorized != nil {
			tlsConfig.InsecureSkipVerify = !*config.SSLRejectUnauthorized
		}

		options.TLS = tlsConfig
	}

	// Create connection
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Clickhouse database: %v", err)
	}

	// Test the connection
	ctx := context.Background()
	if err := testConnection(ctx, conn); err != nil {
		return nil, fmt.Errorf("error testing Clickhouse connection: %v", err)
	}

	return &dbclient.DatabaseClient{
		DB:           conn,
		DatabaseType: "clickhouse",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Clickhouse instance
func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {

	var decryptedPassword string
	if config.Password == "" {
		decryptedPassword = ""
	} else {
		dp, err := encryption.DecryptPassword(config.TenantID, config.Password)
		if err != nil {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
		decryptedPassword = dp
	}

	// Build connection options
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.DatabaseName,
			Username: config.Username,
			Password: decryptedPassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 10,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	}

	// Configure TLS if SSL is enabled
	if config.SSL {
		tlsConfig := &tls.Config{}

		if config.SSLRootCert != "" {
			tlsConfig.RootCAs = x509.NewCertPool()
			tlsConfig.RootCAs.AppendCertsFromPEM([]byte(config.SSLRootCert))
		}

		if config.SSLCert != "" && config.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(config.SSLCert, config.SSLKey)
			if err != nil {
				return nil, fmt.Errorf("error loading TLS certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Set InsecureSkipVerify based on SSLRejectUnauthorized
		if config.SSLRejectUnauthorized != nil {
			tlsConfig.InsecureSkipVerify = !*config.SSLRejectUnauthorized
		}

		options.TLS = tlsConfig
	}

	// Create connection
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Clickhouse database: %v", err)
	}

	// Test the connection
	ctx := context.Background()
	if err := testConnection(ctx, conn); err != nil {
		return nil, fmt.Errorf("error testing Clickhouse connection: %v", err)
	}

	return &dbclient.InstanceClient{
		DB:           conn,
		InstanceType: "clickhouse",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// testConnection tests the Clickhouse connection by executing a simple query
func testConnection(ctx context.Context, conn chdriver.Conn) error {
	rows, err := conn.Query(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	defer rows.Close()
	return nil
}

// DiscoverDetails fetches the details of a Clickhouse database
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	conn, ok := db.(chdriver.Conn)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "clickhouse"

	// Get server version
	var version string
	err := conn.QueryRow(context.Background(), "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details["version"] = version

	// Get database size (approximate)
	var size int64
	err = conn.QueryRow(context.Background(), `
		SELECT sum(bytes) 
		FROM system.parts 
		WHERE active AND database = currentDatabase()
	`).Scan(&size)
	if err != nil {
		return nil, fmt.Errorf("error fetching database size: %v", err)
	}
	details["databaseSize"] = size

	// Get unique identifier (using database name and host as there's no direct equivalent to PostgreSQL's OID)
	var dbName, hostName string
	err = conn.QueryRow(context.Background(), "SELECT currentDatabase(), hostName()").Scan(&dbName, &hostName)
	if err != nil {
		return nil, fmt.Errorf("error fetching database identifier: %v", err)
	}
	details["uniqueIdentifier"] = fmt.Sprintf("%s_%s", hostName, dbName)

	// Determine edition
	if strings.Contains(strings.ToLower(version), "enterprise") {
		details["databaseEdition"] = "enterprise"
	} else {
		details["databaseEdition"] = "community"
	}

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a Clickhouse database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	conn, ok := db.(chdriver.Conn)
	if !ok {
		return nil, fmt.Errorf("invalid clickhouse connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size
	var sizeBytes int64
	err = conn.QueryRow(ctx, `
		SELECT sum(bytes) 
		FROM system.parts 
		WHERE active AND database = currentDatabase()
	`).Scan(&sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get database size: %w", err)
	}
	metadata["size_bytes"] = sizeBytes

	// Get tables count
	var tablesCount int
	err = conn.QueryRow(ctx, `
		SELECT count() 
		FROM system.tables 
		WHERE database = currentDatabase()
	`).Scan(&tablesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Clickhouse instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	conn, ok := db.(chdriver.Conn)
	if !ok {
		return nil, fmt.Errorf("invalid clickhouse connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get uptime
	var uptimeSeconds int64
	err = conn.QueryRow(ctx, "SELECT uptime()").Scan(&uptimeSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to get uptime: %w", err)
	}
	metadata["uptime_seconds"] = uptimeSeconds

	// Get total databases
	var totalDatabases int
	err = conn.QueryRow(ctx, "SELECT count() FROM system.databases").Scan(&totalDatabases)
	if err != nil {
		return nil, fmt.Errorf("failed to get total databases: %w", err)
	}
	metadata["total_databases"] = totalDatabases

	// Get current connections
	var totalConnections int
	err = conn.QueryRow(ctx, "SELECT count() FROM system.processes").Scan(&totalConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get total connections: %w", err)
	}
	metadata["total_connections"] = totalConnections

	// Get max connections (not directly available in Clickhouse, using a reasonable default)
	metadata["max_connections"] = 100

	return metadata, nil
}

// ExecuteCommand executes a command on a ClickHouse database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	conn, ok := db.(chdriver.Conn)
	if !ok {
		return nil, fmt.Errorf("invalid clickhouse connection type")
	}

	// Execute the command
	rows, err := conn.Query(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}
	defer rows.Close()

	// Get column information
	columnTypes := rows.ColumnTypes()
	columnNames := make([]string, len(columnTypes))
	for i, colType := range columnTypes {
		columnNames[i] = colType.Name()
	}

	// Collect results in a structured format
	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}
		results = append(results, row)
	}

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during iteration: %w", err)
	}

	// Structure the response for gRPC
	response := map[string]interface{}{
		"columns": columnNames,
		"rows":    results,
		"count":   len(results),
	}

	// Convert to JSON bytes for gRPC transmission
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal results to JSON: %w", err)
	}

	return jsonBytes, nil
}

// CreateDatabase creates a new ClickHouse database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	conn, ok := db.(chdriver.Conn)
	if !ok {
		return fmt.Errorf("invalid clickhouse connection type")
	}

	// Build the CREATE DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString("CREATE DATABASE")

	// Check for IF NOT EXISTS option
	if ifNotExists, ok := options["if_not_exists"].(bool); ok && ifNotExists {
		commandBuilder.WriteString(" IF NOT EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", databaseName))

	// Parse and apply options
	if len(options) > 0 {
		var optionParts []string

		// Engine option
		if engine, ok := options["engine"].(string); ok && engine != "" {
			optionParts = append(optionParts, fmt.Sprintf("ENGINE = %s", engine))
		}

		// Comment option
		if comment, ok := options["comment"].(string); ok && comment != "" {
			optionParts = append(optionParts, fmt.Sprintf("COMMENT '%s'", comment))
		}

		if len(optionParts) > 0 {
			commandBuilder.WriteString(" ")
			commandBuilder.WriteString(strings.Join(optionParts, " "))
		}
	}

	// Create the database
	if err := conn.Exec(ctx, commandBuilder.String()); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase drops a ClickHouse database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	conn, ok := db.(chdriver.Conn)
	if !ok {
		return fmt.Errorf("invalid clickhouse connection type")
	}

	// Build the DROP DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP DATABASE")

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		commandBuilder.WriteString(" IF EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", databaseName))

	// ClickHouse supports SYNC option for immediate deletion
	if sync, ok := options["sync"].(bool); ok && sync {
		commandBuilder.WriteString(" SYNC")
	}

	// Drop the database
	if err := conn.Exec(ctx, commandBuilder.String()); err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
