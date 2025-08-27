package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to a PostgreSQL database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	var connString strings.Builder

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

	// Build base connection string
	fmt.Fprintf(&connString, "postgres://%s:%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration
	if config.SSL {
		sslMode := getSslMode(config)
		fmt.Fprintf(&connString, "?sslmode=%s", sslMode)

		if config.SSLCert != "" && config.SSLKey != "" {
			fmt.Fprintf(&connString, "&sslcert=%s&sslkey=%s",
				config.SSLCert, config.SSLKey)
		}
		if config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "&sslrootcert=%s", config.SSLRootCert)
		}
	} else {
		connString.WriteString("?sslmode=disable")
	}

	// Create connection pool
	pool, err := pgxpool.New(context.Background(), connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &common.DatabaseClient{
		DB:           pool,
		DatabaseType: "postgres",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a PostgreSQL instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	var connString strings.Builder

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

	// Build base connection string
	fmt.Fprintf(&connString, "postgres://%s:%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration
	if config.SSL {
		sslMode := getInstanceSslMode(config)
		fmt.Fprintf(&connString, "?sslmode=%s", sslMode)

		if config.SSLCert != "" && config.SSLKey != "" {
			fmt.Fprintf(&connString, "&sslcert=%s&sslkey=%s",
				config.SSLCert, config.SSLKey)
		}
		if config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "&sslrootcert=%s", config.SSLRootCert)
		}
	} else {
		connString.WriteString("?sslmode=disable")
	}

	// Create connection pool
	pool, err := pgxpool.New(context.Background(), connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &common.InstanceClient{
		DB:           pool,
		InstanceType: "postgres",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches basic details of a PostgreSQL database for metadata purposes
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "postgres"

	// Get server version
	var version string
	err := pool.QueryRow(context.Background(), "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details["version"] = version

	// Get database size
	var size int64
	err = pool.QueryRow(context.Background(),
		"SELECT pg_database_size(current_database())").Scan(&size)
	if err != nil {
		return nil, fmt.Errorf("error fetching database size: %v", err)
	}
	details["databaseSize"] = size

	// Get unique identifier (OID)
	var oid string
	err = pool.QueryRow(context.Background(),
		"SELECT oid::text FROM pg_database WHERE datname = current_database()").Scan(&oid)
	if err != nil {
		return nil, fmt.Errorf("error fetching database OID: %v", err)
	}
	details["uniqueIdentifier"] = oid

	// Get database edition
	if strings.Contains(strings.ToLower(version), "enterprise") {
		details["databaseEdition"] = "enterprise"
	} else {
		details["databaseEdition"] = "community"
	}

	return details, nil
}

func getSslMode(config common.DatabaseConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "verify-ca"
	}
	return "verify-full"
}

func getInstanceSslMode(config common.InstanceConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "verify-ca"
	}
	return "verify-full"
}

// CollectDatabaseMetadata collects metadata from a PostgreSQL database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid postgres connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size
	var sizeBytes int64
	err = pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get database size: %w", err)
	}
	metadata["size_bytes"] = sizeBytes

	// Get tables count
	var tablesCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'").Scan(&tablesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a PostgreSQL instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid postgres connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get uptime (PostgreSQL doesn't provide this directly, so we'll use a placeholder)
	var uptimeSeconds int64 = 0
	metadata["uptime_seconds"] = uptimeSeconds

	// Get total databases
	var totalDatabases int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM pg_database WHERE datistemplate = false").Scan(&totalDatabases)
	if err != nil {
		return nil, fmt.Errorf("failed to get total databases: %w", err)
	}
	metadata["total_databases"] = totalDatabases

	// Get current connections
	var totalConnections int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&totalConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get total connections: %w", err)
	}
	metadata["total_connections"] = totalConnections

	// Get max connections
	var maxConnectionsStr string
	err = pool.QueryRow(ctx, "SHOW max_connections").Scan(&maxConnectionsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to get max connections: %w", err)
	}

	// Convert string to integer
	maxConnections, err := strconv.Atoi(maxConnectionsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max connections: %w", err)
	}

	metadata["max_connections"] = maxConnections

	return metadata, nil
}

// ExecuteCommand executes a command on a PostgreSQL database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid postgres connection type")
	}

	// Execute the command
	rows, err := pool.Query(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}
	defer rows.Close()

	// Get column descriptions
	fieldDescriptions := rows.FieldDescriptions()
	columnNames := make([]string, len(fieldDescriptions))
	for i, desc := range fieldDescriptions {
		columnNames[i] = string(desc.Name)
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

// CreateDatabase creates a new PostgreSQL database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid postgres connection type")
	}

	// Build the CREATE DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE DATABASE %s", databaseName))

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
	_, err := pool.Exec(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase drops a PostgreSQL database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid postgres connection type")
	}

	// Build the DROP DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP DATABASE")

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		commandBuilder.WriteString(" IF EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", databaseName))

	// Check for CASCADE/RESTRICT options
	if cascade, ok := options["cascade"].(bool); ok && cascade {
		commandBuilder.WriteString(" CASCADE")
	} else if restrict, ok := options["restrict"].(bool); ok && restrict {
		commandBuilder.WriteString(" RESTRICT")
	}

	// Drop the database
	_, err := pool.Exec(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
