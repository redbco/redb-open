package cockroach

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

// Connect establishes a connection to a CockroachDB database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	var connString strings.Builder

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
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
		DatabaseType: "cockroach",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a CockroachDB instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	var connString strings.Builder

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
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
		InstanceType: "cockroach",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of a CockroachDB database
func DiscoverDetails(db interface{}) (*CockroachDetails, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	var details CockroachDetails
	details.DatabaseType = "cockroach"

	// Get server version
	var version string
	err := pool.QueryRow(context.Background(), "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details.Version = version

	// Get database size
	var size int64
	err = pool.QueryRow(context.Background(),
		"SELECT pg_database_size(current_database())").Scan(&size)
	if err != nil {
		return nil, fmt.Errorf("error fetching database size: %v", err)
	}
	details.DatabaseSize = size

	// Get unique identifier (database ID)
	var dbID string
	err = pool.QueryRow(context.Background(),
		"SELECT oid::text FROM pg_database WHERE datname = current_database()").Scan(&dbID)
	if err != nil {
		return nil, fmt.Errorf("error fetching database ID: %v", err)
	}
	details.UniqueIdentifier = dbID

	// Get cluster ID
	var clusterID string
	err = pool.QueryRow(context.Background(), "SELECT cluster_id FROM crdb_internal.cluster_info").Scan(&clusterID)
	if err != nil {
		// If we can't get the cluster ID, it's not critical
		clusterID = "unknown"
	}
	details.ClusterID = clusterID

	// Get node ID
	var nodeID string
	err = pool.QueryRow(context.Background(), "SELECT node_id FROM crdb_internal.node_build_info LIMIT 1").Scan(&nodeID)
	if err != nil {
		// If we can't get the node ID, it's not critical
		nodeID = "unknown"
	}
	details.NodeID = nodeID

	// Determine edition
	if strings.Contains(strings.ToLower(version), "enterprise") {
		details.DatabaseEdition = "enterprise"
	} else {
		details.DatabaseEdition = "core"
	}

	return &details, nil
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

// CollectDatabaseMetadata collects metadata from a CockroachDB database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid cockroach connection type")
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

	// Get CockroachDB specific information
	var clusterID string
	err = pool.QueryRow(ctx, "SELECT cluster_id FROM crdb_internal.cluster_info").Scan(&clusterID)
	if err == nil {
		metadata["cluster_id"] = clusterID
	}

	// Get range count
	var rangeCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM crdb_internal.ranges").Scan(&rangeCount)
	if err == nil {
		metadata["range_count"] = rangeCount
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a CockroachDB instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid cockroach connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get uptime (CockroachDB doesn't provide this directly, so we'll use a placeholder)
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

	// Get CockroachDB specific metrics
	// Get node count
	var nodeCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM crdb_internal.gossip_nodes").Scan(&nodeCount)
	if err == nil {
		metadata["node_count"] = nodeCount
	}

	// Get cluster settings
	var clusterSettings []map[string]interface{}
	rows, err := pool.Query(ctx, "SELECT variable, value, type FROM crdb_internal.cluster_settings WHERE type != 'z'")
	if err == nil {
		defer rows.Close()

		for rows.Next() {
			var variable, value, settingType string
			if err := rows.Scan(&variable, &value, &settingType); err == nil {
				clusterSettings = append(clusterSettings, map[string]interface{}{
					"variable": variable,
					"value":    value,
					"type":     settingType,
				})
			}
		}

		if len(clusterSettings) > 0 {
			metadata["cluster_settings"] = clusterSettings
		}
	}

	return metadata, nil
}

// ExecuteCommand executes a command on a CockroachDB database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("invalid cockroach connection type")
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

// CreateDatabase creates a new CockroachDB database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid cockroach connection type")
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

// DropDatabase drops a CockroachDB database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	pool, ok := db.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("invalid cockroach connection type")
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
