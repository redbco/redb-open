package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"github.com/snowflakedb/gosnowflake"
)

// Connect establishes a connection to a Snowflake database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {
	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	var connString strings.Builder

	// Build connection string
	fmt.Fprintf(&connString, "%s:%s@%s/%s",
		config.Username,
		decryptedPassword,
		config.Host, // This should be the account identifier (e.g., "myorg-myaccount")
		config.DatabaseName)

	// Add warehouse if specified
	if config.Role != "" {
		fmt.Fprintf(&connString, "?role=%s", config.Role)
	}

	// Add warehouse if specified in the host field (format: account/warehouse)
	parts := strings.Split(config.Host, "/")
	if len(parts) > 1 {
		fmt.Fprintf(&connString, "&warehouse=%s", parts[1])
	}

	// Configure DSN
	cfg, err := gosnowflake.ParseDSN(connString.String())
	if err != nil {
		return nil, fmt.Errorf("error parsing Snowflake DSN: %v", err)
	}

	// Set additional connection parameters
	cfg.Authenticator = gosnowflake.AuthTypeSnowflake // Default password auth
	cfg.Application = "redb-anchor"

	// Create connection
	db, err := sql.Open("snowflake", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to Snowflake: %v", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging Snowflake database: %v", err)
	}

	return &common.DatabaseClient{
		DB:           db,
		DatabaseType: "snowflake",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Snowflake instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {
	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	var connString strings.Builder

	// Build connection string
	fmt.Fprintf(&connString, "%s:%s@%s/%s",
		config.Username,
		decryptedPassword,
		config.Host, // This should be the account identifier (e.g., "myorg-myaccount")
		config.DatabaseName)

	// Add warehouse if specified
	if config.Role != "" {
		fmt.Fprintf(&connString, "?role=%s", config.Role)
	}

	// Add warehouse if specified in the host field (format: account/warehouse)
	parts := strings.Split(config.Host, "/")
	if len(parts) > 1 {
		fmt.Fprintf(&connString, "&warehouse=%s", parts[1])
	}

	// Configure DSN
	cfg, err := gosnowflake.ParseDSN(connString.String())
	if err != nil {
		return nil, fmt.Errorf("error parsing Snowflake DSN: %v", err)
	}

	// Set additional connection parameters
	cfg.Authenticator = gosnowflake.AuthTypeSnowflake // Default password auth
	cfg.Application = "redb-anchor"

	// Create connection
	db, err := sql.Open("snowflake", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to Snowflake: %v", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging Snowflake database: %v", err)
	}

	return &common.InstanceClient{
		DB:           db,
		InstanceType: "snowflake",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of a Snowflake database
func DiscoverDetails(db interface{}) (*SnowflakeDetails, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	var details SnowflakeDetails
	details.DatabaseType = "snowflake"

	// Get server version
	var version string
	err := sqlDB.QueryRow("SELECT CURRENT_VERSION()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details.Version = version

	// Get account information
	var account, region string
	err = sqlDB.QueryRow("SELECT CURRENT_ACCOUNT(), CURRENT_REGION()").Scan(&account, &region)
	if err != nil {
		return nil, fmt.Errorf("error fetching account information: %v", err)
	}
	details.Account = account
	details.Region = region

	// Get current role
	var role string
	err = sqlDB.QueryRow("SELECT CURRENT_ROLE()").Scan(&role)
	if err != nil {
		return nil, fmt.Errorf("error fetching current role: %v", err)
	}
	details.Role = role

	// Get current warehouse
	var warehouse string
	err = sqlDB.QueryRow("SELECT CURRENT_WAREHOUSE()").Scan(&warehouse)
	if err != nil {
		return nil, fmt.Errorf("error fetching current warehouse: %v", err)
	}
	details.Warehouse = warehouse

	// Get database size (approximate)
	var size int64
	err = sqlDB.QueryRow(`
		SELECT SUM(BYTES) 
		FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(
			SCHEMA_NAME => CURRENT_SCHEMA(),
			TABLE_NAME => NULL
		))
	`).Scan(&size)
	if err != nil {
		// If we can't get the size, just set it to 0 and continue
		size = 0
	}
	details.DatabaseSize = size

	// Get unique identifier (account + database name)
	var dbName string
	err = sqlDB.QueryRow("SELECT CURRENT_DATABASE()").Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("error fetching database name: %v", err)
	}
	details.UniqueIdentifier = fmt.Sprintf("%s.%s", account, dbName)

	// Get edition
	var edition string
	err = sqlDB.QueryRow("SELECT SYSTEM$WHITELIST()").Scan(&edition)
	if err != nil {
		// If we can't determine the edition, default to "standard"
		edition = "standard"
	} else if strings.Contains(strings.ToLower(edition), "enterprise") {
		edition = "enterprise"
	} else if strings.Contains(strings.ToLower(edition), "business") {
		edition = "business"
	} else {
		edition = "standard"
	}
	details.DatabaseEdition = edition

	return &details, nil
}

// CollectDatabaseMetadata collects metadata from a Snowflake database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid snowflake connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT CURRENT_VERSION()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size
	var sizeBytes int64
	err = sqlDB.QueryRowContext(ctx, `
		SELECT SUM(BYTES) 
		FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(
			SCHEMA_NAME => CURRENT_SCHEMA(),
			TABLE_NAME => NULL
		))
	`).Scan(&sizeBytes)
	if err == nil {
		metadata["size_bytes"] = sizeBytes
	} else {
		metadata["size_bytes"] = 0
	}

	// Get tables count
	var tablesCount int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = CURRENT_SCHEMA() 
		AND TABLE_TYPE = 'BASE TABLE'
	`).Scan(&tablesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	// Get current warehouse
	var warehouse string
	err = sqlDB.QueryRowContext(ctx, "SELECT CURRENT_WAREHOUSE()").Scan(&warehouse)
	if err == nil {
		metadata["warehouse"] = warehouse
	}

	// Get current role
	var role string
	err = sqlDB.QueryRowContext(ctx, "SELECT CURRENT_ROLE()").Scan(&role)
	if err == nil {
		metadata["role"] = role
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Snowflake instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid snowflake connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT CURRENT_VERSION()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get account information
	var account, region string
	err = sqlDB.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT(), CURRENT_REGION()").Scan(&account, &region)
	if err == nil {
		metadata["account"] = account
		metadata["region"] = region
	}

	// Get total databases
	var totalDatabases int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.DATABASES
	`).Scan(&totalDatabases)
	if err == nil {
		metadata["total_databases"] = totalDatabases
	} else {
		metadata["total_databases"] = 0
	}

	// Get warehouses information
	rows, err := sqlDB.QueryContext(ctx, `
		SELECT WAREHOUSE_NAME, WAREHOUSE_SIZE, STATE
		FROM INFORMATION_SCHEMA.WAREHOUSES
		WHERE OWNER_ROLE = CURRENT_ROLE()
	`)
	if err == nil {
		defer rows.Close()

		warehouses := []map[string]string{}
		for rows.Next() {
			var name, size, state string
			if err := rows.Scan(&name, &size, &state); err == nil {
				warehouse := map[string]string{
					"name":  name,
					"size":  size,
					"state": state,
				}
				warehouses = append(warehouses, warehouse)
			}
		}
		metadata["warehouses"] = warehouses
	}

	// Get current sessions
	var totalSessions int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
		WHERE EXECUTION_STATUS = 'RUNNING'
	`).Scan(&totalSessions)
	if err == nil {
		metadata["active_sessions"] = totalSessions
	} else {
		metadata["active_sessions"] = 0
	}

	// Set default values for connections if not available
	if _, ok := metadata["total_connections"]; !ok {
		metadata["total_connections"] = metadata["active_sessions"] // Use active_sessions as total_connections
	}

	if _, ok := metadata["max_connections"]; !ok {
		metadata["max_connections"] = 100 // Default placeholder
	}

	return metadata, nil
}

// ExecuteCommand executes a command on a Snowflake database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid snowflake connection type")
	}

	// Execute the command
	rows, err := sqlDB.QueryContext(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}
	defer rows.Close()

	// Get column information
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
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

// CreateDatabase creates a new Snowflake database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid snowflake connection type")
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

		if dataRetentionTimeInDays, ok := options["data_retention_time_in_days"].(int); ok && dataRetentionTimeInDays > 0 {
			optionParts = append(optionParts, fmt.Sprintf("DATA_RETENTION_TIME_IN_DAYS = %d", dataRetentionTimeInDays))
		}

		if comment, ok := options["comment"].(string); ok && comment != "" {
			optionParts = append(optionParts, fmt.Sprintf("COMMENT = '%s'", comment))
		}

		if len(optionParts) > 0 {
			commandBuilder.WriteString(" WITH ")
			commandBuilder.WriteString(strings.Join(optionParts, " "))
		}
	}

	// Create the database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase drops a Snowflake database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid snowflake connection type")
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
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
