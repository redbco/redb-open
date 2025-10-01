package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Connect establishes a connection to a MySQL database
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

	var sslMode string
	if config.SSL {
		if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
			sslMode = "skip-verify"
		} else {
			sslMode = "true"
		}
	} else {
		sslMode = "false"
	}

	// Build the connection string
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s",
		config.Username, decryptedPassword, config.Host, config.Port, config.DatabaseName, sslMode)

	// Add SSL configuration if enabled
	if config.SSL && config.SSLCert != "" && config.SSLKey != "" {
		dsn = fmt.Sprintf("%s&sslcert=%s&sslkey=%s", dsn, config.SSLCert, config.SSLKey)
		if config.SSLRootCert != "" {
			dsn = fmt.Sprintf("%s&sslrootcert=%s", dsn, config.SSLRootCert)
		}
	}

	// Open the database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	return &dbclient.DatabaseClient{
		DB:           db,
		DatabaseType: "mysql",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a MySQL instance
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

	var sslMode string
	if config.SSL {
		if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
			sslMode = "skip-verify"
		} else {
			sslMode = "true"
		}
	} else {
		sslMode = "false"
	}

	// Build the connection string
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s",
		config.Username, decryptedPassword, config.Host, config.Port, config.DatabaseName, sslMode)

	// Add SSL configuration if enabled
	if config.SSL && config.SSLCert != "" && config.SSLKey != "" {
		dsn = fmt.Sprintf("%s&sslcert=%s&sslkey=%s", dsn, config.SSLCert, config.SSLKey)
		if config.SSLRootCert != "" {
			dsn = fmt.Sprintf("%s&sslrootcert=%s", dsn, config.SSLRootCert)
		}
	}

	// Open the database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// Test the connection
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	return &dbclient.InstanceClient{
		DB:           db,
		InstanceType: "mysql",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails retrieves basic details about a MySQL database for metadata purposes
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid MySQL connection type")
	}

	details := make(map[string]interface{})
	details["uniqueIdentifier"] = dbclient.GenerateUniqueID()
	details["databaseType"] = "mysql"

	// Get database version
	var version string
	err := sqlDB.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	details["version"] = version

	// Determine database edition based on version
	if strings.Contains(strings.ToLower(version), "mariadb") {
		details["databaseEdition"] = "MariaDB"
	} else {
		details["databaseEdition"] = "MySQL"
	}

	// Get database size (approximate for MySQL)
	var sizeBytes int64
	query := `
		SELECT SUM(data_length + index_length) 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()`
	err = sqlDB.QueryRow(query).Scan(&sizeBytes)
	if err != nil {
		// If we can't get the size, set it to 0 but continue
		sizeBytes = 0
	}
	details["databaseSize"] = sizeBytes

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a MySQL database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid MySQL connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size (approximate for MySQL)
	var sizeBytes int64
	query := `
		SELECT SUM(data_length + index_length) 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()`
	err = sqlDB.QueryRowContext(ctx, query).Scan(&sizeBytes)
	if err != nil {
		// If we can't get the size, set it to 0 but continue
		sizeBytes = 0
	}
	metadata["size_bytes"] = sizeBytes

	// Get tables count
	var tablesCount int
	err = sqlDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()").Scan(&tablesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a MySQL instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid MySQL connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get uptime
	var uptimeSeconds int64
	err = sqlDB.QueryRowContext(ctx, "SELECT variable_value FROM performance_schema.global_status WHERE variable_name = 'Uptime'").Scan(&uptimeSeconds)
	if err != nil {
		// If we can't get uptime, set it to 0 but continue
		uptimeSeconds = 0
	}
	metadata["uptime_seconds"] = uptimeSeconds

	// Get total databases
	var totalDatabases int
	err = sqlDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')").Scan(&totalDatabases)
	if err != nil {
		return nil, fmt.Errorf("failed to get total databases: %w", err)
	}
	metadata["total_databases"] = totalDatabases

	// Get current connections
	var totalConnections int
	err = sqlDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.processlist").Scan(&totalConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get total connections: %w", err)
	}
	metadata["total_connections"] = totalConnections

	// Get max connections
	var maxConnections int
	err = sqlDB.QueryRowContext(ctx, "SELECT @@max_connections").Scan(&maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get max connections: %w", err)
	}
	metadata["max_connections"] = maxConnections

	return metadata, nil
}

// ExecuteCommand executes a command on a MySQL database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid mysql connection type")
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

// ExecuteQuery executes a generic SQL query and returns results as a slice of maps
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid mysql connection type")
	}

	ctx := context.Background()
	rows, err := sqlDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute mysql query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get mysql column names: %w", err)
	}

	// Collect results
	var results []interface{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan mysql result: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql rows iteration error: %w", err)
	}

	return results, nil
}

// ExecuteCountQuery executes a count query and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return 0, fmt.Errorf("invalid mysql connection type")
	}

	ctx := context.Background()
	var count int64
	row := sqlDB.QueryRowContext(ctx, query)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to scan mysql count result: %w", err)
	}

	return count, nil
}

// StreamTableData streams data from a table in batches for efficient data copying
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid mysql connection type")
	}

	// Build column list for SELECT
	columnList := "*"
	if len(columns) > 0 {
		columnList = strings.Join(columns, ", ")
	}

	// Build query with LIMIT and OFFSET
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY 1 LIMIT ? OFFSET ?", columnList, QuoteIdentifier(tableName))

	ctx := context.Background()
	rows, err := sqlDB.QueryContext(ctx, query, batchSize, offset)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to execute mysql streaming query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to get mysql column names: %w", err)
	}

	// Collect results
	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, false, "", fmt.Errorf("failed to scan mysql result: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, false, "", fmt.Errorf("mysql rows iteration error: %w", err)
	}

	rowCount := len(results)
	isComplete := rowCount < int(batchSize)

	// For simple offset-based pagination, we don't use cursor values
	nextCursorValue := ""

	return results, isComplete, nextCursorValue, nil
}

// GetTableRowCount returns the number of rows in a table, optionally with a WHERE clause
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return 0, false, fmt.Errorf("invalid mysql connection type")
	}

	// Build count query
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", QuoteIdentifier(tableName))
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	ctx := context.Background()
	var count int64
	row := sqlDB.QueryRowContext(ctx, query)
	if err := row.Scan(&count); err != nil {
		return 0, false, fmt.Errorf("failed to scan mysql count result: %w", err)
	}

	// MySQL COUNT(*) is always exact, not an estimate
	return count, false, nil
}

// CreateDatabase creates a new MySQL database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid mysql connection type")
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

		if characterSet, ok := options["character_set"].(string); ok && characterSet != "" {
			optionParts = append(optionParts, fmt.Sprintf("CHARACTER SET = %s", characterSet))
		}

		if collate, ok := options["collate"].(string); ok && collate != "" {
			optionParts = append(optionParts, fmt.Sprintf("COLLATE = %s", collate))
		}

		if len(optionParts) > 0 {
			commandBuilder.WriteString(" ")
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

// DropDatabase drops a MySQL database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid mysql connection type")
	}

	// Build the DROP DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP DATABASE")

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		commandBuilder.WriteString(" IF EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", databaseName))

	// Drop the database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
