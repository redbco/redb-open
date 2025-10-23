package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Connect establishes a connection to a Microsoft SQL Server database
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
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
	fmt.Fprintf(&connString, "server=%s;port=%d;database=%s;user id=%s;password=%s",
		config.Host,
		config.Port,
		config.DatabaseName,
		config.Username,
		decryptedPassword)

	// Add SSL configuration
	if config.SSL {
		if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
			connString.WriteString(";encrypt=true;trustservercertificate=true")
		} else {
			connString.WriteString(";encrypt=true;trustservercertificate=false")
		}
	} else {
		connString.WriteString(";encrypt=false")
	}

	// Create connection
	db, err := sql.Open("sqlserver", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &dbclient.DatabaseClient{
		DB:           db,
		DatabaseType: "mssql",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Microsoft SQL Server instance
func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
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
	fmt.Fprintf(&connString, "server=%s;port=%d;database=%s;user id=%s;password=%s",
		config.Host,
		config.Port,
		config.DatabaseName,
		config.Username,
		decryptedPassword)

	// Add SSL configuration
	if config.SSL {
		if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
			connString.WriteString(";encrypt=true;trustservercertificate=true")
		} else {
			connString.WriteString(";encrypt=true;trustservercertificate=false")
		}
	} else {
		connString.WriteString(";encrypt=false")
	}

	// Create connection
	db, err := sql.Open("sqlserver", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &dbclient.InstanceClient{
		DB:           db,
		InstanceType: "mssql",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of a Microsoft SQL Server database
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "mssql"

	// Get server version and edition
	var version, edition string
	err := sqlDB.QueryRow(`
		SELECT 
			SERVERPROPERTY('ProductVersion') AS Version,
			SERVERPROPERTY('Edition') AS Edition
	`).Scan(&version, &edition)
	if err != nil {
		return nil, fmt.Errorf("error fetching version and edition: %v", err)
	}
	details["version"] = version
	details["databaseEdition"] = edition

	// Get database size
	var size int64
	err = sqlDB.QueryRow(`
		SELECT SUM(size) * 8 * 1024 
		FROM sys.database_files
		WHERE type IN (0, 1) -- Data and Log files
	`).Scan(&size)
	if err != nil {
		return nil, fmt.Errorf("error fetching database size: %v", err)
	}
	details["databaseSize"] = size

	// Get unique identifier (database_id)
	var dbID string
	err = sqlDB.QueryRow(`
		SELECT DB_ID() AS DatabaseID
	`).Scan(&dbID)
	if err != nil {
		return nil, fmt.Errorf("error fetching database ID: %v", err)
	}
	details["uniqueIdentifier"] = dbID

	return details, nil
}

// CollectDatabaseMetadata collects metadata from a Microsoft SQL Server database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid mssql connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size
	var sizeBytes int64
	err = sqlDB.QueryRowContext(ctx, `
		SELECT SUM(size) * 8 * 1024 
		FROM sys.database_files
		WHERE type IN (0, 1) -- Data and Log files
	`).Scan(&sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get database size: %w", err)
	}
	metadata["size_bytes"] = sizeBytes

	// Get tables count
	var tablesCount int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM sys.tables 
		WHERE type = 'U'
	`).Scan(&tablesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Microsoft SQL Server instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid mssql connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get SQL Server start time and calculate uptime
	var startTime string
	err = sqlDB.QueryRowContext(ctx, `
		SELECT sqlserver_start_time 
		FROM sys.dm_os_sys_info
	`).Scan(&startTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get SQL Server start time: %w", err)
	}

	// Parse start time and calculate uptime
	layout := "2006-01-02 15:04:05.000"
	t, err := time.Parse(layout, startTime)
	if err != nil {
		metadata["uptime_seconds"] = 0
	} else {
		metadata["uptime_seconds"] = int64(time.Since(t).Seconds())
	}

	// Get total databases
	var totalDatabases int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM sys.databases 
		WHERE database_id > 4 -- Exclude system databases
	`).Scan(&totalDatabases)
	if err != nil {
		return nil, fmt.Errorf("failed to get total databases: %w", err)
	}
	metadata["total_databases"] = totalDatabases

	// Get current connections
	var totalConnections int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM sys.dm_exec_connections
	`).Scan(&totalConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get total connections: %w", err)
	}
	metadata["total_connections"] = totalConnections

	// Get max connections
	var maxConnections int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT value_in_use 
		FROM sys.configurations 
		WHERE name = 'user connections'
	`).Scan(&maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get max connections: %w", err)
	}
	metadata["max_connections"] = maxConnections

	return metadata, nil
}

// ExecuteCommand executes a command on a Microsoft SQL Server database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid mssql connection type")
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
		return nil, fmt.Errorf("invalid mssql connection type")
	}

	ctx := context.Background()
	rows, err := sqlDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute mssql query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get mssql column names: %w", err)
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
			return nil, fmt.Errorf("failed to scan mssql result: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mssql rows iteration error: %w", err)
	}

	return results, nil
}

// ExecuteCountQuery executes a count query and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return 0, fmt.Errorf("invalid mssql connection type")
	}

	ctx := context.Background()
	var count int64
	row := sqlDB.QueryRowContext(ctx, query)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to scan mssql count result: %w", err)
	}

	return count, nil
}

// StreamTableData streams data from a table in batches for efficient data copying
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid mssql connection type")
	}

	// Build column list for SELECT
	columnList := "*"
	if len(columns) > 0 {
		columnList = strings.Join(columns, ", ")
	}

	// Build query with OFFSET and FETCH NEXT (SQL Server 2012+ syntax)
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY 1 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", columnList, quoteMSSQLIdentifier(tableName))

	ctx := context.Background()
	rows, err := sqlDB.QueryContext(ctx, query, offset, batchSize)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to execute mssql streaming query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to get mssql column names: %w", err)
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
			return nil, false, "", fmt.Errorf("failed to scan mssql result: %w", err)
		}

		row := make(map[string]interface{})
		for i, colName := range columnNames {
			row[colName] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, false, "", fmt.Errorf("mssql rows iteration error: %w", err)
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
		return 0, false, fmt.Errorf("invalid mssql connection type")
	}

	// Build count query
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteMSSQLIdentifier(tableName))
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	ctx := context.Background()
	var count int64
	row := sqlDB.QueryRowContext(ctx, query)
	if err := row.Scan(&count); err != nil {
		return 0, false, fmt.Errorf("failed to scan mssql count result: %w", err)
	}

	// SQL Server COUNT(*) is always exact, not an estimate
	return count, false, nil
}

// quoteMSSQLIdentifier quotes SQL Server identifiers with square brackets
func quoteMSSQLIdentifier(identifier string) string {
	return fmt.Sprintf("[%s]", strings.ReplaceAll(identifier, "]", "]]"))
}

// CreateDatabase creates a new Microsoft SQL Server database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid mssql connection type")
	}

	// Build the CREATE DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE DATABASE [%s]", databaseName))

	// Parse and apply options
	if len(options) > 0 {
		var optionParts []string

		if initialSize, ok := options["initial_size"].(string); ok && initialSize != "" {
			optionParts = append(optionParts, fmt.Sprintf("(SIZE = %s", initialSize))
		}

		if maxSize, ok := options["max_size"].(string); ok && maxSize != "" {
			optionParts = append(optionParts, fmt.Sprintf("MAXSIZE = %s", maxSize))
		}

		if fileGrowth, ok := options["file_growth"].(string); ok && fileGrowth != "" {
			optionParts = append(optionParts, fmt.Sprintf("FILEGROWTH = %s", fileGrowth))
		}

		if len(optionParts) > 0 {
			commandBuilder.WriteString(" ")
			commandBuilder.WriteString(strings.Join(optionParts, ", "))
			if strings.Contains(commandBuilder.String(), "(SIZE =") {
				commandBuilder.WriteString(")")
			}
		}

		if collation, ok := options["collation"].(string); ok && collation != "" {
			commandBuilder.WriteString(fmt.Sprintf(" COLLATE %s", collation))
		}
	}

	// Create the database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase drops a Microsoft SQL Server database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid mssql connection type")
	}

	// Build the DROP DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP DATABASE")

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		commandBuilder.WriteString(" IF EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" [%s]", databaseName))

	// Drop the database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
