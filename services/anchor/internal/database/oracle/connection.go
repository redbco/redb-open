package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/godror/godror" // Oracle driver

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
)

// Connect establishes a connection to an Oracle database
func Connect(config dbclient.DatabaseConfig) (*dbclient.DatabaseClient, error) {
	var connString strings.Builder

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Build connection string in Oracle format
	// Format: user/password@host:port/service_name
	fmt.Fprintf(&connString, "%s/%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration if needed
	if config.SSL {
		// Oracle uses a different approach for SSL, typically through wallet configuration
		// This would need to be configured in the Oracle client
		walletLocation := getWalletLocation(config)
		if walletLocation != "" {
			// Set environment variables or connection parameters for SSL
			// This is implementation-specific and may need adjustment
		}
	}

	// Create connection
	db, err := sql.Open("godror", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &dbclient.DatabaseClient{
		DB:           db,
		DatabaseType: "oracle",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to an Oracle instance
func ConnectInstance(config dbclient.InstanceConfig) (*dbclient.InstanceClient, error) {
	var connString strings.Builder

	decryptedPassword, err := encryption.DecryptPassword(config.TenantID, config.Password)
	if err != nil {
		return nil, fmt.Errorf("error decrypting password: %v", err)
	}

	// Build connection string in Oracle format
	// Format: user/password@host:port/service_name
	fmt.Fprintf(&connString, "%s/%s@%s:%d/%s",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration if needed
	if config.SSL {
		// Oracle uses a different approach for SSL, typically through wallet configuration
		// This would need to be configured in the Oracle client
		walletLocation := getInstanceWalletLocation(config)
		if walletLocation != "" {
			// Set environment variables or connection parameters for SSL
			// This is implementation-specific and may need adjustment
		}
	}

	// Create connection
	db, err := sql.Open("godror", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &dbclient.InstanceClient{
		DB:           db,
		InstanceType: "oracle",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of an Oracle database
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "oracle"

	// Get server version
	var version string
	err := sqlDB.QueryRow("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details["version"] = version

	// Get database size (total allocated space)
	var size int64
	err = sqlDB.QueryRow(`
		SELECT SUM(bytes) 
		FROM dba_data_files
	`).Scan(&size)
	if err != nil {
		// Fallback to user_data_files if dba_data_files is not accessible
		err = sqlDB.QueryRow(`
			SELECT SUM(bytes) 
			FROM user_data_files
		`).Scan(&size)
		if err != nil {
			return nil, fmt.Errorf("error fetching database size: %v", err)
		}
	}
	details["databaseSize"] = size

	// Get unique identifier (DBID)
	var dbid string
	err = sqlDB.QueryRow("SELECT dbid FROM v$database").Scan(&dbid)
	if err != nil {
		return nil, fmt.Errorf("error fetching database ID: %v", err)
	}
	details["uniqueIdentifier"] = dbid

	// Get database edition
	var edition string
	err = sqlDB.QueryRow("SELECT edition_name FROM v$instance").Scan(&edition)
	if err != nil {
		// If we can't get the edition, try to infer from version
		if strings.Contains(strings.ToLower(version), "enterprise") {
			edition = "enterprise"
		} else {
			edition = "standard"
		}
	}
	details["databaseEdition"] = strings.ToLower(edition)

	return details, nil
}

func getWalletLocation(config dbclient.DatabaseConfig) string {
	// This is a placeholder for Oracle wallet location logic
	// In a real implementation, this would return the path to the Oracle wallet
	// based on the SSL configuration
	if config.SSLCert != "" {
		// Return directory containing the certificates
		parts := strings.Split(config.SSLCert, "/")
		if len(parts) > 1 {
			return strings.Join(parts[:len(parts)-1], "/")
		}
	}
	return ""
}

func getInstanceWalletLocation(config dbclient.InstanceConfig) string {
	// This is a placeholder for Oracle wallet location logic
	// In a real implementation, this would return the path to the Oracle wallet
	// based on the SSL configuration
	if config.SSLCert != "" {
		// Return directory containing the certificates
		parts := strings.Split(config.SSLCert, "/")
		if len(parts) > 1 {
			return strings.Join(parts[:len(parts)-1], "/")
		}
	}
	return ""
}

// CollectDatabaseMetadata collects metadata from an Oracle database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid oracle connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size
	var sizeBytes int64
	err = sqlDB.QueryRowContext(ctx, `
		SELECT SUM(bytes) 
		FROM dba_data_files
	`).Scan(&sizeBytes)
	if err != nil {
		// Fallback to user_data_files if dba_data_files is not accessible
		err = sqlDB.QueryRowContext(ctx, `
			SELECT SUM(bytes) 
			FROM user_data_files
		`).Scan(&sizeBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to get database size: %w", err)
		}
	}
	metadata["size_bytes"] = sizeBytes

	// Get tables count
	var tablesCount int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM user_tables
	`).Scan(&tablesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from an Oracle instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid oracle connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get uptime
	var startupTime time.Time
	err = sqlDB.QueryRowContext(ctx, "SELECT startup_time FROM v$instance").Scan(&startupTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get startup time: %w", err)
	}
	uptimeSeconds := int64(time.Since(startupTime).Seconds())
	metadata["uptime_seconds"] = uptimeSeconds

	// Get total databases (in Oracle, typically just 1 per instance)
	metadata["total_databases"] = 1

	// Get current connections
	var totalConnections int
	err = sqlDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM v$session WHERE type = 'USER'").Scan(&totalConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get total connections: %w", err)
	}
	metadata["total_connections"] = totalConnections

	// Get max connections
	var maxConnections int
	err = sqlDB.QueryRowContext(ctx, "SELECT value FROM v$parameter WHERE name = 'processes'").Scan(&maxConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get max connections: %w", err)
	}
	metadata["max_connections"] = maxConnections

	return metadata, nil
}

// ExecuteCommand executes a command on an Oracle database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid oracle connection type")
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

// CreateDatabase creates a new Oracle database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid oracle connection type")
	}

	// In Oracle, we create a new schema/user which acts as a database
	// Build the CREATE USER command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE USER %s IDENTIFIED BY", databaseName))

	// Get password from options
	password, ok := options["password"].(string)
	if !ok || password == "" {
		password = "defaultpassword123" // Default password
	}
	commandBuilder.WriteString(fmt.Sprintf(" %s", password))

	// Parse and apply options
	if len(options) > 0 {
		if defaultTablespace, ok := options["default_tablespace"].(string); ok && defaultTablespace != "" {
			commandBuilder.WriteString(fmt.Sprintf(" DEFAULT TABLESPACE %s", defaultTablespace))
		}

		if tempTablespace, ok := options["temporary_tablespace"].(string); ok && tempTablespace != "" {
			commandBuilder.WriteString(fmt.Sprintf(" TEMPORARY TABLESPACE %s", tempTablespace))
		}

		if quota, ok := options["quota"].(string); ok && quota != "" {
			if defaultTablespace, ok := options["default_tablespace"].(string); ok && defaultTablespace != "" {
				commandBuilder.WriteString(fmt.Sprintf(" QUOTA %s ON %s", quota, defaultTablespace))
			}
		}
	}

	// Create the user/database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Grant necessary privileges
	grantCmd := fmt.Sprintf("GRANT CONNECT, RESOURCE TO %s", databaseName)
	_, err = sqlDB.ExecContext(ctx, grantCmd)
	if err != nil {
		return fmt.Errorf("failed to grant privileges: %w", err)
	}

	return nil
}

// DropDatabase drops an Oracle database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid oracle connection type")
	}

	// Build the DROP USER command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("DROP USER %s", databaseName))

	// Check for CASCADE option (drops all objects owned by the user)
	if cascade, ok := options["cascade"].(bool); ok && cascade {
		commandBuilder.WriteString(" CASCADE")
	}

	// Drop the user/database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
