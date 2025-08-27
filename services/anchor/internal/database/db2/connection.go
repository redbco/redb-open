package db2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/ibmdb/go_ibm_db" // Db2 driver

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to an IBM Db2 database
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

	// Build connection string for Db2
	// Format: HOSTNAME=host;DATABASE=dbname;PORT=port;UID=username;PWD=password;
	fmt.Fprintf(&connString, "HOSTNAME=%s;DATABASE=%s;PORT=%d;UID=%s;PWD=%s;",
		config.Host,
		config.DatabaseName,
		config.Port,
		config.Username,
		decryptedPassword)

	// Add SSL configuration if enabled
	if config.SSL {
		sslMode := getSslMode(config)
		fmt.Fprintf(&connString, "Security=%s;", sslMode)

		if config.SSLCert != "" && config.SSLKey != "" {
			fmt.Fprintf(&connString, "SSLClientKeystoredb=%s;SSLClientKeystash=%s;",
				config.SSLCert, config.SSLKey)
		}
		if config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "SSLServerCertificate=%s;", config.SSLRootCert)
		}
	}

	// Open database connection
	db, err := sql.Open("go_ibm_db", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &common.DatabaseClient{
		DB:           db,
		DatabaseType: "db2",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to an IBM Db2 instance
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

	// Build connection string for Db2
	// Format: HOSTNAME=host;DATABASE=dbname;PORT=port;UID=username;PWD=password;
	fmt.Fprintf(&connString, "HOSTNAME=%s;DATABASE=%s;PORT=%d;UID=%s;PWD=%s;",
		config.Host,
		config.DatabaseName,
		config.Port,
		config.Username,
		decryptedPassword)

	// Add SSL configuration if enabled
	if config.SSL {
		sslMode := getInstanceSslMode(config)
		fmt.Fprintf(&connString, "Security=%s;", sslMode)

		if config.SSLCert != "" && config.SSLKey != "" {
			fmt.Fprintf(&connString, "SSLClientKeystoredb=%s;SSLClientKeystash=%s;",
				config.SSLCert, config.SSLKey)
		}
		if config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "SSLServerCertificate=%s;", config.SSLRootCert)
		}
	}

	// Open database connection
	db, err := sql.Open("go_ibm_db", connString.String())
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return &common.InstanceClient{
		DB:           db,
		InstanceType: "db2",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of an IBM Db2 database
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "db2"

	// Get server version
	var version string
	err := sqlDB.QueryRow("SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details["version"] = version

	// Get database size (in bytes)
	var size int64
	err = sqlDB.QueryRow(`
		SELECT SUM(TBSP_USED_SIZE_KB) * 1024 
		FROM TABLE(MON_GET_TABLESPACE('', -1)) AS T`).Scan(&size)
	if err != nil {
		return nil, fmt.Errorf("error fetching database size: %v", err)
	}
	details["databaseSize"] = size

	// Get unique identifier (database name + instance name)
	var dbName, instanceName string
	err = sqlDB.QueryRow("SELECT current database, current server FROM SYSIBMADM.ENV_SYS_INFO").Scan(&dbName, &instanceName)
	if err != nil {
		return nil, fmt.Errorf("error fetching database identifier: %v", err)
	}
	details["uniqueIdentifier"] = fmt.Sprintf("%s_%s", instanceName, dbName)

	// Get database edition
	var productName string
	err = sqlDB.QueryRow("SELECT product_name FROM TABLE(SYSPROC.ENV_GET_PROD_INFO()) AS PRODUCTINFO").Scan(&productName)
	if err != nil {
		return nil, fmt.Errorf("error fetching product info: %v", err)
	}

	if strings.Contains(strings.ToLower(productName), "enterprise") {
		details["databaseEdition"] = "enterprise"
	} else if strings.Contains(strings.ToLower(productName), "advanced") {
		details["databaseEdition"] = "advanced"
	} else if strings.Contains(strings.ToLower(productName), "standard") {
		details["databaseEdition"] = "standard"
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
		return "SSL"
	}
	return "SSL_VERIFY_SERVER_CERTIFICATE"
}

func getInstanceSslMode(config common.InstanceConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "SSL"
	}
	return "SSL_VERIFY_SERVER_CERTIFICATE"
}

// CollectDatabaseMetadata collects metadata from an IBM Db2 database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid db2 connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size
	var sizeBytes int64
	err = sqlDB.QueryRowContext(ctx, `
		SELECT SUM(TBSP_USED_SIZE_KB) * 1024 
		FROM TABLE(MON_GET_TABLESPACE('', -1)) AS T`).Scan(&sizeBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to get database size: %w", err)
	}
	metadata["size_bytes"] = sizeBytes

	// Get tables count
	var tablesCount int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM SYSCAT.TABLES 
		WHERE TABSCHEMA NOT LIKE 'SYS%'`).Scan(&tablesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables count: %w", err)
	}
	metadata["tables_count"] = tablesCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from an IBM Db2 instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid db2 connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := sqlDB.QueryRowContext(ctx, "SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS INSTANCEINFO").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get uptime (in seconds)
	var uptimeSeconds int64
	err = sqlDB.QueryRowContext(ctx, `
		SELECT (CURRENT TIMESTAMP - DB_CONN_TIME) SECONDS 
		FROM TABLE(MON_GET_DATABASE(-1)) AS T`).Scan(&uptimeSeconds)
	if err != nil {
		// If we can't get uptime, set it to 0 and continue
		uptimeSeconds = 0
	}
	metadata["uptime_seconds"] = uptimeSeconds

	// Get total databases in the instance
	var totalDatabases int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM TABLE(ADMIN_LIST_DB_PARTITION_GROUPS()) AS T`).Scan(&totalDatabases)
	if err != nil {
		return nil, fmt.Errorf("failed to get total databases: %w", err)
	}
	metadata["total_databases"] = totalDatabases

	// Get current connections
	var totalConnections int
	err = sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM TABLE(MON_GET_CONNECTION(NULL, -1)) AS T`).Scan(&totalConnections)
	if err != nil {
		return nil, fmt.Errorf("failed to get total connections: %w", err)
	}
	metadata["total_connections"] = totalConnections

	// Get max connections
	var maxConnectionsStr string
	err = sqlDB.QueryRowContext(ctx, "SELECT VALUE FROM SYSIBMADM.DBCFG WHERE NAME = 'max_connections'").Scan(&maxConnectionsStr)
	if err != nil {
		return nil, fmt.Errorf("failed to get max connections: %w", err)
	}
	maxConnections, _ := strconv.Atoi(maxConnectionsStr)
	metadata["max_connections"] = maxConnections

	return metadata, nil
}

// ExecuteCommand executes a command on an IBM Db2 database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid db2 connection type")
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

// CreateDatabase creates a new IBM Db2 database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid db2 connection type")
	}

	// Build the CREATE DATABASE command with options
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE DATABASE %s", databaseName))

	// Parse and apply options
	if len(options) > 0 {
		if codepage, ok := options["codepage"].(string); ok && codepage != "" {
			commandBuilder.WriteString(fmt.Sprintf(" USING CODESET %s", codepage))
		}

		if territory, ok := options["territory"].(string); ok && territory != "" {
			commandBuilder.WriteString(fmt.Sprintf(" TERRITORY %s", territory))
		}

		if collate, ok := options["collate"].(string); ok && collate != "" {
			commandBuilder.WriteString(fmt.Sprintf(" COLLATE USING %s", collate))
		}

		if pagesize, ok := options["pagesize"].(int); ok && pagesize > 0 {
			commandBuilder.WriteString(fmt.Sprintf(" PAGESIZE %d", pagesize))
		}
	}

	// Create the database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase drops an IBM Db2 database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	sqlDB, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("invalid db2 connection type")
	}

	// Build the DROP DATABASE command
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("DROP DATABASE %s", databaseName))

	// Drop the database
	_, err := sqlDB.ExecContext(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
