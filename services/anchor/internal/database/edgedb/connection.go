package edgedb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	gel "github.com/geldata/gel-go"
	gelcfg "github.com/geldata/gel-go/gelcfg"
	"github.com/geldata/gel-go/geltypes"
	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to an EdgeDB database
func Connect(config common.DatabaseConfig) (*common.DatabaseClient, error) {

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
	opts := gelcfg.Options{
		Host: config.Host,
		Port: int(config.Port),
		User: config.Username,
		// Convert string to OptionalStr
		Password: geltypes.NewOptionalStr(decryptedPassword),
		Database: config.DatabaseName,
		// Default concurrency and timeout
		Concurrency:    4,
		ConnectTimeout: 5 * time.Second,
	}

	// Configure TLS/SSL
	if config.SSL {
		// Check the actual field names in gelcfg.TLSOptions
		// Since the field names have changed, we need to adapt
		opts.TLSOptions.SecurityMode = gelcfg.TLSModeStrict

		if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
			opts.TLSOptions.SecurityMode = gelcfg.TLSModeInsecure
		}

		// Set certificate paths if provided
		// Check if these fields exist in the new API
		if config.SSLCert != "" {
			opts.TLSOptions.CA = []byte(config.SSLCert)
		}

		if config.SSLRootCert != "" {
			opts.TLSOptions.CA = []byte(config.SSLRootCert)
		}
	} else {
		opts.TLSOptions.SecurityMode = gelcfg.TLSModeInsecure
	}

	// Create connection
	client, err := gel.CreateClient(opts)
	if err != nil {
		return nil, fmt.Errorf("error connecting to EdgeDB: %v", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result string
	err = client.QuerySingle(ctx, "SELECT 'connected'", &result)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("error testing EdgeDB connection: %v", err)
	}

	if result != "connected" {
		client.Close()
		return nil, fmt.Errorf("unexpected response from EdgeDB connection test")
	}

	return &common.DatabaseClient{
		DB:           client,
		DatabaseType: "edgedb",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to an EdgeDB instance
func ConnectInstance(config common.InstanceConfig) (*common.InstanceClient, error) {

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
	opts := gelcfg.Options{
		Host: config.Host,
		Port: int(config.Port),
		User: config.Username,
		// Convert string to OptionalStr
		Password: geltypes.NewOptionalStr(decryptedPassword),
		Database: config.DatabaseName,
		// Default concurrency and timeout
		Concurrency:    4,
		ConnectTimeout: 5 * time.Second,
	}

	// Configure TLS/SSL
	if config.SSL {
		// Check the actual field names in gelcfg.TLSOptions
		// Since the field names have changed, we need to adapt
		opts.TLSOptions.SecurityMode = gelcfg.TLSModeStrict

		if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
			opts.TLSOptions.SecurityMode = gelcfg.TLSModeInsecure
		}

		// Set certificate paths if provided
		// Check if these fields exist in the new API
		if config.SSLCert != "" {
			opts.TLSOptions.CA = []byte(config.SSLCert)
		}

		if config.SSLRootCert != "" {
			opts.TLSOptions.CA = []byte(config.SSLRootCert)
		}
	} else {
		opts.TLSOptions.SecurityMode = gelcfg.TLSModeInsecure
	}

	// Create connection
	client, err := gel.CreateClient(opts)
	if err != nil {
		return nil, fmt.Errorf("error connecting to EdgeDB: %v", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result string
	err = client.QuerySingle(ctx, "SELECT 'connected'", &result)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("error testing EdgeDB connection: %v", err)
	}

	if result != "connected" {
		client.Close()
		return nil, fmt.Errorf("unexpected response from EdgeDB connection test")
	}

	return &common.InstanceClient{
		DB:           client,
		InstanceType: "edgedb",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of an EdgeDB database
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*gel.Client)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "edgedb"

	ctx := context.Background()

	// Get server version
	var version string
	err := client.QuerySingle(ctx, "SELECT sys::get_version_as_str()", &version)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}
	details["version"] = version

	// Get database size (approximate)
	var sizeStr string
	err = client.QuerySingle(ctx, `
		SELECT sum(
			sys::get_object_size(obj)
		) 
		FROM (
			SELECT schema::Object {
				id
			} AS obj
		)`, &sizeStr)
	if err != nil {
		// If we can't get the exact size, use a placeholder
		details["databaseSize"] = int64(0)
	} else {
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			details["databaseSize"] = int64(0)
		} else {
			details["databaseSize"] = size
		}
	}

	// Get unique identifier
	var dbid string
	err = client.QuerySingle(ctx, "SELECT sys::get_current_database().id", &dbid)
	if err != nil {
		return nil, fmt.Errorf("error fetching database ID: %v", err)
	}
	details["uniqueIdentifier"] = dbid

	// Determine edition
	if strings.Contains(strings.ToLower(version), "enterprise") {
		details["databaseEdition"] = "enterprise"
	} else {
		details["databaseEdition"] = "community"
	}

	return details, nil
}

// CollectDatabaseMetadata collects metadata from an EdgeDB database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*gel.Client)
	if !ok {
		return nil, fmt.Errorf("invalid edgedb connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := client.QuerySingle(ctx, "SELECT sys::get_version_as_str()", &version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// Get database size (approximate)
	var sizeBytes int64
	err = client.QuerySingle(ctx, `
		SELECT sum(
			sys::get_object_size(obj)
		) 
		FROM (
			SELECT schema::Object {
				id
			} AS obj
		)`, &sizeBytes)
	if err != nil {
		metadata["size_bytes"] = 0
	} else {
		metadata["size_bytes"] = sizeBytes
	}

	// Get types count
	var typesCount int
	err = client.QuerySingle(ctx, `
		SELECT count(
			schema::ObjectType
		)`, &typesCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get types count: %w", err)
	}
	metadata["types_count"] = typesCount

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from an EdgeDB instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*gel.Client)
	if !ok {
		return nil, fmt.Errorf("invalid edgedb connection type")
	}

	metadata := make(map[string]interface{})

	// Get database version
	var version string
	err := client.QuerySingle(ctx, "SELECT sys::get_version_as_str()", &version)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}
	metadata["version"] = version

	// EdgeDB doesn't provide direct uptime information, so we'll use a placeholder
	var uptimeSeconds int64 = 0
	metadata["uptime_seconds"] = uptimeSeconds

	// Get total databases
	var totalDatabases int
	err = client.QuerySingle(ctx, `
		SELECT count(
			sys::Database
		)`, &totalDatabases)
	if err != nil {
		return nil, fmt.Errorf("failed to get total databases: %w", err)
	}
	metadata["total_databases"] = totalDatabases

	// EdgeDB doesn't provide direct connection count information
	metadata["total_connections"] = 0
	metadata["max_connections"] = 100 // Default placeholder

	return metadata, nil
}

// ExecuteCommand executes a command on an EdgeDB database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	client, ok := db.(*gel.Client)
	if !ok {
		return nil, fmt.Errorf("invalid edgedb connection type")
	}

	// Execute the query and get results as JSON
	var result interface{}
	err := client.Query(ctx, command, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}

	// Structure the response for gRPC
	var results []map[string]interface{}
	var columnNames []string

	// Handle different result types
	switch r := result.(type) {
	case []interface{}:
		// Multiple results
		for _, item := range r {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if len(columnNames) == 0 {
					// Extract column names from first row
					for key := range itemMap {
						columnNames = append(columnNames, key)
					}
				}
				results = append(results, itemMap)
			}
		}
	case map[string]interface{}:
		// Single result
		for key := range r {
			columnNames = append(columnNames, key)
		}
		results = append(results, r)
	default:
		// Scalar result
		columnNames = []string{"result"}
		results = append(results, map[string]interface{}{"result": result})
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

// CreateDatabase creates a new EdgeDB database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*gel.Client)
	if !ok {
		return fmt.Errorf("invalid edgedb connection type")
	}

	// Build the CREATE DATABASE command
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE DATABASE %s", databaseName))

	// Parse and apply options (EdgeDB has limited options for CREATE DATABASE)
	if len(options) > 0 {
		if template, ok := options["template"].(string); ok && template != "" {
			commandBuilder.WriteString(fmt.Sprintf(" FROM %s", template))
		}
	}

	// Execute the command
	err := client.Execute(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase drops an EdgeDB database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*gel.Client)
	if !ok {
		return fmt.Errorf("invalid edgedb connection type")
	}

	// Build the DROP DATABASE command
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP DATABASE")

	// Check for IF EXISTS option (EdgeDB doesn't support this, but we can handle it gracefully)
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		// We'll try to list databases first to check if it exists
		var databases []string
		err := client.Query(ctx, "SELECT sys::Database.name", &databases)
		if err != nil {
			return fmt.Errorf("failed to check if database exists: %w", err)
		}

		found := false
		for _, dbName := range databases {
			if dbName == databaseName {
				found = true
				break
			}
		}

		if !found {
			// Database doesn't exist, but that's OK with if_exists
			return nil
		}
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", databaseName))

	// Execute the command
	err := client.Execute(ctx, commandBuilder.String())
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
