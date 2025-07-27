package neo4j

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to a Neo4j database
func Connect(cfg common.DatabaseConfig) (*common.DatabaseClient, error) {
	var connString strings.Builder

	decryptedPassword, err := encryption.DecryptPassword(cfg.TenantID, cfg.Password)
	if err != nil {
		if cfg.Password == "" {
			decryptedPassword = ""
		} else {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
	}

	// Build connection URI
	scheme := "neo4j"
	if cfg.SSL {
		scheme = "neo4j+s" // Use secure connection
	} else {
		scheme = "neo4j" // Use insecure connection
	}

	fmt.Fprintf(&connString, "%s://%s:%d", scheme, cfg.Host, cfg.Port)

	// Create auth config
	auth := neo4j.BasicAuth(cfg.Username, decryptedPassword, "")

	// Create driver
	driver, err := neo4j.NewDriverWithContext(connString.String(), auth)
	if err != nil {
		return nil, fmt.Errorf("error creating Neo4j driver: %v", err)
	}

	// Test the connection
	ctx := context.Background()
	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("error connecting to Neo4j database: %v", err)
	}

	return &common.DatabaseClient{
		DB:           driver,
		DatabaseType: "neo4j",
		DatabaseID:   cfg.DatabaseID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Neo4j instance
func ConnectInstance(cfg common.InstanceConfig) (*common.InstanceClient, error) {
	var connString strings.Builder

	decryptedPassword, err := encryption.DecryptPassword(cfg.TenantID, cfg.Password)
	if err != nil {
		if cfg.Password == "" {
			decryptedPassword = ""
		} else {
			return nil, fmt.Errorf("error decrypting password: %v", err)
		}
	}

	// Build connection URI
	scheme := "neo4j"
	if cfg.SSL {
		scheme = "neo4j+s" // Use secure connection
	} else {
		scheme = "neo4j" // Use insecure connection
	}

	fmt.Fprintf(&connString, "%s://%s:%d", scheme, cfg.Host, cfg.Port)

	// Create auth config
	auth := neo4j.BasicAuth(cfg.Username, decryptedPassword, "")

	// Create driver
	driver, err := neo4j.NewDriverWithContext(connString.String(), auth)
	if err != nil {
		return nil, fmt.Errorf("error creating Neo4j driver: %v", err)
	}

	// Test the connection
	ctx := context.Background()
	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("error connecting to Neo4j database: %v", err)
	}

	return &common.InstanceClient{
		DB:           driver,
		InstanceType: "neo4j",
		InstanceID:   cfg.InstanceID,
		Config:       cfg,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of a Neo4j database
func DiscoverDetails(db interface{}) (*Neo4jDetails, error) {
	driver, ok := db.(neo4j.DriverWithContext)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	var details Neo4jDetails
	details.DatabaseType = "neo4j"

	// Get server version
	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil)
	if err != nil {
		return nil, fmt.Errorf("error fetching version: %v", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		name, _ := record.Get("name")
		versions, _ := record.Get("versions")
		edition, _ := record.Get("edition")

		versionsArr, ok := versions.([]interface{})
		if ok && len(versionsArr) > 0 {
			details.Version = fmt.Sprintf("%s %v", name, versionsArr[0])
		} else {
			details.Version = fmt.Sprintf("%s", name)
		}

		details.DatabaseEdition = fmt.Sprintf("%v", edition)
	}

	// Get database size (approximate)
	result, err = session.Run(ctx, "CALL dbms.database.size() YIELD totalStoreSize RETURN totalStoreSize", nil)
	if err != nil {
		// This procedure might not be available in all editions, so we'll handle the error gracefully
		details.DatabaseSize = -1
	} else if result.Next(ctx) {
		record := result.Record()
		size, _ := record.Get("totalStoreSize")
		if sizeVal, ok := size.(int64); ok {
			details.DatabaseSize = sizeVal
		}
	}

	// Get unique identifier (database ID)
	result, err = session.Run(ctx, "CALL dbms.info() YIELD id RETURN id", nil)
	if err != nil {
		details.UniqueIdentifier = "unknown"
	} else if result.Next(ctx) {
		record := result.Record()
		id, _ := record.Get("id")
		details.UniqueIdentifier = fmt.Sprintf("%v", id)
	}

	return &details, nil
}

// CollectDatabaseMetadata collects metadata from a Neo4j database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	driver, ok := db.(neo4j.DriverWithContext)
	if !ok {
		return nil, fmt.Errorf("invalid neo4j connection type")
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	metadata := make(map[string]interface{})

	// Get database version
	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions RETURN name, versions", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		name, _ := record.Get("name")
		versions, _ := record.Get("versions")
		versionsArr, ok := versions.([]interface{})
		if ok && len(versionsArr) > 0 {
			metadata["version"] = fmt.Sprintf("%s %v", name, versionsArr[0])
		} else {
			metadata["version"] = fmt.Sprintf("%s", name)
		}
	}

	// Get node count
	result, err = session.Run(ctx, "MATCH (n) RETURN count(n) as nodeCount", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get node count: %w", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		nodeCount, _ := record.Get("nodeCount")
		metadata["node_count"] = nodeCount
	}

	// Get relationship count
	result, err = session.Run(ctx, "MATCH ()-[r]->() RETURN count(r) as relCount", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get relationship count: %w", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		relCount, _ := record.Get("relCount")
		metadata["relationship_count"] = relCount
	}

	// Get database size if available
	result, err = session.Run(ctx, "CALL dbms.database.size() YIELD totalStoreSize RETURN totalStoreSize", nil)
	if err == nil && result.Next(ctx) {
		record := result.Record()
		size, _ := record.Get("totalStoreSize")
		metadata["size_bytes"] = size
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Neo4j instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	driver, ok := db.(neo4j.DriverWithContext)
	if !ok {
		return nil, fmt.Errorf("invalid neo4j connection type")
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	metadata := make(map[string]interface{})

	// Get database version
	result, err := session.Run(ctx, "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		name, _ := record.Get("name")
		versions, _ := record.Get("versions")
		edition, _ := record.Get("edition")

		versionsArr, ok := versions.([]interface{})
		if ok && len(versionsArr) > 0 {
			metadata["version"] = fmt.Sprintf("%s %v", name, versionsArr[0])
		} else {
			metadata["version"] = fmt.Sprintf("%s", name)
		}
		metadata["edition"] = edition
	}

	// Get available databases
	result, err = session.Run(ctx, "SHOW DATABASES YIELD name RETURN count(name) as dbCount", nil)
	if err == nil && result.Next(ctx) {
		record := result.Record()
		dbCount, _ := record.Get("dbCount")
		metadata["total_databases"] = dbCount
	}

	// Get active connections
	result, err = session.Run(ctx, "CALL dbms.connectionStatus() YIELD connectionCount RETURN connectionCount", nil)
	if err == nil && result.Next(ctx) {
		record := result.Record()
		connCount, _ := record.Get("connectionCount")
		metadata["total_connections"] = connCount
	}

	// Get uptime if available
	result, err = session.Run(ctx, "CALL dbms.queryJmx('org.neo4j:instance=kernel#0,name=Kernel') YIELD attributes RETURN attributes", nil)
	if err == nil && result.Next(ctx) {
		record := result.Record()
		attributes, ok := record.Get("attributes")
		if ok {
			attrsMap, ok := attributes.(map[string]interface{})
			if ok {
				uptime, ok := attrsMap["KernelStartTime"]
				if ok {
					metadata["uptime_seconds"] = uptime
				}
			}
		}
	}

	// Set default max connections if not available
	if _, ok := metadata["max_connections"]; !ok {
		metadata["max_connections"] = 100 // Default placeholder
	}

	return metadata, nil
}

// ExecuteCommand executes a command on a Neo4j database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	driver, ok := db.(neo4j.DriverWithContext)
	if !ok {
		return nil, fmt.Errorf("invalid neo4j connection type")
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// Execute the command
	result, err := session.Run(ctx, command, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}

	// Collect all records
	var records []map[string]interface{}
	var columnNames []string

	// Get the first record to determine column names
	if result.Next(ctx) {
		record := result.Record()
		columnNames = record.Keys

		values := make(map[string]interface{})
		for _, key := range columnNames {
			value, _ := record.Get(key)
			values[key] = value
		}
		records = append(records, values)

		// Get remaining records
		for result.Next(ctx) {
			record := result.Record()
			values := make(map[string]interface{})
			for _, key := range columnNames {
				value, _ := record.Get(key)
				values[key] = value
			}
			records = append(records, values)
		}
	}

	// Check for any errors during iteration
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error during iteration: %w", err)
	}

	// Structure the response for gRPC
	response := map[string]interface{}{
		"columns": columnNames,
		"rows":    records,
		"count":   len(records),
	}

	// Convert to JSON bytes for gRPC transmission
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal results to JSON: %w", err)
	}

	return jsonBytes, nil
}

// CreateDatabase creates a new Neo4j database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	driver, ok := db.(neo4j.DriverWithContext)
	if !ok {
		return fmt.Errorf("invalid neo4j connection type")
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system", // Use system database for admin operations
	})
	defer session.Close(ctx)

	// Build the CREATE DATABASE command
	var commandBuilder strings.Builder
	commandBuilder.WriteString(fmt.Sprintf("CREATE DATABASE %s", databaseName))

	// Parse and apply options
	if len(options) > 0 {
		if ifNotExists, ok := options["if_not_exists"].(bool); ok && ifNotExists {
			// Neo4j uses IF NOT EXISTS syntax
			commandBuilder = strings.Builder{}
			commandBuilder.WriteString(fmt.Sprintf("CREATE DATABASE %s IF NOT EXISTS", databaseName))
		}
	}

	// Execute the command
	_, err := session.Run(ctx, commandBuilder.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

// DropDatabase drops a Neo4j database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	driver, ok := db.(neo4j.DriverWithContext)
	if !ok {
		return fmt.Errorf("invalid neo4j connection type")
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system", // Use system database for admin operations
	})
	defer session.Close(ctx)

	// Build the DROP DATABASE command
	var commandBuilder strings.Builder
	commandBuilder.WriteString("DROP DATABASE")

	// Check for IF EXISTS option
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		commandBuilder.WriteString(" IF EXISTS")
	}

	commandBuilder.WriteString(fmt.Sprintf(" %s", databaseName))

	// Execute the command
	_, err := session.Run(ctx, commandBuilder.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
