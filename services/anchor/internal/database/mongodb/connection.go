package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/dbclient"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Connect establishes a connection to a MongoDB database
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
	fmt.Fprintf(&connString, "mongodb://%s:%s@%s:%d/%s?authSource=admin",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration
	if config.SSL {
		sslMode := getSslMode(config)
		fmt.Fprintf(&connString, "&tls=%t", sslMode != "disable")

		if config.SSLCert != "" && config.SSLKey != "" {
			fmt.Fprintf(&connString, "&tlsCertificateKeyFile=%s",
				config.SSLCert)
		}
		if config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "&tlsCAFile=%s", config.SSLRootCert)
		}
		if sslMode == "allow" || sslMode == "prefer" {
			fmt.Fprintf(&connString, "&tlsInsecure=true")
		}
	} else {
		connString.WriteString("&tls=false")
	}

	// Set client options
	clientOptions := options.Client().ApplyURI(connString.String())

	// Create client and connect (in v2, Connect handles both creation and connection)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Set context with timeout for ping
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test the connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	// Get the database
	db := client.Database(config.DatabaseName)

	return &dbclient.DatabaseClient{
		DB:           db,
		DatabaseType: "mongodb",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a MongoDB instance
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
	fmt.Fprintf(&connString, "mongodb://%s:%s@%s:%d/%s?authSource=admin",
		config.Username,
		decryptedPassword,
		config.Host,
		config.Port,
		config.DatabaseName)

	// Add SSL configuration
	if config.SSL {
		sslMode := getInstanceSslMode(config)
		fmt.Fprintf(&connString, "&tls=%t", sslMode != "disable")

		if config.SSLCert != "" && config.SSLKey != "" {
			fmt.Fprintf(&connString, "&tlsCertificateKeyFile=%s",
				config.SSLCert)
		}
		if config.SSLRootCert != "" {
			fmt.Fprintf(&connString, "&tlsCAFile=%s", config.SSLRootCert)
		}
		if sslMode == "allow" || sslMode == "prefer" {
			fmt.Fprintf(&connString, "&tlsInsecure=true")
		}
	} else {
		connString.WriteString("&tls=false")
	}

	// Set client options
	clientOptions := options.Client().ApplyURI(connString.String())

	// Create client and connect (in v2, Connect handles both creation and connection)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	// Set context with timeout for ping
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test the connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	// Get the database
	db := client.Database(config.DatabaseName)

	return &dbclient.InstanceClient{
		DB:           db,
		InstanceType: "mongodb",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches basic details of a MongoDB database for metadata purposes
func DiscoverDetails(db interface{}) (map[string]interface{}, error) {
	database, ok := db.(*mongo.Database)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	details := make(map[string]interface{})
	details["databaseType"] = "mongodb"

	// Get server version and build info
	ctx := context.Background()
	buildInfoCmd := bson.D{{Key: "buildInfo", Value: 1}}
	buildInfoResult := database.Client().Database("admin").RunCommand(ctx, buildInfoCmd)

	var buildInfoDoc bson.M
	if err := buildInfoResult.Decode(&buildInfoDoc); err != nil {
		return nil, fmt.Errorf("error fetching build info: %v", err)
	}

	// Extract version
	if version, ok := buildInfoDoc["version"].(string); ok {
		details["version"] = version
	} else {
		return nil, fmt.Errorf("error extracting version: version field not found or not a string")
	}

	// Determine edition
	databaseEdition := "community"
	if modules, ok := buildInfoDoc["modules"].(bson.A); ok {
		for _, module := range modules {
			if moduleStr, ok := module.(string); ok && moduleStr == "enterprise" {
				databaseEdition = "enterprise"
				break
			}
		}
	}
	details["databaseEdition"] = databaseEdition

	// Get database stats
	statsCmd := bson.D{{Key: "dbStats", Value: 1}}
	statsResult := database.RunCommand(ctx, statsCmd)
	var statsDoc bson.M
	if err := statsResult.Decode(&statsDoc); err != nil {
		return nil, fmt.Errorf("error fetching database stats: %v", err)
	}

	// Extract database size
	var databaseSize int64
	if dataSize, ok := statsDoc["dataSize"]; ok {
		switch size := dataSize.(type) {
		case int64:
			databaseSize = size
		case int32:
			databaseSize = int64(size)
		case float64:
			databaseSize = int64(size)
		default:
			// Handle as zero if type is unexpected
			databaseSize = 0
		}
	}
	details["databaseSize"] = databaseSize

	// Generate a unique identifier
	serverStatusCmd := bson.D{{Key: "serverStatus", Value: 1}}
	serverStatusResult := database.Client().Database("admin").RunCommand(ctx, serverStatusCmd)
	var serverStatusDoc bson.M
	if err := serverStatusResult.Decode(&serverStatusDoc); err != nil {
		return nil, fmt.Errorf("error fetching server status: %v", err)
	}

	if host, ok := serverStatusDoc["host"].(string); ok {
		details["uniqueIdentifier"] = host
	} else {
		return nil, fmt.Errorf("error extracting host: host field not found or not a string")
	}

	return details, nil
}

func getSslMode(config dbclient.DatabaseConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "prefer"
	}
	return "require"
}

func getInstanceSslMode(config dbclient.InstanceConfig) string {
	if config.SSLMode != "" {
		return config.SSLMode
	}
	if config.SSLRejectUnauthorized != nil && !*config.SSLRejectUnauthorized {
		return "prefer"
	}
	return "require"
}

// CollectDatabaseMetadata collects metadata from a MongoDB database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	database, ok := db.(*mongo.Database)
	if !ok {
		return nil, fmt.Errorf("invalid mongodb connection type")
	}

	metadata := make(map[string]interface{})

	// Get database stats
	statsCmd := bson.D{{Key: "dbStats", Value: 1}}
	statsResult := database.RunCommand(ctx, statsCmd)
	var statsDoc bson.M
	if err := statsResult.Decode(&statsDoc); err != nil {
		return nil, fmt.Errorf("failed to get database stats: %w", err)
	}

	// Extract relevant metadata
	if dataSize, exists := statsDoc["dataSize"]; exists {
		metadata["size_bytes"] = dataSize
	}

	// Get collections count
	collections, err := database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to get collections count: %w", err)
	}
	metadata["collections_count"] = len(collections)

	// Get database version
	buildInfoCmd := bson.D{{Key: "buildInfo", Value: 1}}
	buildInfoResult := database.Client().Database("admin").RunCommand(ctx, buildInfoCmd)
	var buildInfoDoc bson.M
	if err := buildInfoResult.Decode(&buildInfoDoc); err != nil {
		return nil, fmt.Errorf("failed to get database version: %w", err)
	}

	if version, ok := buildInfoDoc["version"].(string); ok {
		metadata["version"] = version
	} else {
		return nil, fmt.Errorf("error extracting version: version field not found or not a string")
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a MongoDB instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	database, ok := db.(*mongo.Database)
	if !ok {
		return nil, fmt.Errorf("invalid mongodb connection type")
	}

	metadata := make(map[string]interface{})

	// Get server status
	serverStatusCmd := bson.D{{Key: "serverStatus", Value: 1}}
	serverStatusResult := database.Client().Database("admin").RunCommand(ctx, serverStatusCmd)
	var statusDoc bson.M
	if err := serverStatusResult.Decode(&statusDoc); err != nil {
		return nil, fmt.Errorf("failed to get server status: %w", err)
	}

	// Extract version
	if version, ok := statusDoc["version"].(string); ok {
		metadata["version"] = version
	}

	// Extract uptime
	if uptime, exists := statusDoc["uptime"]; exists {
		switch u := uptime.(type) {
		case int64:
			metadata["uptime_seconds"] = u
		case int32:
			metadata["uptime_seconds"] = int64(u)
		case float64:
			metadata["uptime_seconds"] = int64(u)
		default:
			metadata["uptime_seconds"] = int64(0)
		}
	}

	// Get total databases
	client := database.Client()
	dbs, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to get total databases: %w", err)
	}
	metadata["total_databases"] = len(dbs)

	// Get connections info
	if connections, ok := statusDoc["connections"].(bson.M); ok {
		if current, ok := connections["current"]; ok {
			switch c := current.(type) {
			case int32:
				metadata["total_connections"] = int(c)
			case int64:
				metadata["total_connections"] = int(c)
			case float64:
				metadata["total_connections"] = int(c)
			}
		}

		if available, ok := connections["available"]; ok {
			switch a := available.(type) {
			case int32:
				metadata["max_connections"] = int(a)
			case int64:
				metadata["max_connections"] = int(a)
			case float64:
				metadata["max_connections"] = int(a)
			}
		}
	}

	return metadata, nil
}

// ExecuteCommand executes a command on a MongoDB database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	database, ok := db.(*mongo.Database)
	if !ok {
		return nil, fmt.Errorf("invalid mongodb connection type")
	}

	// Parse the command as BSON
	var commandDoc bson.M
	if err := bson.UnmarshalExtJSON([]byte(command), false, &commandDoc); err != nil {
		// If it's not valid JSON, try to parse as a simple command
		// For example: "find" -> {"find": "collection"}
		parts := strings.Fields(command)
		if len(parts) > 0 {
			commandDoc = bson.M{parts[0]: 1}
		} else {
			return nil, fmt.Errorf("invalid command format: %w", err)
		}
	}

	// Execute the command
	result := database.RunCommand(ctx, commandDoc)
	if result.Err() != nil {
		return nil, fmt.Errorf("failed to execute command: %w", result.Err())
	}

	// Decode the result
	var resultDoc bson.M
	if err := result.Decode(&resultDoc); err != nil {
		return nil, fmt.Errorf("failed to decode result: %w", err)
	}

	// Structure the response for gRPC
	response := map[string]interface{}{
		"columns": []string{"result"},
		"rows":    []map[string]interface{}{{"result": resultDoc}},
		"count":   1,
	}

	// Convert to JSON bytes for gRPC transmission
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal results to JSON: %w", err)
	}

	return jsonBytes, nil
}

// CreateDatabase creates a new MongoDB database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	database, ok := db.(*mongo.Database)
	if !ok {
		return fmt.Errorf("invalid mongodb connection type")
	}

	// Get the client to create a new database
	client := database.Client()

	// In MongoDB, databases are created implicitly when you first store data
	// We'll create a collection to ensure the database exists
	newDatabase := client.Database(databaseName)

	// Create a temporary collection to ensure the database is created
	tempCollectionName := "_temp_creation_collection"
	collection := newDatabase.Collection(tempCollectionName)

	// Insert a temporary document
	_, err := collection.InsertOne(ctx, bson.M{"temp": true})
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Delete the temporary document and collection
	_, err = collection.DeleteOne(ctx, bson.M{"temp": true})
	if err != nil {
		return fmt.Errorf("failed to clean up temporary document: %w", err)
	}

	// Drop the temporary collection
	err = collection.Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to clean up temporary collection: %w", err)
	}

	return nil
}

// DropDatabase drops a MongoDB database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	database, ok := db.(*mongo.Database)
	if !ok {
		return fmt.Errorf("invalid mongodb connection type")
	}

	// Get the client to access the target database
	client := database.Client()
	targetDatabase := client.Database(databaseName)

	// Check if database exists first if if_exists option is set
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		// List databases to check if it exists
		databases, err := client.ListDatabaseNames(ctx, bson.M{})
		if err != nil {
			return fmt.Errorf("failed to list databases: %w", err)
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

	// Drop the database
	err := targetDatabase.Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}
