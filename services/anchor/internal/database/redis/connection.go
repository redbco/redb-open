package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/redbco/redb-open/pkg/encryption"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
)

// Connect establishes a connection to a Redis database
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
	var options = &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Username: config.Username,
		Password: decryptedPassword,
		DB:       0, // Default database
	}

	// If a specific database is specified in the DatabaseName field, try to parse it as an integer
	if config.DatabaseName != "" {
		dbIndex, err := strconv.Atoi(config.DatabaseName)
		if err == nil && dbIndex >= 0 {
			options.DB = dbIndex
		}
	}

	// Configure TLS if SSL is enabled
	if config.SSL {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Configure client certificates if provided
		if config.SSLCert != "" && config.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(config.SSLCert, config.SSLKey)
			if err != nil {
				return nil, fmt.Errorf("error loading client certificates: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Configure root CA if provided
		if config.SSLRootCert != "" {
			// In a real implementation, you would load the CA cert here
			// This is simplified for this example
		}

		// Set InsecureSkipVerify based on SSLRejectUnauthorized
		if config.SSLRejectUnauthorized != nil {
			tlsConfig.InsecureSkipVerify = !*config.SSLRejectUnauthorized
		}

		options.TLSConfig = tlsConfig
	}

	// Create Redis client
	client := redis.NewClient(options)

	// Test the connection with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("error connecting to Redis: %v", err)
	}

	return &common.DatabaseClient{
		DB:           client,
		DatabaseType: "redis",
		DatabaseID:   config.DatabaseID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// ConnectInstance establishes a connection to a Redis instance
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
	var options = &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Username: config.Username,
		Password: decryptedPassword,
		DB:       0, // Default database
	}

	// If a specific database is specified in the DatabaseName field, try to parse it as an integer
	if config.DatabaseName != "" {
		dbIndex, err := strconv.Atoi(config.DatabaseName)
		if err == nil && dbIndex >= 0 {
			options.DB = dbIndex
		}
	}

	// Configure TLS if SSL is enabled
	if config.SSL {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Configure client certificates if provided
		if config.SSLCert != "" && config.SSLKey != "" {
			cert, err := tls.LoadX509KeyPair(config.SSLCert, config.SSLKey)
			if err != nil {
				return nil, fmt.Errorf("error loading client certificates: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Configure root CA if provided
		if config.SSLRootCert != "" {
			// In a real implementation, you would load the CA cert here
			// This is simplified for this example
		}

		// Set InsecureSkipVerify based on SSLRejectUnauthorized
		if config.SSLRejectUnauthorized != nil {
			tlsConfig.InsecureSkipVerify = !*config.SSLRejectUnauthorized
		}

		options.TLSConfig = tlsConfig
	}

	// Create Redis client
	client := redis.NewClient(options)

	// Test the connection with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("error connecting to Redis: %v", err)
	}

	return &common.InstanceClient{
		DB:           client,
		InstanceType: "redis",
		InstanceID:   config.InstanceID,
		Config:       config,
		IsConnected:  1,
	}, nil
}

// DiscoverDetails fetches the details of a Redis database
func DiscoverDetails(db interface{}) (*RedisDetails, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("invalid database connection")
	}

	ctx := context.Background()
	details := RedisDetails{
		DatabaseType: "redis",
	}

	// Get server info
	info, err := client.Info(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("error fetching Redis info: %v", err)
	}

	// Parse server info
	infoMap := parseRedisInfo(info)

	// Get Redis version
	if version, ok := infoMap["redis_version"]; ok {
		details.Version = version
	}

	// Get database size (total keys)
	if dbSize, err := client.DBSize(ctx).Result(); err == nil {
		details.KeyCount = dbSize
	}

	// Get memory usage
	if usedMemory, ok := infoMap["used_memory"]; ok {
		if memoryBytes, err := strconv.ParseInt(usedMemory, 10, 64); err == nil {
			details.MemoryUsage = memoryBytes
			details.DatabaseSize = memoryBytes // Use memory usage as database size
		}
	}

	// Determine Redis edition (enterprise or open source)
	if _, ok := infoMap["redis_mode"]; ok && infoMap["redis_mode"] == "cluster" {
		details.DatabaseEdition = "enterprise"
	} else if _, ok := infoMap["redis_build_id"]; ok && strings.Contains(strings.ToLower(infoMap["redis_build_id"]), "enterprise") {
		details.DatabaseEdition = "enterprise"
	} else {
		details.DatabaseEdition = "open-source"
	}

	// Generate a unique identifier
	if runID, ok := infoMap["run_id"]; ok {
		details.UniqueIdentifier = runID
	} else {
		details.UniqueIdentifier = "unknown"
	}

	return &details, nil
}

// parseRedisInfo parses the Redis INFO command output into a map
func parseRedisInfo(info string) map[string]string {
	result := make(map[string]string)
	lines := strings.Split(info, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}

	return result
}

// CollectDatabaseMetadata collects metadata from a Redis database
func CollectDatabaseMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("invalid redis connection type")
	}

	metadata := make(map[string]interface{})

	// Get Redis info
	info, err := client.Info(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis info: %w", err)
	}
	infoMap := parseRedisInfo(info)

	// Get database version
	if version, ok := infoMap["redis_version"]; ok {
		metadata["version"] = version
	}

	// Get database size (key count)
	keyCount, err := client.DBSize(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get database size: %w", err)
	}
	metadata["key_count"] = keyCount

	// Get memory usage
	if usedMemory, ok := infoMap["used_memory"]; ok {
		if memoryBytes, err := strconv.ParseInt(usedMemory, 10, 64); err == nil {
			metadata["memory_bytes"] = memoryBytes
		}
	}

	return metadata, nil
}

// CollectInstanceMetadata collects metadata from a Redis instance
func CollectInstanceMetadata(ctx context.Context, db interface{}) (map[string]interface{}, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("invalid redis connection type")
	}

	metadata := make(map[string]interface{})

	// Get Redis info
	info, err := client.Info(ctx, "server", "clients", "memory", "stats").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis info: %w", err)
	}
	infoMap := parseRedisInfo(info)

	// Get Redis version
	if version, ok := infoMap["redis_version"]; ok {
		metadata["version"] = version
	}

	// Get uptime in seconds
	if uptime, ok := infoMap["uptime_in_seconds"]; ok {
		if uptimeSeconds, err := strconv.ParseInt(uptime, 10, 64); err == nil {
			metadata["uptime_seconds"] = uptimeSeconds
		}
	}

	// Get connected clients
	if clients, ok := infoMap["connected_clients"]; ok {
		if connectedClients, err := strconv.Atoi(clients); err == nil {
			metadata["connected_clients"] = connectedClients
		}
	}

	// Get max clients
	if maxClients, ok := infoMap["maxclients"]; ok {
		if maxClientsNum, err := strconv.Atoi(maxClients); err == nil {
			metadata["max_clients"] = maxClientsNum
			metadata["max_connections"] = maxClientsNum // Set max_connections to max_clients
		}
	}

	// Get memory usage
	if usedMemory, ok := infoMap["used_memory"]; ok {
		if memoryBytes, err := strconv.ParseInt(usedMemory, 10, 64); err == nil {
			metadata["used_memory_bytes"] = memoryBytes
		}
	}

	// Get max memory
	if maxMemory, ok := infoMap["maxmemory"]; ok {
		if maxMemoryBytes, err := strconv.ParseInt(maxMemory, 10, 64); err == nil {
			metadata["max_memory_bytes"] = maxMemoryBytes
		}
	}

	// Get total commands processed
	if cmdProcessed, ok := infoMap["total_commands_processed"]; ok {
		if commands, err := strconv.ParseInt(cmdProcessed, 10, 64); err == nil {
			metadata["total_commands_processed"] = commands
		}
	}

	return metadata, nil
}

// ExecuteCommand executes a command on a Redis database and returns results as bytes
func ExecuteCommand(ctx context.Context, db interface{}, command string) ([]byte, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("invalid redis connection type")
	}

	// Parse the command string into command and arguments
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	cmdName := strings.ToUpper(parts[0])
	args := make([]interface{}, len(parts)-1)
	for i, part := range parts[1:] {
		args[i] = part
	}

	// Execute the command
	allArgs := make([]interface{}, len(args)+1)
	allArgs[0] = cmdName
	copy(allArgs[1:], args)
	result := client.Do(ctx, allArgs...)
	if result.Err() != nil {
		return nil, fmt.Errorf("failed to execute command: %w", result.Err())
	}

	// Get the result value
	value, err := result.Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get result: %w", err)
	}

	// Structure the response for gRPC
	response := map[string]interface{}{
		"columns": []string{"result"},
		"rows":    []map[string]interface{}{{"result": value}},
		"count":   1,
	}

	// Convert to JSON bytes for gRPC transmission
	jsonBytes, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal results to JSON: %w", err)
	}

	return jsonBytes, nil
}

// CreateDatabase creates a new Redis database with optional parameters
func CreateDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*redis.Client)
	if !ok {
		return fmt.Errorf("invalid redis connection type")
	}

	// Parse database name as integer (Redis databases are numbered)
	dbIndex, err := strconv.Atoi(databaseName)
	if err != nil {
		return fmt.Errorf("invalid database name for Redis (must be integer): %w", err)
	}

	// Redis databases are automatically available (0-15 by default)
	// We can't "create" them, but we can verify access
	tempClient := redis.NewClient(&redis.Options{
		Addr:     client.Options().Addr,
		Username: client.Options().Username,
		Password: client.Options().Password,
		DB:       dbIndex,
	})
	defer tempClient.Close()

	// Test access to the database
	err = tempClient.Ping(ctx).Err()
	if err != nil {
		return fmt.Errorf("failed to access Redis database %d: %w", dbIndex, err)
	}

	return nil
}

// DropDatabase drops a Redis database with optional parameters
func DropDatabase(ctx context.Context, db interface{}, databaseName string, options map[string]interface{}) error {
	client, ok := db.(*redis.Client)
	if !ok {
		return fmt.Errorf("invalid redis connection type")
	}

	// Parse database name as integer (Redis databases are numbered)
	dbIndex, err := strconv.Atoi(databaseName)
	if err != nil {
		return fmt.Errorf("invalid database name for Redis (must be integer): %w", err)
	}

	// Create a client for the specific database
	tempClient := redis.NewClient(&redis.Options{
		Addr:     client.Options().Addr,
		Username: client.Options().Username,
		Password: client.Options().Password,
		DB:       dbIndex,
	})
	defer tempClient.Close()

	// Check if database should exist first
	if ifExists, ok := options["if_exists"].(bool); ok && ifExists {
		// Test if we can connect to verify existence
		err = tempClient.Ping(ctx).Err()
		if err != nil {
			// Database doesn't exist or is inaccessible, but that's OK with if_exists
			return nil
		}
	}

	// Flush all keys in the database (Redis equivalent of dropping a database)
	err = tempClient.FlushDB(ctx).Err()
	if err != nil {
		return fmt.Errorf("failed to flush Redis database %d: %w", dbIndex, err)
	}

	return nil
}
