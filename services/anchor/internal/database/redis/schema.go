package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redis/go-redis/v9"
)

// DiscoverSchema fetches the current schema of a Redis database and returns a UnifiedModel
func DiscoverSchema(client *redis.Client) (*unifiedmodel.UnifiedModel, error) {
	ctx := context.Background()

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType:  dbcapabilities.Redis,
		KeyValuePairs: make(map[string]unifiedmodel.KeyValuePair),
		Modules:       make(map[string]unifiedmodel.Module),
		Functions:     make(map[string]unifiedmodel.Function),
		Streams:       make(map[string]unifiedmodel.Stream),
		Namespaces:    make(map[string]unifiedmodel.Namespace),
		Extensions:    make(map[string]unifiedmodel.Extension),
	}

	// Discover key-value pairs (sample keys from the database)
	if err := discoverKeyValuePairsUnified(ctx, client, um); err != nil {
		// Non-fatal error, just log and continue
		fmt.Printf("Warning: Could not discover key-value pairs: %v\n", err)
	}

	// Get loaded modules
	if err := discoverModulesUnified(ctx, client, um); err != nil {
		return nil, fmt.Errorf("error discovering modules: %v", err)
	}

	// Get Redis functions (Redis 7.0+)
	if err := discoverFunctionsUnified(ctx, client, um); err != nil {
		// Functions might not be available in older Redis versions
		// Just log the error and continue
		fmt.Printf("Warning: Could not discover Redis functions: %v\n", err)
	}

	// Get Redis streams
	if err := discoverStreamsUnified(ctx, client, um); err != nil {
		return nil, fmt.Errorf("error discovering streams: %v", err)
	}

	// Get keyspace info
	if err := discoverKeySpacesUnified(ctx, client, um); err != nil {
		return nil, fmt.Errorf("error discovering keyspaces: %v", err)
	}

	return um, nil
}

// discoverModulesUnified discovers Redis modules directly into UnifiedModel
func discoverModulesUnified(ctx context.Context, client *redis.Client, um *unifiedmodel.UnifiedModel) error {
	// Get list of loaded modules
	result, err := client.Do(ctx, "MODULE", "LIST").Result()
	if err != nil {
		return fmt.Errorf("error getting module list: %v", err)
	}

	modules, ok := result.([]interface{})
	if !ok {
		return fmt.Errorf("unexpected result type for MODULE LIST")
	}

	for _, moduleData := range modules {
		moduleInfo, ok := moduleData.([]interface{})
		if ok && len(moduleInfo) >= 2 {
			if nameInterface, ok := moduleInfo[1].(string); ok {
				module := unifiedmodel.Module{
					Name:    nameInterface,
					Comment: "", // Redis modules don't have descriptions in MODULE LIST
				}
				um.Modules[nameInterface] = module
			}
		}
	}

	return nil
}

// discoverFunctionsUnified discovers Redis functions directly into UnifiedModel
func discoverFunctionsUnified(ctx context.Context, client *redis.Client, um *unifiedmodel.UnifiedModel) error {
	// Get list of Redis functions (Redis 7.0+)
	result, err := client.Do(ctx, "FUNCTION", "LIST").Result()
	if err != nil {
		return err // Return error to caller for handling
	}

	functions, ok := result.([]interface{})
	if !ok {
		return fmt.Errorf("unexpected result type for FUNCTION LIST")
	}

	for _, functionData := range functions {
		functionInfo, ok := functionData.([]interface{})
		if ok && len(functionInfo) >= 4 {
			if nameInterface, ok := functionInfo[1].(string); ok {
				var body string
				if len(functionInfo) > 3 {
					if bodyInterface, ok := functionInfo[3].(string); ok {
						body = bodyInterface
					}
				}

				function := unifiedmodel.Function{
					Name:       nameInterface,
					Language:   "lua", // Redis functions are typically Lua-based
					Definition: body,
				}
				um.Functions[nameInterface] = function
			}
		}
	}

	return nil
}

// discoverStreamsUnified discovers Redis streams directly into UnifiedModel
func discoverStreamsUnified(ctx context.Context, client *redis.Client, um *unifiedmodel.UnifiedModel) error {
	// Get all keys that are streams
	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		return fmt.Errorf("error getting keys: %v", err)
	}

	for _, key := range keys {
		keyType, err := client.Type(ctx, key).Result()
		if err != nil {
			continue // Skip keys we can't check
		}

		if keyType == "stream" {
			// Get stream info
			streamInfo, err := client.XInfoStream(ctx, key).Result()
			if err != nil {
				continue // Skip streams we can't get info for
			}

			stream := unifiedmodel.Stream{
				Name: key,
				Options: map[string]interface{}{
					"length":           streamInfo.Length,
					"radix_tree_keys":  streamInfo.RadixTreeKeys,
					"radix_tree_nodes": streamInfo.RadixTreeNodes,
					"groups":           streamInfo.Groups,
				},
			}
			um.Streams[key] = stream
		}
	}

	return nil
}

// discoverKeySpacesUnified discovers Redis keyspaces directly into UnifiedModel
func discoverKeySpacesUnified(ctx context.Context, client *redis.Client, um *unifiedmodel.UnifiedModel) error {
	// Get keyspace info from INFO command
	info, err := client.Info(ctx, "keyspace").Result()
	if err != nil {
		return fmt.Errorf("error getting keyspace info: %v", err)
	}

	lines := strings.Split(info, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "db") {
			// Parse db line: db0:keys=1,expires=0,avg_ttl=0
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				dbName := parts[0]
				dbID, err := strconv.Atoi(strings.TrimPrefix(dbName, "db"))
				if err != nil {
					continue
				}

				// Parse keyspace stats
				stats := parts[1]
				var keys, expires int64
				var avgTTL time.Duration

				statPairs := strings.Split(stats, ",")
				for _, pair := range statPairs {
					kv := strings.Split(pair, "=")
					if len(kv) == 2 {
						switch kv[0] {
						case "keys":
							keys, _ = strconv.ParseInt(kv[1], 10, 64)
						case "expires":
							expires, _ = strconv.ParseInt(kv[1], 10, 64)
						case "avg_ttl":
							ttlMs, _ := strconv.ParseInt(kv[1], 10, 64)
							avgTTL = time.Duration(ttlMs) * time.Millisecond
						}
					}
				}

				namespace := unifiedmodel.Namespace{
					Name: fmt.Sprintf("db%d", dbID),
					Options: map[string]interface{}{
						"database_id": dbID,
						"keys":        keys,
						"expires":     expires,
						"avg_ttl":     avgTTL,
					},
				}
				um.Namespaces[namespace.Name] = namespace
			}
		}
	}

	return nil
}

// discoverKeyValuePairsUnified discovers key-value pairs (samples keys) directly into UnifiedModel
func discoverKeyValuePairsUnified(ctx context.Context, client *redis.Client, um *unifiedmodel.UnifiedModel) error {
	// Use SCAN to sample keys from the database (limit to prevent overwhelming large databases)
	const maxKeys = 100
	var cursor uint64
	var keys []string

	for len(keys) < maxKeys {
		var scanKeys []string
		var err error
		scanKeys, cursor, err = client.Scan(ctx, cursor, "*", 10).Result()
		if err != nil {
			return fmt.Errorf("error scanning keys: %v", err)
		}

		keys = append(keys, scanKeys...)

		if cursor == 0 {
			break
		}
	}

	// Limit to maxKeys
	if len(keys) > maxKeys {
		keys = keys[:maxKeys]
	}

	// Get details for each key
	for _, key := range keys {
		// Get key type
		keyType, err := client.Type(ctx, key).Result()
		if err != nil {
			continue // Skip this key if we can't get its type
		}

		// Get TTL
		ttl, err := client.TTL(ctx, key).Result()
		var ttlSeconds *int64
		if err == nil && ttl > 0 {
			seconds := int64(ttl.Seconds())
			ttlSeconds = &seconds
		}

		// Get encoding (memory optimization)
		encoding := ""
		if encodingResult, err := client.Do(ctx, "OBJECT", "ENCODING", key).Result(); err == nil {
			if enc, ok := encodingResult.(string); ok {
				encoding = enc
			}
		}

		// Create KeyValuePair entry
		kvPair := unifiedmodel.KeyValuePair{
			Name:     key,
			Key:      key,
			DataType: keyType,
			TTL:      ttlSeconds,
			Encoding: encoding,
			Options: map[string]any{
				"database": client.Options().DB,
			},
		}

		um.KeyValuePairs[key] = kvPair
	}

	return nil
}
