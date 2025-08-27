package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redbco/redb-open/pkg/dbcapabilities"
	"github.com/redbco/redb-open/pkg/unifiedmodel"
	"github.com/redbco/redb-open/services/anchor/internal/database/common"
	"github.com/redis/go-redis/v9"
)

// DiscoverSchema fetches the current schema of a Redis database and returns a UnifiedModel
func DiscoverSchema(client *redis.Client) (*unifiedmodel.UnifiedModel, error) {
	ctx := context.Background()

	// Create the unified model
	um := &unifiedmodel.UnifiedModel{
		DatabaseType: dbcapabilities.Redis,
		Modules:      make(map[string]unifiedmodel.Module),
		Functions:    make(map[string]unifiedmodel.Function),
		Streams:      make(map[string]unifiedmodel.Stream),
		Namespaces:   make(map[string]unifiedmodel.Namespace),
		Extensions:   make(map[string]unifiedmodel.Extension),
	}

	var err error

	// Get loaded modules
	modules, err := discoverModules(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("error discovering modules: %v", err)
	}

	// Convert modules to unified model
	for _, module := range modules {
		um.Modules[module.Name] = ConvertRedisModule(module)
	}

	// Get Redis functions (Redis 7.0+)
	functions, err := discoverFunctions(ctx, client)
	if err != nil {
		// Functions might not be available in older Redis versions
		// Just log the error and continue
		fmt.Printf("Warning: Could not discover Redis functions: %v\n", err)
	} else {
		// Convert functions to unified model
		for _, function := range functions {
			um.Functions[function.Name] = unifiedmodel.Function{
				Name:       function.Name,
				Language:   "lua", // Redis functions are typically Lua-based
				Definition: function.Body,
			}
		}
	}

	// Get Redis streams
	streams, err := discoverStreams(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("error discovering streams: %v", err)
	}

	// Convert streams to unified model
	for _, stream := range streams {
		um.Streams[stream.Name] = ConvertRedisStream(stream)
	}

	// Get keyspace info
	keySpaces, err := discoverKeySpaces(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("error discovering keyspaces: %v", err)
	}

	// Convert keyspaces to unified model as namespaces
	for _, keySpace := range keySpaces {
		namespace := ConvertRedisKeySpace(keySpace)
		um.Namespaces[namespace.Name] = namespace
	}

	return um, nil
}

// discoverKeys fetches information about Redis keys
func discoverKeys(ctx context.Context, client *redis.Client) ([]common.KeyInfo, error) {
	// Get all keys (warning: KEYS is not recommended for production use with large datasets)
	// In a real implementation, you might want to use SCAN instead
	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		return nil, fmt.Errorf("error fetching keys: %v", err)
	}

	// Limit the number of keys to process to avoid performance issues
	maxKeys := 1000
	if len(keys) > maxKeys {
		keys = keys[:maxKeys]
	}

	var keyInfos []common.KeyInfo
	for _, key := range keys {
		// Get key type
		keyType, err := client.Type(ctx, key).Result()
		if err != nil {
			continue
		}

		// Get TTL
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			continue
		}

		ttlSeconds := int64(-1)
		if ttl != time.Duration(-1) && ttl != time.Duration(-2) {
			ttlSeconds = int64(ttl.Seconds())
		}

		// Get size/length based on type
		var size int64
		var sampleValue interface{}

		switch keyType {
		case "string":
			val, err := client.Get(ctx, key).Result()
			if err == nil {
				size = int64(len(val))
				if len(val) <= 100 {
					sampleValue = val
				} else {
					sampleValue = val[:100] + "..."
				}
			}
		case "list":
			size, err = client.LLen(ctx, key).Result()
			if err == nil && size > 0 {
				// Get a sample of the list
				samples, err := client.LRange(ctx, key, 0, 2).Result()
				if err == nil && len(samples) > 0 {
					sampleValue = samples
				}
			}
		case "set":
			size, err = client.SCard(ctx, key).Result()
			if err == nil && size > 0 {
				// Get a sample of the set
				samples, err := client.SRandMemberN(ctx, key, 3).Result()
				if err == nil && len(samples) > 0 {
					sampleValue = samples
				}
			}
		case "zset":
			size, err = client.ZCard(ctx, key).Result()
			if err == nil && size > 0 {
				// Get a sample of the sorted set
				samples, err := client.ZRangeWithScores(ctx, key, 0, 2).Result()
				if err == nil && len(samples) > 0 {
					sampleValue = samples
				}
			}
		case "hash":
			size, err = client.HLen(ctx, key).Result()
			if err == nil && size > 0 {
				// Get a sample of the hash
				if size <= 3 {
					samples, err := client.HGetAll(ctx, key).Result()
					if err == nil {
						sampleValue = samples
					}
				} else {
					// Just get a few fields
					fields, err := client.HKeys(ctx, key).Result()
					if err == nil && len(fields) > 0 {
						sampleFields := fields
						if len(fields) > 3 {
							sampleFields = fields[:3]
						}
						samples := make(map[string]string)
						for _, field := range sampleFields {
							val, err := client.HGet(ctx, key, field).Result()
							if err == nil {
								samples[field] = val
							}
						}
						sampleValue = samples
					}
				}
			}
		case "stream":
			size, err = client.XLen(ctx, key).Result()
			if err != nil {
				continue
			}
			// Sample value for streams is handled in discoverStreams
		}

		keyInfos = append(keyInfos, common.KeyInfo{
			Name:        key,
			Type:        keyType,
			TTL:         ttlSeconds,
			Size:        size,
			SampleValue: sampleValue,
		})
	}

	return keyInfos, nil
}

// discoverModules fetches information about loaded Redis modules
func discoverModules(ctx context.Context, client *redis.Client) ([]common.ModuleInfo, error) {
	// Get Redis modules info
	info, err := client.Info(ctx, "modules").Result()
	if err != nil {
		return nil, fmt.Errorf("error fetching modules info: %v", err)
	}

	infoMap := parseRedisInfo(info)

	var modules []common.ModuleInfo

	// Parse modules from info
	for key, value := range infoMap {
		if strings.HasPrefix(key, "module:") {
			parts := strings.Split(value, ",")
			if len(parts) > 0 {
				moduleName := parts[0]
				modules = append(modules, common.ModuleInfo{
					Name: moduleName,
				})
			}
		}
	}

	return modules, nil
}

// discoverFunctions fetches information about Redis functions (Redis 7.0+)
func discoverFunctions(ctx context.Context, client *redis.Client) ([]common.FunctionInfo, error) {
	// Check if FUNCTION LIST command is available (Redis 7.0+)
	cmd := client.Do(ctx, "COMMAND", "INFO", "FUNCTION")
	if cmd.Err() != nil {
		return nil, fmt.Errorf("FUNCTION command not available: %v", cmd.Err())
	}

	// Get all functions
	result, err := client.Do(ctx, "FUNCTION", "LIST").Result()
	if err != nil {
		return nil, fmt.Errorf("error listing functions: %v", err)
	}

	var functions []common.FunctionInfo

	// Parse the result based on Redis response format
	// This is a simplified implementation and might need adjustment
	// based on the actual response format
	if funcList, ok := result.([]interface{}); ok {
		for _, funcData := range funcList {
			if funcMap, ok := funcData.(map[string]interface{}); ok {
				name, _ := funcMap["name"].(string)
				body, _ := funcMap["code"].(string)

				functions = append(functions, common.FunctionInfo{
					Name: name,
					Body: body,
					// Other fields might be available depending on Redis version
				})
			}
		}
	}

	return functions, nil
}

// discoverStreams fetches information about Redis streams
func discoverStreams(ctx context.Context, client *redis.Client) ([]common.StreamInfo, error) {
	// First, find all keys that are streams
	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		return nil, fmt.Errorf("error fetching keys: %v", err)
	}

	var streams []common.StreamInfo

	for _, key := range keys {
		keyType, err := client.Type(ctx, key).Result()
		if err != nil || keyType != "stream" {
			continue
		}

		// Get stream length
		length, err := client.XLen(ctx, key).Result()
		if err != nil {
			continue
		}

		// Get stream info
		streamInfo := common.StreamInfo{
			Name:   key,
			Length: length,
		}

		// Get first and last entry IDs if stream is not empty
		if length > 0 {
			// Get first entry
			first, err := client.XRange(ctx, key, "-", "+").Result()
			if err == nil && len(first) > 0 {
				streamInfo.FirstEntryID = first[0].ID
			}

			// Get last entry
			last, err := client.XRevRange(ctx, key, "+", "-").Result()
			if err == nil && len(last) > 0 {
				streamInfo.LastEntryID = last[0].ID
			}
		}

		// Get consumer groups
		groups, err := client.XInfoGroups(ctx, key).Result()
		if err == nil {
			streamInfo.Groups = len(groups)
		}

		streams = append(streams, streamInfo)
	}

	return streams, nil
}

// discoverKeySpaces fetches information about Redis keyspaces
func discoverKeySpaces(ctx context.Context, client *redis.Client) ([]common.KeySpaceInfo, error) {
	// Get keyspace info
	info, err := client.Info(ctx, "keyspace").Result()
	if err != nil {
		return nil, fmt.Errorf("error fetching keyspace info: %v", err)
	}

	infoMap := parseRedisInfo(info)

	var keyspaces []common.KeySpaceInfo

	// Parse keyspace info
	for key, value := range infoMap {
		if strings.HasPrefix(key, "db") {
			// Extract database number
			dbNumStr := strings.TrimPrefix(key, "db")
			dbNum, err := strconv.Atoi(dbNumStr)
			if err != nil {
				continue
			}

			// Parse keyspace stats
			// Format: keys=123,expires=12,avg_ttl=3600
			stats := make(map[string]int64)
			for _, stat := range strings.Split(value, ",") {
				parts := strings.Split(stat, "=")
				if len(parts) == 2 {
					if val, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
						stats[parts[0]] = val
					}
				}
			}

			keyspaces = append(keyspaces, common.KeySpaceInfo{
				ID:      dbNum,
				Keys:    stats["keys"],
				Expires: stats["expires"],
				AvgTTL:  stats["avg_ttl"],
			})
		}
	}

	return keyspaces, nil
}
