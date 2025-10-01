package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ExecuteQuery executes a query on Redis and returns results as a slice of maps
func ExecuteQuery(db interface{}, query string, args ...interface{}) ([]interface{}, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("invalid redis connection type")
	}

	ctx := context.Background()

	// Parse query to extract Redis command and parameters
	// Expected format: {"command": "GET|HGET|SCAN|KEYS", "key": "key_name", "pattern": "pattern", ...}
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return nil, fmt.Errorf("failed to parse redis query: %w", err)
	}

	command, ok := queryReq["command"].(string)
	if !ok {
		return nil, fmt.Errorf("command is required in redis query")
	}

	var results []interface{}

	switch strings.ToUpper(command) {
	case "GET":
		key, ok := queryReq["key"].(string)
		if !ok {
			return nil, fmt.Errorf("key is required for GET command")
		}

		val, err := client.Get(ctx, key).Result()
		if err != nil {
			if err == redis.Nil {
				return results, nil // Empty result for non-existent key
			}
			return nil, fmt.Errorf("failed to execute GET: %w", err)
		}

		results = append(results, map[string]interface{}{
			"key":   key,
			"value": val,
			"type":  "string",
		})

	case "HGETALL":
		key, ok := queryReq["key"].(string)
		if !ok {
			return nil, fmt.Errorf("key is required for HGETALL command")
		}

		hash, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to execute HGETALL: %w", err)
		}

		if len(hash) > 0 {
			results = append(results, map[string]interface{}{
				"key":   key,
				"value": hash,
				"type":  "hash",
			})
		}

	case "KEYS":
		pattern, ok := queryReq["pattern"].(string)
		if !ok {
			pattern = "*" // Default to all keys
		}

		// Use SCAN instead of KEYS for production safety with large datasets
		cursor := uint64(0)
		for {
			keys, nextCursor, err := client.Scan(ctx, cursor, pattern, 1000).Result()
			if err != nil {
				return nil, fmt.Errorf("failed to execute SCAN (KEYS replacement): %w", err)
			}

			for _, key := range keys {
				keyType, err := client.Type(ctx, key).Result()
				if err != nil {
					continue
				}

				results = append(results, map[string]interface{}{
					"key":  key,
					"type": keyType,
				})
			}

			// Break if we've reached the end
			if nextCursor == 0 {
				break
			}
			cursor = nextCursor
		}

	case "SCAN":
		cursor := uint64(0)
		if c, ok := queryReq["cursor"].(float64); ok {
			cursor = uint64(c)
		}

		pattern, ok := queryReq["pattern"].(string)
		if !ok {
			pattern = "*"
		}

		count := int64(10)
		if c, ok := queryReq["count"].(float64); ok {
			count = int64(c)
		}

		keys, nextCursor, err := client.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to execute SCAN: %w", err)
		}

		for _, key := range keys {
			keyType, err := client.Type(ctx, key).Result()
			if err != nil {
				continue
			}

			results = append(results, map[string]interface{}{
				"key":  key,
				"type": keyType,
			})
		}

		// Add cursor information for pagination
		if len(results) > 0 {
			results = append(results, map[string]interface{}{
				"_cursor": nextCursor,
				"_type":   "cursor_info",
			})
		}

	default:
		return nil, fmt.Errorf("unsupported Redis command: %s", command)
	}

	return results, nil
}

// ExecuteCountQuery executes a count query on Redis and returns the result
func ExecuteCountQuery(db interface{}, query string) (int64, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return 0, fmt.Errorf("invalid redis connection type")
	}

	ctx := context.Background()

	// Parse query to extract command and parameters
	var queryReq map[string]interface{}
	if err := json.Unmarshal([]byte(query), &queryReq); err != nil {
		return 0, fmt.Errorf("failed to parse redis count query: %w", err)
	}

	command, ok := queryReq["command"].(string)
	if !ok {
		return 0, fmt.Errorf("command is required in redis count query")
	}

	switch strings.ToUpper(command) {
	case "DBSIZE":
		// Count all keys in current database
		size, err := client.DBSize(ctx).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to execute DBSIZE: %w", err)
		}
		return size, nil

	case "KEYS_COUNT":
		pattern, ok := queryReq["pattern"].(string)
		if !ok {
			pattern = "*"
		}

		// Use SCAN instead of KEYS for production safety
		var totalCount int64
		cursor := uint64(0)

		for {
			keys, nextCursor, err := client.Scan(ctx, cursor, pattern, 1000).Result()
			if err != nil {
				return 0, fmt.Errorf("failed to scan keys for count: %w", err)
			}

			totalCount += int64(len(keys))

			if nextCursor == 0 {
				break
			}
			cursor = nextCursor
		}

		return totalCount, nil

	case "HLEN":
		key, ok := queryReq["key"].(string)
		if !ok {
			return 0, fmt.Errorf("key is required for HLEN command")
		}

		length, err := client.HLen(ctx, key).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to execute HLEN: %w", err)
		}
		return length, nil

	case "LLEN":
		key, ok := queryReq["key"].(string)
		if !ok {
			return 0, fmt.Errorf("key is required for LLEN command")
		}

		length, err := client.LLen(ctx, key).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to execute LLEN: %w", err)
		}
		return length, nil

	case "SCARD":
		key, ok := queryReq["key"].(string)
		if !ok {
			return 0, fmt.Errorf("key is required for SCARD command")
		}

		cardinality, err := client.SCard(ctx, key).Result()
		if err != nil {
			return 0, fmt.Errorf("failed to execute SCARD: %w", err)
		}
		return cardinality, nil

	default:
		return 0, fmt.Errorf("unsupported Redis count command: %s", command)
	}
}

// StreamTableData streams keys from Redis in batches for efficient data copying
// For Redis, tableName represents a key pattern
func StreamTableData(db interface{}, tableName string, batchSize int32, offset int64, columns []string) ([]map[string]interface{}, bool, string, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return nil, false, "", fmt.Errorf("invalid redis connection type")
	}

	ctx := context.Background()

	// Use SCAN for efficient iteration
	cursor := uint64(offset)
	pattern := tableName
	if pattern == "" {
		pattern = "*"
	}

	keys, nextCursor, err := client.Scan(ctx, cursor, pattern, int64(batchSize)).Result()
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to scan redis keys: %w", err)
	}

	var results []map[string]interface{}
	for _, key := range keys {
		// Get key type
		keyType, err := client.Type(ctx, key).Result()
		if err != nil {
			continue
		}

		result := map[string]interface{}{
			"key":  key,
			"type": keyType,
		}

		// Get value based on type
		switch keyType {
		case "string":
			if val, err := client.Get(ctx, key).Result(); err == nil {
				result["value"] = val
			}
		case "hash":
			if hash, err := client.HGetAll(ctx, key).Result(); err == nil {
				result["value"] = hash
			}
		case "list":
			if list, err := client.LRange(ctx, key, 0, -1).Result(); err == nil {
				result["value"] = list
			}
		case "set":
			if set, err := client.SMembers(ctx, key).Result(); err == nil {
				result["value"] = set
			}
		case "zset":
			if zset, err := client.ZRangeWithScores(ctx, key, 0, -1).Result(); err == nil {
				result["value"] = zset
			}
		}

		// Add TTL information
		if ttl, err := client.TTL(ctx, key).Result(); err == nil {
			result["ttl"] = ttl.Seconds()
		}

		results = append(results, result)
	}

	rowCount := len(results)
	isComplete := nextCursor == 0 || rowCount < int(batchSize)

	// Use cursor for pagination
	nextCursorValue := fmt.Sprintf("%d", nextCursor)

	return results, isComplete, nextCursorValue, nil
}

// GetTableRowCount returns the number of keys in Redis matching a pattern
func GetTableRowCount(db interface{}, tableName string, whereClause string) (int64, bool, error) {
	client, ok := db.(*redis.Client)
	if !ok {
		return 0, false, fmt.Errorf("invalid redis connection type")
	}

	ctx := context.Background()

	pattern := tableName
	if pattern == "" {
		pattern = "*"
	}

	// Use SCAN instead of KEYS for production-safe counting of large datasets
	var totalCount int64
	cursor := uint64(0)

	for {
		keys, nextCursor, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return 0, false, fmt.Errorf("failed to scan redis keys for count: %w", err)
		}

		// Apply where clause filtering if provided
		if whereClause != "" {
			var filteredKeys []string

			// Parse where clause as JSON for type filtering
			var whereReq map[string]interface{}
			if err := json.Unmarshal([]byte(whereClause), &whereReq); err == nil {
				if keyType, ok := whereReq["type"].(string); ok {
					for _, key := range keys {
						if actualType, err := client.Type(ctx, key).Result(); err == nil && actualType == keyType {
							filteredKeys = append(filteredKeys, key)
						}
					}
					keys = filteredKeys
				}
			}
		}

		totalCount += int64(len(keys))

		// Break if we've reached the end
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	// Redis count is always exact, not an estimate
	return totalCount, false, nil
}
